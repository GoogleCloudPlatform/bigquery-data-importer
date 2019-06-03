// Copyright 2019 Google LLC
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//      http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package com.google.cloud.healthcare.process.pipeline;

import com.google.cloud.healthcare.config.GcpConfiguration;
import com.google.cloud.healthcare.io.GcsOutputWriter;
import com.google.cloud.healthcare.io.OutputWriter;
import com.google.cloud.healthcare.process.schema.FieldType;
import com.google.cloud.healthcare.process.schema.SchemaUtil;
import com.google.cloud.healthcare.util.StringUtil;
import com.google.common.base.Strings;
import com.google.common.collect.Sets;
import java.io.IOException;
import java.nio.channels.Channels;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.time.temporal.ChronoUnit;
import java.time.temporal.Temporal;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.apache.avro.Schema;
import org.apache.avro.file.DataFileWriter;
import org.apache.avro.generic.GenericData.Record;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.generic.GenericRecord;
import org.apache.beam.sdk.transforms.CombineWithContext.CombineFnWithContext;
import org.apache.beam.sdk.transforms.CombineWithContext.Context;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollectionView;

/**
 * A {@link CombineFnWithContext} that writes data to an AVRO file on GCS, the schema is passed in
 * as a side input. A {@link Map} is used for schema instead of {@link Schema} as it is not
 * serializable.
 *
 * Each block writes to its own file on GCS and later will be loaded to the same table in one
 * job.
 */
public class GcsWriteAvroFn extends CombineFnWithContext<KV<String, List<String[]>>,
    Set<String>, Set<String>> {
  private static final long MICROS_TO_NANOS = 1000L;

  // Schema is passed in as side input.
  private final PCollectionView<Map<String, FieldType[]>> schemaView;
  private final PCollectionView<Map<String, String[]>> headersView;
  private final GcpConfiguration config;
  private final String tempBucket;

  public GcsWriteAvroFn(GcpConfiguration config, String tempBucket,
      PCollectionView<Map<String, String[]>> headers,
      PCollectionView<Map<String, FieldType[]>> schema) {
    this.config = config;
    this.tempBucket = tempBucket;
    this.headersView = headers;
    this.schemaView = schema;
  }

  @Override
  public Set<String> createAccumulator(Context c) {
    return Sets.newHashSet();
  }

  @Override
  public Set<String> addInput(Set<String> accumulator, KV<String, List<String[]>> input,
      Context c) {
    Map<String, String[]> allHeaders = c.sideInput(headersView);
    Map<String, FieldType[]> allSchema = c.sideInput(schemaView);

    String name = input.getKey();
    try {
      write(allHeaders.get(name), allSchema.get(name), input);
    } catch (IOException e) {
      throw new RuntimeException(e);
    }

    accumulator.add(name);
    return accumulator;
  }

  @Override
  public Set<String> mergeAccumulators(Iterable<Set<String>> accumulators, Context c) {
    Set<String> result = Sets.newHashSet();
    accumulators.forEach(result::addAll);
    return result;
  }

  @Override
  public Set<String> extractOutput(Set<String> accumulator, Context c) {
    return accumulator;
  }

  @Override
  public Set<String> defaultValue() {
    return Sets.newHashSet();
  }

  private void write(String[] headers, FieldType[] types,
      KV<String, List<String[]>> records) throws IOException {
    Schema schema = SchemaUtil.generateAvroSchema(headers, Arrays.asList(types));

    String name = records.getKey();
    String path = StringUtil.splitGcsUri(name)[1];
    try (DataFileWriter<GenericRecord> writer
        = new DataFileWriter<>(new GenericDatumWriter<>(schema))) {
      OutputWriter outputWriter = new GcsOutputWriter(config.getCredentials(),
          StringUtil.getGcsAvroUri(StringUtil.generateGcsUri(tempBucket, path)));
      writer.create(schema, Channels.newOutputStream(outputWriter.getWriteChannel()));

      for (String[] record : records.getValue()) {
        GenericRecord avroRecord = new Record(schema);
        for (int i = 0; i < record.length; i++) {
          String key = headers[i];
          FieldType type = types[i];

          if (Strings.isNullOrEmpty(record[i])) {
            // TODO(b/123357608): Remove this hack after
            // https://issues.apache.org/jira/browse/AVRO-1891 is fixed.
            if (type == FieldType.DATE) {
              avroRecord.put(key, ChronoUnit.DAYS.between(LocalDate.ofEpochDay(0),
                  LocalDate.of(2099, 12, 31)));
            } else if (type == FieldType.TIME) {
              avroRecord.put(key,
                  LocalTime.of(23, 59, 59, 999_999_000).toNanoOfDay() / MICROS_TO_NANOS);
            } else if (type == FieldType.DATETIME) {
              avroRecord.put(key, ChronoUnit.MILLIS.between(Instant.EPOCH,
                  ZonedDateTime.of(2099, 12, 31, 0, 0, 0, 0, ZoneId.of("UTC")).toInstant()));
            }
            continue;
          }

          switch (type) {
            case INT:
              avroRecord.put(key, SchemaUtil.convertToInteger(record[i]));
              break;
            case LONG:
              avroRecord.put(key, SchemaUtil.convertToLong(record[i]));
              break;
            case DOUBLE:
              avroRecord.put(key, SchemaUtil.convertToDouble(record[i]));
              break;
            // TODO(b/120794993): Skip date/time conversion based on a flag supplied by users.
            case TIME:
              // AVRO requires micros from epoch for time logical type.
              avroRecord.put(key,
                  SchemaUtil.convertToTime(record[i]).toNanoOfDay() / MICROS_TO_NANOS);
              break;
            case DATE:
              // AVRO requires days from epoch for date logical type.
              long days =
                  ChronoUnit.DAYS.between(
                      LocalDate.ofEpochDay(0), SchemaUtil.convertToDate(record[i]));
              avroRecord.put(key, (int) days);
              break;
            case DATETIME:
              Temporal dateTime = SchemaUtil.convertToDateTime(record[i]);
              if (dateTime instanceof LocalDateTime) {
                dateTime = ((LocalDateTime) dateTime).atZone(ZoneId.of("UTC"));
              }
              // Use millis instead of micros because the underlying implementation in Java uses
              // Instant.nanosUntil for converting nanos and micros, which only allows timestamps in
              // the range of [now - 262 years, now + 262 years] roughly.
              long millis = ChronoUnit.MILLIS.between(Instant.EPOCH, dateTime);
              avroRecord.put(key, millis);
              break;
            case BOOLEAN:
              avroRecord.put(key, SchemaUtil.isTrue(record[i]));
              break;
            default:
              avroRecord.put(key, record[i]);
              break;
          }
        }
        writer.append(avroRecord);
      }
      writer.flush();
    }
  }
}
