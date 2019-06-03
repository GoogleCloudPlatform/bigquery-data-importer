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

package com.google.cloud.healthcare.process.pipeline.csv;

import com.google.cloud.healthcare.config.CsvConfiguration;
import com.google.cloud.healthcare.process.schema.FieldType;
import com.google.cloud.healthcare.process.schema.SchemaUtil;
import com.google.common.base.Strings;
import com.google.common.collect.Sets;
import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.nio.channels.Channels;
import java.util.Arrays;
import java.util.List;
import org.apache.beam.sdk.io.FileIO.ReadableFile;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.values.KV;

/**
 * A {@link DoFn} that extracts the first line of a {@link ReadableFile} as headers and apply
 * simple validations.
 */
public class CsvExtractHeadersFn extends DoFn<ReadableFile, KV<String, String[]>> {

  @ProcessElement
  public void extract(ProcessContext ctx) throws IOException {
    ReadableFile file = ctx.element();

    try (BufferedReader reader = new BufferedReader(
        new InputStreamReader(Channels.newInputStream(file.open())))) {
      String line = reader.readLine();

      String delimiter = CsvConfiguration.getInstance().getDelimiter().toString();
      String[] headers = normalizeHeaders(line.split(delimiter));
      if (!validateHeaders(headers)) {
        throw new IllegalArgumentException(String.format(
            "%s of file %s contains invalid headers. Headers are required for every file.",
            line, file.getMetadata().resourceId().toString()));
      }

      ctx.output(KV.of(file.getMetadata().resourceId().toString(), headers));
    }
  }

  /**
   * Normalizes headers by removing quotes (only those at the beginning and at the end), and
   * replacing any non-word characters with underscores to meet the requirements on column names
   * from both AVRO and BigQuery.
   *
   * Headers might collide after the normalization, in which case we'll abort.
   */
  private static String[] normalizeHeaders(String[] headers) {
    for (int i = 0; i < headers.length; i++) {
      headers[i] = headers[i]
          // Surrounding quotes.
          .replaceAll("^\"|\"$", "")
          // Non-word chars.
          .replaceAll("\\W", "_");
    }
    return headers;
  }

  /**
   * Checks if the first line are really headers.
   *
   * The heuristics deployed here are:
   *
   * 1. There are no duplicate values
   * 2. There are no non-string values
   * 3. There are no empty values
   *
   * @return false if we know for sure that these are not valid headers, true meaning based on our
   * heuristics these look like valid headers.
   */
  private static boolean validateHeaders(String[] headers) {
    // Empty values.
    for (String header : headers) {
      if (Strings.isNullOrEmpty(header) || Strings.isNullOrEmpty(header.trim())) {
        return false;
      }
    }

    // Non-string values.
    List<FieldType> types =  SchemaUtil.infer(Arrays.asList(headers));
    if (types.stream().anyMatch(t -> t != FieldType.STRING)) {
      return false;
    }

    // Duplicate values.
    return Sets.newHashSet(headers).size() == headers.length;
  }
}
