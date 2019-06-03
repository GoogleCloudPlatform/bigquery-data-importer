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

import static org.junit.Assert.assertEquals;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.when;
import static org.mockito.MockitoAnnotations.initMocks;

import com.google.cloud.WriteChannel;
import com.google.cloud.healthcare.config.GcpConfiguration;
import com.google.cloud.healthcare.io.GcsOutputWriter;
import com.google.cloud.healthcare.process.schema.FieldType;
import com.google.cloud.healthcare.process.schema.SchemaUtil;
import com.google.cloud.storage.Blob;
import com.google.cloud.storage.BlobId;
import com.google.cloud.storage.Storage;
import com.google.cloud.storage.StorageOptions;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.channels.Channels;
import java.nio.channels.WritableByteChannel;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import org.apache.avro.Schema;
import org.apache.avro.file.DataFileReader;
import org.apache.avro.file.SeekableByteArrayInput;
import org.apache.avro.generic.GenericData.Record;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericRecord;
import org.apache.beam.sdk.testing.TestPipeline;
import org.apache.beam.sdk.transforms.Combine;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.transforms.View;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollectionView;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.powermock.api.mockito.PowerMockito;
import org.powermock.api.support.membermodification.MemberMatcher;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;

/** Test for GcsWriteAvroFn */
@RunWith(PowerMockRunner.class)
@PrepareForTest({GcsOutputWriter.class, StorageOptions.class})
public class GcsWriteAvroFnTest {

  private static final String FILENAME = "gs://mybucket/path/to/file.csv";

  private static final String[] RECORD1 = new String[] {
      "123", "36", "3.1415", "true", "2018-12-02", "05:00", "2018-05-12T08:00:00"};
  private static final String[] RECORD2 = new String[] {
      "52", "50", "2.734", "false", "1977-02-01", "21:55", "2016-04-12T08:00:00"};
  private static final String[] RECORD3 = new String[] {
      "52", "50", "2.734", "false", "", "", ""};

  private static final String[] CONVERTED_RECORD1 = new String[] {
      "123", "36", "3.1415", "true", "17867", "18000000000", "1526112000000"};
  private static final String[] CONVERTED_RECORD2 = new String[] {
      "52", "50", "2.734", "false", "2588", "78900000000", "1460448000000"};
  private static final String[] CONVERTED_RECORD3 = new String[] {
      "52", "50", "2.734", "false", "47481", "86399999999", "4102358400000"};

  private static final FieldType[] SCHEMA = new FieldType[] {
      FieldType.INT, FieldType.LONG, FieldType.DOUBLE, FieldType.BOOLEAN,
      FieldType.DATE, FieldType.TIME, FieldType.DATETIME
  };

  private static final String[] HEADERS = new String[] {
      "int", "long", "double", "bool", "date", "time", "datetime"};

  private ByteArrayOutputStream os = new ByteArrayOutputStream();

  private WritableByteChannel outputChannel;

  @Rule
  public final transient TestPipeline p = TestPipeline.create();

  @Mock
  private Storage storage;

  @Mock
  private StorageOptions storageOptions;

  @Mock
  private Blob blob;

  @Mock
  private WriteChannel channel;

  @Before
  public void setUp() {
    outputChannel = Channels.newChannel(os);
    initMocks(this);
    PowerMockito.mockStatic(Channels.class, StorageOptions.class);
    PowerMockito.stub(MemberMatcher.method(GcsOutputWriter.class,
        "getWriteChannel")).toReturn(outputChannel);
    when(StorageOptions.getDefaultInstance()).thenReturn(storageOptions);
    when(storageOptions.getService()).thenReturn(storage);
    when(storage.get(any(BlobId.class))).thenReturn(blob);
    when(blob.writer()).thenReturn(channel);
  }

  @Test
  public void write_expectedContent() throws IOException {
    PCollection<KV<String, List<String[]>>> input = p.apply("create",
        Create.of(KV.of(FILENAME, Lists.newArrayList(RECORD1, RECORD2, RECORD3))));

    PCollectionView<Map<String, String[]>> headersView =
        p.apply("create_headers", Create.of(
            ImmutableMap.<String, String[]>builder()
                .put(FILENAME, HEADERS).build()))
            .apply("headers", View.asMap());
    PCollectionView<Map<String, FieldType[]>> schemaView =
        p.apply("create_schema", Create.of(
            ImmutableMap.<String, FieldType[]>builder().put(FILENAME, SCHEMA).build()))
            .apply("schema", View.asMap());
    input.apply(
        "write",
        Combine.globally(new GcsWriteAvroFn(GcpConfiguration.getInstance(), "temp",
            headersView, schemaView)).withSideInputs(headersView, schemaView));
    p.run();

    List<GenericRecord> records = parseAvro(os.toByteArray());
    List<List<String>> result = Lists.newArrayList();
    for (GenericRecord record : records) {
      List<String> row = Lists.newArrayList();
      for (int i = 0; i < SCHEMA.length; i++) {
        row.add(record.get(i).toString());
      }
      result.add(row);
    }
    assertEquals("Parsed file should be the same as input.",
        Lists.newArrayList(
            Arrays.asList(CONVERTED_RECORD1),
            Arrays.asList(CONVERTED_RECORD2),
            Arrays.asList(CONVERTED_RECORD3)),
        result);
  }

  private static List<GenericRecord> parseAvro(byte[] data) throws IOException {
    Schema schema = SchemaUtil.generateAvroSchema(HEADERS, Arrays.asList(SCHEMA));

    List<GenericRecord> records = Lists.newArrayList();
    DataFileReader<Record> reader = new DataFileReader<>(new SeekableByteArrayInput(data),
        new GenericDatumReader<>(schema));
    for (GenericRecord record : reader) {
      records.add(record);
    }

    return records;
  }
}