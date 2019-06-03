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
import com.google.common.collect.Lists;
import java.io.IOException;
import java.net.URISyntaxException;
import java.net.URL;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.List;
import org.apache.beam.sdk.testing.PAssert;
import org.apache.beam.sdk.testing.TestPipeline;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.junit.Rule;
import org.junit.Test;

/** Test for CsvDetectSchemaFn. */
public class CsvDetectSchemaFnTest {

  private static final String FILENAME = "test_input_parse.csv";

  @Rule
  public final transient TestPipeline p = TestPipeline.create();

  @Test
  public void detect_returnExpectedSchema() throws IOException, URISyntaxException {
    URL url = this.getClass().getClassLoader().getResource(FILENAME);
    byte[] bytes = Files.readAllBytes(Paths.get(url.toURI()));

    PCollection<KV<String, byte[]>> input = p.apply(Create.of(KV.of(FILENAME, bytes)));
    PCollection<KV<String, List<FieldType>>> output = input.apply(ParDo.of(new CsvParseDataFn(
        CsvConfiguration.getInstance())))
        .apply(ParDo.of(new CsvDetectSchemaFn()));
    PAssert.thatSingleton(output).isEqualTo(KV.of(FILENAME,
        Lists.newArrayList(FieldType.INT, FieldType.STRING, FieldType.STRING)));
    p.run();
  }
}