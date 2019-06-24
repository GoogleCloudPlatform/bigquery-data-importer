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

package com.google.cloud.healthcare.process.pipeline.csv.advance;

import com.google.cloud.healthcare.config.CsvConfiguration;
import java.io.IOException;
import java.net.URISyntaxException;
import java.net.URL;
import java.nio.file.Files;
import java.nio.file.Paths;
import org.apache.beam.sdk.testing.PAssert;
import org.apache.beam.sdk.testing.TestPipeline;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.junit.Rule;
import org.junit.Test;

/** Test for {@link CsvParseDataAdvanceFn}. */
public class CsvParseDataAdvanceFnTest {
  private static final String FILENAME = "test_input_parse.csv";

  @Rule
  public final transient TestPipeline p = TestPipeline.create();

  @Test
  public void parse_returnExpectedParsedResult() throws IOException, URISyntaxException {
    URL url = this.getClass().getClassLoader().getResource(FILENAME);
    byte[] bytes = Files.readAllBytes(Paths.get(url.toURI()));

    PCollection<KV<String, byte[]>> input = p.apply(Create.of(KV.of(FILENAME, bytes)));
    PCollection<KV<String, String>> output = input
        .apply(ParDo.of(new CsvParseDataAdvanceFn(
            CsvConfiguration.getInstance()
                .withRecordSeparatorRegex("\n(?=\\d{4})").withDelimiterRegex(",(?=\\S+)"))))
        .apply(ParDo.of(new FlattenDoFn()));
    PAssert.that(output).containsInAnyOrder(
        KV.of(FILENAME, "1999"), KV.of(FILENAME, "Chevy"),
        KV.of(FILENAME, "\"Venture \"\"Extended Edition\"\"\""),
        KV.of(FILENAME, "1999"), KV.of(FILENAME, "Chevy"),
        KV.of(FILENAME, "\"Venture \"\"Extended Edition, Very Large\"\"\""),
        KV.of(FILENAME, "1997"), KV.of(FILENAME, "Ford"),
        KV.of(FILENAME, "\"Super, luxurious truck\""),
        KV.of(FILENAME, "1996"), KV.of(FILENAME, "Jeep"),
        KV.of(FILENAME, "\"MUST SELL!\nair, moon roof, loaded\"")
    );
    p.run();
  }

  /** Flatten the result for easier comparison. */
  public static class FlattenDoFn extends DoFn<KV<String, String[]>, KV<String, String>> {
    @ProcessElement
    public void process(ProcessContext ctx) {
      KV<String, String[]> input = ctx.element();
      for (String column : input.getValue()) {
        ctx.output(KV.of(input.getKey(), column));
      }
    }
  }
}