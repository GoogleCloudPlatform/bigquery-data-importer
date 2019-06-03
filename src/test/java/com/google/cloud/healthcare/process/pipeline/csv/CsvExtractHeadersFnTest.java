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

import java.net.URL;
import org.apache.beam.sdk.Pipeline.PipelineExecutionException;
import org.apache.beam.sdk.io.FileIO;
import org.apache.beam.sdk.io.FileIO.ReadableFile;
import org.apache.beam.sdk.testing.PAssert;
import org.apache.beam.sdk.testing.TestPipeline;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.junit.Rule;
import org.junit.Test;

/** Test for {@link CsvExtractHeadersFn}. */
public class CsvExtractHeadersFnTest {

  @Rule
  public final transient TestPipeline p = TestPipeline.create();

  @Test
  public void extract_validHeaders_returnHeaders() {
    URL url1 = this.getClass().getClassLoader().getResource("test_input_valid_headers1.csv");
    URL url2 = this.getClass().getClassLoader().getResource("test_input_valid_headers2.csv");

    PCollection<String> urls = p.apply(Create.of(url1.toString(), url2.toString()));
    PCollection<ReadableFile> input = urls
        .apply(FileIO.matchAll())
        .apply(FileIO.readMatches());
    PCollection<KV<String, String[]>> output = input.apply(ParDo.of(new CsvExtractHeadersFn()));
    PAssert.that(output).containsInAnyOrder(
        KV.of(url1.getPath(), new String[] {"YEAR", "MAKE", "DESCRIPTION"}),
        KV.of(url2.getPath(), new String[] {"BIRTH_DATE", "GENDER", "HEIGHT"})
    );
    p.run();
  }

  @Test(expected = PipelineExecutionException.class)
  public void extract_invalidHeaders_exception() {
    URL url = this.getClass().getClassLoader().getResource("test_input_invalid_headers.csv");

    PCollection<ReadableFile> input = p
        .apply(FileIO.match().filepattern(url.toString()))
        .apply(FileIO.readMatches());
    input.apply(ParDo.of(new CsvExtractHeadersFn()));
    p.run();
  }
}