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

import java.util.Map;
import org.apache.beam.sdk.testing.PAssert;
import org.apache.beam.sdk.testing.TestPipeline;
import org.apache.beam.sdk.transforms.Combine;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.junit.Rule;
import org.junit.Test;

/** Test for {@link CsvAggregateHeadersFn}. */
public class CsvAggregateHeadersFnTest {

  @Rule
  public final transient TestPipeline p = TestPipeline.create();

  @Test
  public void merge_returnExpectedSchema() {
    PCollection<KV<String, String[]>> input = p.apply(Create.of(
        KV.of("file1", new String[]{"1", "2", "3"}), KV.of("file2", new String[] {"3", "1", "2"})));
    PCollection<Map<String, String[]>> output = input.apply(
        Combine.globally(new CsvAggregateHeadersFn()));
    PAssert.thatSingleton(output).satisfies(m -> {
      for (String h : m.get("file1")) {
        System.out.println("file1: " + h);
      }
      for (String h : m.get("file2")) {
        System.out.println("file2: " + h);
      }
      return null;
    });
    p.run();
  }
}