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

import static org.junit.Assert.assertEquals;

import com.google.cloud.healthcare.process.schema.FieldType;
import com.google.common.collect.Lists;
import java.util.List;
import org.apache.beam.sdk.testing.PAssert;
import org.apache.beam.sdk.testing.TestPipeline;
import org.apache.beam.sdk.transforms.Combine;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.values.PCollection;
import org.junit.Rule;
import org.junit.Test;

/** Test for {@link CsvMergeSchemaFn}. */
public class CsvMergeSchemaFnTest {

  @Rule public final transient TestPipeline p = TestPipeline.create();

  @Test
  public void merge_returnMergedSchema() {
    PCollection<List<FieldType>> input =
        p.apply(
            Create.of(
                Lists.newArrayList(
                    FieldType.STRING, FieldType.DATETIME, FieldType.INT, FieldType.BOOLEAN),
                Lists.newArrayList(
                    FieldType.LONG, FieldType.DATE, FieldType.TIME, FieldType.DOUBLE)));

    PCollection<FieldType[]> output = input.apply(Combine.globally(new CsvMergeSchemaFn()));

    FieldType[] expected =
        new FieldType[] {FieldType.STRING, FieldType.DATETIME, FieldType.STRING, FieldType.STRING};

    PAssert.thatSingleton(output)
        .satisfies(
            in -> {
              assertEquals("Size matches headers.", expected.length, in.length);
              for (int i = 0; i < expected.length; i++) {
                assertEquals("Type matches.", expected[i], in[i]);
              }
              return null;
            });
    p.run();
  }
}
