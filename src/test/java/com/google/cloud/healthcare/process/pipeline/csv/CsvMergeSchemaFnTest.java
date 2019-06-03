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
import java.util.Map;
import org.apache.beam.sdk.testing.PAssert;
import org.apache.beam.sdk.testing.TestPipeline;
import org.apache.beam.sdk.transforms.Combine;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.junit.Rule;
import org.junit.Test;

/** Test for CsvMergeSchemaFn. */
public class CsvMergeSchemaFnTest {

  private static final String FILENAME1 = "file1";
  private static final String FILENAME2 = "file2";

  @Rule
  public final transient TestPipeline p = TestPipeline.create();

  @Test
  public void merge_sameFile_returnMergedSchema() {
    PCollection<KV<String, List<FieldType>>> input = p.apply(Create.of(
        KV.of(FILENAME1,
            Lists.newArrayList(FieldType.STRING, FieldType.DATETIME, FieldType.INT,
                FieldType.BOOLEAN)),
        KV.of(FILENAME1,
            Lists.newArrayList(FieldType.LONG, FieldType.DATE, FieldType.TIME, FieldType.DOUBLE))
    ));

    PCollection<Map<String, FieldType[]>> output = input.apply(
        Combine.globally(new CsvMergeSchemaFn()));

    FieldType[] expected = new FieldType[] {
        FieldType.STRING, FieldType.DATETIME, FieldType.STRING, FieldType.STRING};

    PAssert.thatSingleton(output).satisfies(in -> {
      assertEquals("Only one item.", 1, in.size());
      assertEquals("Size matches headers.", expected.length, in.get(FILENAME1).length);
      for (int i = 0; i < expected.length; i++) {
        assertEquals("Type matches.", expected[i], in.get(FILENAME1)[i]);
      }
      return null;
    });
    p.run();
  }

  @Test
  public void merge_diffFile_returnRespectiveSchema() {
    PCollection<KV<String, List<FieldType>>> input = p.apply(Create.of(
        KV.of(FILENAME1,
            Lists.newArrayList(FieldType.STRING, FieldType.DATETIME, FieldType.INT,
                FieldType.BOOLEAN)),
        KV.of(FILENAME2,
            Lists.newArrayList(FieldType.LONG, FieldType.DATE, FieldType.TIME, FieldType.DOUBLE))
    ));

    PCollection<Map<String, FieldType[]>> output = input.apply(
        Combine.globally(new CsvMergeSchemaFn()));

    FieldType[] expected1 = new FieldType[] {
        FieldType.STRING, FieldType.DATETIME, FieldType.INT, FieldType.BOOLEAN};
    FieldType[] expected2 = new FieldType[] {
        FieldType.LONG, FieldType.DATE, FieldType.TIME, FieldType.DOUBLE};

    PAssert.thatSingleton(output).satisfies(in -> {
      assertEquals("Only two item.", 2, in.size());
      assertEquals("Size matches headers.", expected1.length, in.get(FILENAME1).length);
      assertEquals("Size matches headers.", expected2.length, in.get(FILENAME2).length);
      for (int i = 0; i < expected1.length; i++) {
        assertEquals("Type matches.", expected1[i], in.get(FILENAME1)[i]);
      }
      for (int i = 0; i < expected2.length; i++) {
        assertEquals("Type matches.", expected2[i], in.get(FILENAME2)[i]);
      }
      return null;
    });
    p.run();
  }
}