/*
 * Copyright 2019 Google LLC.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.google.cloud.healthcare.process.pipeline.csv;

import com.google.api.services.bigquery.model.TableRow;
import com.google.cloud.healthcare.process.pipeline.FillTableRowFn;
import com.google.cloud.healthcare.process.schema.FieldType;
import com.google.common.collect.Lists;
import java.util.Map;
import org.apache.beam.sdk.testing.PAssert;
import org.apache.beam.sdk.testing.TestPipeline;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.View;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollectionView;
import org.junit.Rule;
import org.junit.Test;

/** Test for {@link FillTableRowFn}. */
public class FillTableRowFnTest {

  private static final String FILENAME = "file.csv";

  private static final String[] row1 = new String[] {"1", "a", "true", "0.5"};
  private static final String[] row2 = new String[] {"0", "e", "true", "2.71"};
  private static final String[] row3 = new String[] {"20", "ab", "false", "0.1"};
  private static final String[] row4 = new String[] {"100", "pi", "false", "3.14"};

  private static final FieldType[] schema =
      new FieldType[] {FieldType.INT, FieldType.STRING, FieldType.BOOLEAN, FieldType.DOUBLE};
  private static final String[] header = new String[] {"count", "name", "valid", "value"};

  @Rule public final transient TestPipeline p = TestPipeline.create();

  @Test
  public void fill_returnFilledTableRow() {
    PCollection<KV<String, String[]>> input =
        p.apply(
            Create.of(
                KV.of(FILENAME, row1),
                KV.of(FILENAME, row2),
                KV.of(FILENAME, row3),
                KV.of(FILENAME, row4)));

    PCollectionView<Map<String, FieldType[]>> schemasView =
        p.apply("create_schemas", Create.of(KV.of(FILENAME, schema)))
            .apply("as_map_schemas", View.asMap());

    PCollectionView<Map<String, String[]>> headersView =
        p.apply("create_headers", Create.of(KV.of(FILENAME, header)))
            .apply("as_map_headers", View.asMap());

    PCollection<KV<String, TableRow>> output =
        input.apply(
            ParDo.of(new FillTableRowFn(schemasView, headersView))
                .withSideInputs(schemasView, headersView));

    PAssert.that(output)
        .containsInAnyOrder(
            Lists.newArrayList(
                KV.of(
                    FILENAME,
                    new TableRow()
                        .set("count", 1)
                        .set("name", "a")
                        .set("valid", true)
                        .set("value", 0.5)),
                KV.of(
                    FILENAME,
                    new TableRow()
                        .set("count", 0)
                        .set("name", "e")
                        .set("valid", true)
                        .set("value", 2.71)),
                KV.of(
                    FILENAME,
                    new TableRow()
                        .set("count", 20)
                        .set("name", "ab")
                        .set("valid", false)
                        .set("value", 0.1)),
                KV.of(
                    FILENAME,
                    new TableRow()
                        .set("count", 100)
                        .set("name", "pi")
                        .set("valid", false)
                        .set("value", 3.14))));

    p.run();
  }
}
