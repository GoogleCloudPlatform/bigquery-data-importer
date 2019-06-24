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

package com.google.cloud.healthcare.process.pipeline;

import com.google.api.services.bigquery.model.TableRow;
import com.google.cloud.healthcare.process.schema.FieldType;
import com.google.cloud.healthcare.process.schema.SchemaUtil;
import com.google.common.base.Strings;
import java.util.Map;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollectionView;

/**
 * A {@link DoFn} that fill data into {@link TableRow}s accepted by {@link
 * org.apache.beam.sdk.io.gcp.bigquery.BigQueryIO}. The schemas and headers are passed in as side
 * inputs.
 */
public class FillTableRowFn extends DoFn<KV<String, String[]>, KV<String, TableRow>> {

  private final PCollectionView<Map<String, FieldType[]>> schemasView;
  private final PCollectionView<Map<String, String[]>> headersView;

  public FillTableRowFn(
      PCollectionView<Map<String, FieldType[]>> schemasView,
      PCollectionView<Map<String, String[]>> headersView) {
    this.schemasView = schemasView;
    this.headersView = headersView;
  }

  @ProcessElement
  public void fill(
      @Element KV<String, String[]> in,
      OutputReceiver<KV<String, TableRow>> out,
      ProcessContext ctx) {
    FieldType[] schema = ctx.sideInput(schemasView).get(in.getKey());
    String[] header = ctx.sideInput(headersView).get(in.getKey());
    String[] record = in.getValue();

    TableRow tableRow = new TableRow();
    for (int i = 0; i < record.length; i++) {
      String field = record[i];
      if (!Strings.isNullOrEmpty(field)) {
        String key = header[i];
        FieldType type = schema[i];

        switch (type) {
          case INT:
            tableRow.put(key, SchemaUtil.convertToInteger(field));
            break;
          case LONG:
            tableRow.put(key, SchemaUtil.convertToLong(field));
            break;
          case DOUBLE:
            tableRow.put(key, SchemaUtil.convertToDouble(field));
            break;
          // TODO(b/120794993): Skip date/time conversion based on a flag supplied by users.
          case TIME:
            tableRow.put(key, SchemaUtil.convertToTime(field));
            break;
          case DATE:
            tableRow.put(key, SchemaUtil.convertToDate(field));
            break;
          case DATETIME:
            tableRow.put(key, SchemaUtil.convertToDateTime(field));
            break;
          case BOOLEAN:
            tableRow.put(key, SchemaUtil.isTrue(field));
            break;
          default:
            tableRow.put(key, field);
            break;
        }
      }
    }

    out.output(KV.of(in.getKey(), tableRow));
  }
}
