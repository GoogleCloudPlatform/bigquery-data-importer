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

import com.google.api.services.bigquery.model.TableReference;
import com.google.api.services.bigquery.model.TableRow;
import com.google.api.services.bigquery.model.TableSchema;
import com.google.cloud.healthcare.process.schema.FieldType;
import com.google.cloud.healthcare.process.schema.GcpUtil;
import com.google.cloud.healthcare.process.schema.SchemaUtil;
import com.google.common.collect.ImmutableList;
import java.util.List;
import java.util.Map;
import org.apache.beam.sdk.io.gcp.bigquery.DynamicDestinations;
import org.apache.beam.sdk.io.gcp.bigquery.TableDestination;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollectionView;
import org.apache.beam.sdk.values.ValueInSingleWindow;

/**
 * A {@link DynamicDestinations} used by {@link org.apache.beam.sdk.io.gcp.bigquery.BigQueryIO}
 * to indicate the destination BigQuery table for each record. Each record will be assigned by its
 * URI. The schemas and headers are passed in to construct {@link TableSchema} from {@code
 * FieldType[]}. Note that {@link TableSchema} is not serializable.
 */
public class BigQueryDestinations extends DynamicDestinations<KV<String, TableRow>, String> {

  private final PCollectionView<Map<String, FieldType[]>> schemasView;
  private final PCollectionView<Map<String, String[]>> headersView;
  private final String projectId;
  private final String datasetId;

  public BigQueryDestinations(
      PCollectionView<Map<String, FieldType[]>> schemasView,
      PCollectionView<Map<String, String[]>> headersView,
      String projectId,
      String datasetId) {
    this.schemasView = schemasView;
    this.headersView = headersView;
    this.projectId = projectId;
    this.datasetId = datasetId;
  }

  @Override
  public List<PCollectionView<?>> getSideInputs() {
    return ImmutableList.of(schemasView, headersView);
  }

  @Override
  public String getDestination(ValueInSingleWindow<KV<String, TableRow>> element) {
    return element.getValue().getKey();
  }

  @Override
  public TableDestination getTable(String destination) {
    TableReference tableReference =
        GcpUtil.getBigQueryTableReference(projectId, datasetId, destination);

    return new TableDestination(tableReference, null);
  }

  @Override
  public TableSchema getSchema(String destination) {
    // TODO(b/135939392): Avoid reconstructing TableSchema for each record.
    FieldType[] schema = sideInput(schemasView).get(destination);
    String[] header = sideInput(headersView).get(destination);
    return SchemaUtil.generateBigQueryTableSchema(header, schema);
  }
}
