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

import com.google.cloud.bigquery.BigQuery;
import com.google.cloud.bigquery.BigQueryError;
import com.google.cloud.bigquery.FormatOptions;
import com.google.cloud.bigquery.Job;
import com.google.cloud.bigquery.JobInfo;
import com.google.cloud.bigquery.JobInfo.CreateDisposition;
import com.google.cloud.bigquery.LoadJobConfiguration;
import com.google.cloud.bigquery.TableId;
import com.google.cloud.healthcare.config.GcpConfiguration;
import com.google.cloud.healthcare.process.schema.GcpUtil;
import com.google.cloud.healthcare.util.StringUtil;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.util.List;
import org.apache.beam.sdk.transforms.DoFn;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A {@link DoFn} which loads data from GCS to BQ.
 */
public class GcsLoadToBigQueryFn extends DoFn<String, Void> {
  private static final Logger LOG = LoggerFactory.getLogger(GcsLoadToBigQueryFn.class);

  private final GcpConfiguration config;
  private final String dataset;
  private final String tempBucket;
  private transient BigQuery bigquery;

  public GcsLoadToBigQueryFn(GcpConfiguration config, String dataset, String tempBucket) {
    this.config = config;
    this.dataset = dataset;
    this.tempBucket = tempBucket;
    initBigQuery();
  }

  @ProcessElement
  public void load(ProcessContext ctx) throws InterruptedException {
    String name = ctx.element();
    String path = StringUtil.splitGcsUri(name)[1];
    TableId tableId = TableId.of(dataset, StringUtil.getGcsBaseName(name));
    LoadJobConfiguration loadConfig = LoadJobConfiguration
        .newBuilder(tableId, StringUtil.getBqSrcUri(StringUtil.generateGcsUri(tempBucket, path)))
        .setCreateDisposition(CreateDisposition.CREATE_IF_NEEDED)
        .setFormatOptions(FormatOptions.avro())
        .build();
    Job loadJob = bigquery.create(JobInfo.of(loadConfig));
    loadJob = loadJob.waitFor();
    handleErrors(loadJob);
    LOG.info(loadJob.getStatistics().toString());
  }

  // Floats any error messages to the job runner.
  private void handleErrors(Job loadJob) {
    List<BigQueryError> executionErrors = loadJob.getStatus().getExecutionErrors();
    if (executionErrors != null) {
      for (BigQueryError error : executionErrors) {
        LOG.error(error.toString());
      }
    }

    BigQueryError error = loadJob.getStatus().getError();
    if (error != null) {
      throw new RuntimeException(error.toString());
    }
  }

  private void readObject(ObjectInputStream in) throws IOException, ClassNotFoundException {
    in.defaultReadObject();
    initBigQuery();
  }

  private void initBigQuery() {
    bigquery = GcpUtil.getBqClient(config.getCredentials());
  }
}
