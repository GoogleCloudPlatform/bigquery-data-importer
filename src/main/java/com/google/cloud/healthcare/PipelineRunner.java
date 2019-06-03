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

package com.google.cloud.healthcare;

import com.google.api.gax.paging.Page;
import com.google.auth.oauth2.GoogleCredentials;
import com.google.cloud.healthcare.config.CsvConfiguration;
import com.google.cloud.healthcare.config.GcpConfiguration;
import com.google.cloud.healthcare.decompress.Decompressor;
import com.google.cloud.healthcare.io.GcsInputReader;
import com.google.cloud.healthcare.io.GcsOutputWriterFactory;
import com.google.cloud.healthcare.io.InputReader;
import com.google.cloud.healthcare.process.pipeline.GcsGenerateChunksFn;
import com.google.cloud.healthcare.process.pipeline.GcsLoadToBigQueryFn;
import com.google.cloud.healthcare.process.pipeline.GcsWriteAvroFn;
import com.google.cloud.healthcare.process.pipeline.ReadDataFn;
import com.google.cloud.healthcare.process.pipeline.csv.CsvAggregateHeadersFn;
import com.google.cloud.healthcare.process.pipeline.csv.CsvDetectSchemaFn;
import com.google.cloud.healthcare.process.pipeline.csv.CsvExtractHeadersFn;
import com.google.cloud.healthcare.process.pipeline.csv.CsvMergeSchemaFn;
import com.google.cloud.healthcare.process.pipeline.csv.CsvParseDataFn;
import com.google.cloud.healthcare.process.pipeline.csv.GcsSplitCsvFn;
import com.google.cloud.healthcare.process.pipeline.csv.advance.CsvParseDataAdvanceFn;
import com.google.cloud.healthcare.process.pipeline.csv.advance.GcsSplitCsvAdvanceFn;
import com.google.cloud.healthcare.process.schema.FieldType;
import com.google.cloud.healthcare.process.schema.GcpUtil;
import com.google.cloud.healthcare.util.StringUtil;
import com.google.cloud.storage.Blob;
import com.google.cloud.storage.Storage;
import com.google.cloud.storage.StorageBatch;
import com.google.common.base.Strings;
import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.Set;
import javax.annotation.Nullable;
import org.apache.beam.runners.dataflow.DataflowRunner;
import org.apache.beam.runners.dataflow.options.DataflowPipelineOptions;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.PipelineResult;
import org.apache.beam.sdk.io.FileIO;
import org.apache.beam.sdk.io.FileIO.ReadableFile;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.transforms.Combine;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.transforms.Filter;
import org.apache.beam.sdk.transforms.Flatten;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.ParDo.SingleOutput;
import org.apache.beam.sdk.transforms.View;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollectionView;

/** Run the whole pipeline. */
public class PipelineRunner {

  public static void run(String projectId, String dataflowServiceAccount, String dataset,
      String tempBucket, String gcsUri) throws IOException {
    String[] uriParts = StringUtil.splitGcsUri(gcsUri);
    String bucket = uriParts[0];
    String path = uriParts[1];

    try {
      List<String> uris = decompress(tempBucket, bucket, path);
      runDataflowPipeline(projectId, dataflowServiceAccount, tempBucket, uris, dataset);
    } finally {
      cleanUp(tempBucket);
    }
  }

  private static List<String> decompress(String tempBucket, String bucket,
      String path) throws IOException {
    GoogleCredentials credentials = GcpConfiguration.getInstance().getCredentials();
    InputReader decompressReader = new GcsInputReader(credentials, bucket, path);
    Decompressor decompressor = new Decompressor(new GcsOutputWriterFactory(credentials,
        StringUtil.getGcsDecompressUri(tempBucket)));
    return decompressor.decompress(decompressReader);
  }

  private static void runDataflowPipeline(String projectId, @Nullable String serviceAccount,
      String tempBucket, List<String> uris, String dataset) {
    GoogleCredentials credentials = GcpConfiguration.getInstance().getCredentials();
    DataflowPipelineOptions options = PipelineOptionsFactory.as(DataflowPipelineOptions.class);
    if (credentials != null) {
      options.setGcpCredential(credentials);
    }
    if (!Strings.isNullOrEmpty(serviceAccount)) {
      options.setServiceAccount(serviceAccount);
    }
    if (!Strings.isNullOrEmpty(projectId)) {
      options.setProject(projectId);
    }
    options.setTempLocation(String.format("%s/dataflow", StringUtil.getGcsTempDir(tempBucket)));
    options.setRunner(DataflowRunner.class);
    Pipeline p = Pipeline.create(options);

    PCollection<ReadableFile> files = p.apply(Create.of(uris))
        .apply(FileIO.matchAll())
        .apply(FileIO.readMatches())
        // Filter empty files.
        .apply(Filter.by(f -> f.getMetadata().sizeBytes() > 0));

    // Extract headers.
    PCollectionView<Map<String, String[]>> headers = files
        .apply(ParDo.of(new CsvExtractHeadersFn()))
        .apply(Combine.globally(new CsvAggregateHeadersFn()))
        .apply(View.asSingleton());

    // Determine which functions to use.
    CsvConfiguration config = CsvConfiguration.getInstance();
    boolean useAdvancedFns =
        config.getRecordSeparatorRegex() != null && config.getDelimiterRegex() != null;
    SingleOutput<ReadableFile, KV<String, Set<Long>>> splitFn =
        ParDo.of(
            useAdvancedFns
                ? new GcsSplitCsvAdvanceFn(config, headers)
                : new GcsSplitCsvFn(config, headers));
    SingleOutput<KV<String, byte[]>, KV<String, List<String[]>>> parseFn =
        ParDo.of(useAdvancedFns ? new CsvParseDataAdvanceFn(config) : new CsvParseDataFn(config));

    // Process data.
    GcpConfiguration gcpConfig = GcpConfiguration.getInstance();
    PCollection<KV<String, List<String[]>>> parsedData = files
        .apply(splitFn.withSideInputs(headers))
        .apply(ParDo.of(new GcsGenerateChunksFn(gcpConfig)))
        .apply(ParDo.of(new ReadDataFn()))
        .apply(parseFn);

    // Schema detection.
    PCollectionView<Map<String, FieldType[]>> schema =
        parsedData
            .apply(ParDo.of(new CsvDetectSchemaFn()))
            .apply(Combine.globally(new CsvMergeSchemaFn()))
            .apply(View.asSingleton());

    // Write as AVRO files.
    parsedData
        .apply(Combine.globally(new GcsWriteAvroFn(gcpConfig, tempBucket, headers, schema))
            .withSideInputs(headers, schema))
        .apply(Flatten.iterables())
        .apply(ParDo.of(new GcsLoadToBigQueryFn(gcpConfig, dataset, tempBucket)));

    PipelineResult result = p.run();
    result.waitUntilFinish();
  }

  private static void cleanUp(String bucket) {
    String prefix = StringUtil.getGcsTempDir(bucket);
    String[] parts = StringUtil.splitGcsUri(prefix);

    GoogleCredentials credentials = GcpConfiguration.getInstance().getCredentials();
    Storage storage = GcpUtil.getGcsClient(credentials);
    StorageBatch batch = storage.batch();
    Page<Blob> blobs = storage.list(bucket,
        Storage.BlobListOption.prefix(String.format("%s/", parts[1])));
    for (Blob blob : blobs.iterateAll()) {
      batch.delete(blob.getBlobId());
    }
    batch.submit();
  }
}
