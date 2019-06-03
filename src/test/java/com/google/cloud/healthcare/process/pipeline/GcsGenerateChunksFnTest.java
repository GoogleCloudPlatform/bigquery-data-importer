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

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.when;
import static org.mockito.MockitoAnnotations.initMocks;

import avro.shaded.com.google.common.collect.Sets;
import com.google.cloud.healthcare.config.GcpConfiguration;
import com.google.cloud.healthcare.io.GcsInputReader;
import com.google.cloud.healthcare.process.pipeline.Chunk.Range;
import com.google.cloud.storage.Blob;
import com.google.cloud.storage.BlobId;
import com.google.cloud.storage.Storage;
import com.google.cloud.storage.StorageOptions;
import java.nio.channels.Channels;
import java.util.Set;
import org.apache.beam.sdk.testing.PAssert;
import org.apache.beam.sdk.testing.TestPipeline;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.powermock.api.mockito.PowerMockito;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;

/** Test for GcsGenerateChunksFn. */
@RunWith(PowerMockRunner.class)
@PrepareForTest({StorageOptions.class})
public class GcsGenerateChunksFnTest {

  private static final String BUCKET1 = "bucket";
  private static final String PATH1 = "file.csv";
  private static final String FILENAME1 = String.format("gs://%s/%s", BUCKET1, PATH1);

  private static final String BUCKET2 = "mybucket";
  private static final String PATH2 = "path/to/file.csv";
  private static final String FILENAME2 = String.format("gs://%s/%s", BUCKET2, PATH2);

  @Rule
  public final transient TestPipeline p = TestPipeline.create();

  @Mock
  private Storage storage;

  @Mock
  private StorageOptions storageOptions;

  @Mock
  private Blob blob;

  @Before
  public void setUp() {
    initMocks(this);
    PowerMockito.mockStatic(StorageOptions.class, Channels.class);
    when(StorageOptions.getDefaultInstance()).thenReturn(storageOptions);
    when(storageOptions.getService()).thenReturn(storage);
    when(storage.get(any(BlobId.class))).thenReturn(blob);
  }

  @Test
  public void generate_twoElements_oneChunk() {
    PCollection<KV<String, Set<Long>>> input = p.apply(
        Create.of(KV.of(FILENAME1, Sets.newHashSet(0L, 100L))));
    PCollection<KV<String, Chunk>> output = input.apply(ParDo.of(new GcsGenerateChunksFn(
        GcpConfiguration.getInstance())));
    PAssert.that(output).containsInAnyOrder(
        KV.of(FILENAME1, new Chunk(new GcsInputReader(null, BUCKET1, PATH1), new Range(0L, 100L))));
    p.run();
  }

  @Test
  public void generate_multipleElements_expectedChunks() {
    PCollection<KV<String, Set<Long>>> input = p.apply(
        Create.of(
            KV.of(FILENAME1, Sets.newHashSet(0L, 20L)),
            KV.of(FILENAME2, Sets.newHashSet(100L, 0L, 5L, 81L, 32L))));
    PCollection<KV<String, Chunk>> output = input.apply(ParDo.of(
        new GcsGenerateChunksFn(GcpConfiguration.getInstance())));
    PAssert.that(output).containsInAnyOrder(
        KV.of(FILENAME1, new Chunk(new GcsInputReader(null, BUCKET1, PATH1), new Range(0L, 20L))),
        KV.of(FILENAME2, new Chunk(new GcsInputReader(null, BUCKET2, PATH2), new Range(0L, 5L))),
        KV.of(FILENAME2, new Chunk(new GcsInputReader(null, BUCKET2, PATH2), new Range(5L, 32L))),
        KV.of(FILENAME2, new Chunk(new GcsInputReader(null, BUCKET2, PATH2), new Range(32L, 81L))),
        KV.of(FILENAME2, new Chunk(new GcsInputReader(null, BUCKET2, PATH2), new Range(81L, 100L)))
    );
    p.run();
  }
}