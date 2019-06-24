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
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.when;
import static org.mockito.MockitoAnnotations.initMocks;

import avro.shaded.com.google.common.collect.Sets;
import com.google.cloud.healthcare.config.GcpConfiguration;
import com.google.cloud.healthcare.io.GcsInputReader;
import com.google.cloud.healthcare.process.schema.GcpUtil;
import java.io.ByteArrayInputStream;
import java.nio.channels.Channels;
import java.nio.channels.ReadableByteChannel;
import java.util.Set;
import org.apache.beam.sdk.coders.SerializableCoder;
import org.apache.beam.sdk.testing.PAssert;
import org.apache.beam.sdk.testing.TestPipeline;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.TypeDescriptors;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.powermock.api.mockito.PowerMockito;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;

/** Test for {@link GcsReadChunksFn}. */
@RunWith(PowerMockRunner.class)
@PrepareForTest({GcpUtil.class})
public class GcsReadChunksFnTest {

  private static final String BUCKET1 = "bucket";
  private static final String PATH1 = "file.csv";
  private static final String FILENAME1 = String.format("gs://%s/%s", BUCKET1, PATH1);
  private static final String FILE_CONTENT1 = "ABCDEFGHIJKLMNOPQRSTUVWXYZ";
  private ReadableByteChannel readChannel1 =
      Channels.newChannel(new ByteArrayInputStream(FILE_CONTENT1.getBytes()));

  private static final String BUCKET2 = "mybucket";
  private static final String PATH2 = "path/to/file.csv";
  private static final String FILENAME2 = String.format("gs://%s/%s", BUCKET2, PATH2);
  private static final String FILE_CONTENT2 = "ZYXWVUTSRQPONMLKJIHGFEDCBA";
  private ReadableByteChannel readChannel2 =
      Channels.newChannel(new ByteArrayInputStream(FILE_CONTENT2.getBytes()));

  @Rule public final transient TestPipeline p = TestPipeline.create();

  @Mock private GcsInputReader reader1;

  @Mock private GcsInputReader reader2;

  @Before
  public void setUp() {
    initMocks(this);
    PowerMockito.mockStatic(GcpUtil.class);
    when(GcpUtil.openGcsFile(any(), eq(FILENAME1))).thenReturn(readChannel1);
    when(GcpUtil.openGcsFile(any(), eq(FILENAME2))).thenReturn(readChannel2);
  }

  @Test
  public void generate_twoElements_oneByteArray() {
    PCollection<KV<String, Set<Long>>> input =
        p.apply(Create.of(KV.of(FILENAME1, Sets.newHashSet(0L, 10L))));
    PCollection<KV<String, byte[]>> output =
        input.apply(ParDo.of(new GcsReadChunksFn(GcpConfiguration.getInstance())));
    PAssert.that(output).containsInAnyOrder(KV.of(FILENAME1, "ABCDEFGHIJ".getBytes()));
    p.run();
  }

  @Test
  public void generate_multipleElements_expectedByteArrays() {
    PCollection<KV<String, Set<Long>>> input =
        p.apply(
            Create.of(
                KV.of(FILENAME1, Sets.newHashSet(0L, 20L)),
                KV.of(FILENAME2, Sets.newHashSet(26L, 0L, 1L, 14L, 10L))));
    PCollection<KV<String, byte[]>> output =
        input.apply(ParDo.of(new GcsReadChunksFn(GcpConfiguration.getInstance())));
    PAssert.that(output)
        .containsInAnyOrder(
            KV.of(FILENAME1, "ABCDEFGHIJKLMNOPQRST".getBytes()),
            KV.of(FILENAME2, "Z".getBytes()),
            KV.of(FILENAME2, "YXWVUTSRQ".getBytes()),
            KV.of(FILENAME2, "PONM".getBytes()),
            KV.of(FILENAME2, "LKJIHGFEDCBA".getBytes()));
    p.run();
  }

  @Test
  public void read_nothing_returnZeroResult() {
    PCollection<KV<String, Set<Long>>> input =
        p.apply(
            Create.empty(
                SerializableCoder.of(
                    TypeDescriptors.kvs(
                        TypeDescriptors.strings(),
                        TypeDescriptors.sets(TypeDescriptors.longs())))));
    PCollection<KV<String, byte[]>> output =
        input.apply(ParDo.of(new GcsReadChunksFn(GcpConfiguration.getInstance())));
    PAssert.that(output).empty();
    p.run();
  }

  @Test
  public void read_returnExpectedByteArrays() {
    PCollection<KV<String, Set<Long>>> input =
        p.apply(
            Create.of(
                KV.of(FILENAME1, Sets.newHashSet(0L, (long) FILE_CONTENT1.length())),
                KV.of(FILENAME2, Sets.newHashSet(0L, (long) FILE_CONTENT2.length()))));
    PCollection<KV<String, byte[]>> output =
        input.apply(ParDo.of(new GcsReadChunksFn(GcpConfiguration.getInstance())));
    PAssert.that(output)
        .containsInAnyOrder(
            KV.of(FILENAME1, FILE_CONTENT1.getBytes()), KV.of(FILENAME2, FILE_CONTENT2.getBytes()));
    p.run();
  }
}
