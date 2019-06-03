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

import com.google.cloud.healthcare.io.InputReader;
import com.google.cloud.healthcare.process.pipeline.Chunk.Range;
import java.io.Serializable;
import java.nio.channels.ReadableByteChannel;
import javax.annotation.Nullable;
import org.apache.beam.sdk.coders.SerializableCoder;
import org.apache.beam.sdk.testing.PAssert;
import org.apache.beam.sdk.testing.TestPipeline;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.TypeDescriptor;
import org.apache.beam.sdk.values.TypeDescriptors;
import org.apache.commons.compress.utils.SeekableInMemoryByteChannel;
import org.junit.Rule;
import org.junit.Test;

/** Test for ReadDataFn */
public class ReadDataFnTest {

  private static final String FILENAME1 = "file1";
  private static final String FILENAME2 = "file2";

  private static final String CHUNK_CONTENT1 = "ABC";
  private static final String CHUNK_CONTENT2 = "CDE";

  @Rule
  public final transient TestPipeline p = TestPipeline.create();

  @Test
  public void read_noChunk_returnZeroResult() {
    PCollection<KV<String, Chunk>> input =
        p.apply(Create.empty(SerializableCoder.of(TypeDescriptors.kvs(
            TypeDescriptor.of(String.class), TypeDescriptor.of(Chunk.class)))));
    PCollection<KV<String, byte[]>> output = input.apply(ParDo.of(new ReadDataFn()));
    PAssert.that(output).empty();
    p.run();
  }

  @Test
  public void read_returnExpectedByteArrays() {
    byte[] byteArr1 = CHUNK_CONTENT1.getBytes();
    byte[] byteArr2 = CHUNK_CONTENT2.getBytes();
    Chunk firstChunk = new Chunk(new ByteArrayInputReader(byteArr1), new Range(0, byteArr1.length));
    Chunk secondChunk = new Chunk(
        new ByteArrayInputReader(byteArr2), new Range(0, byteArr2.length));

    PCollection<KV<String, Chunk>> input = p.apply(Create.of(
        KV.of(FILENAME1, firstChunk), KV.of(FILENAME2, secondChunk)));
    PCollection<KV<String, byte[]>> output = input.apply(ParDo.of(new ReadDataFn()));
    PAssert.that(output).containsInAnyOrder(
        KV.of(FILENAME1, CHUNK_CONTENT1.getBytes()),
        KV.of(FILENAME2, CHUNK_CONTENT2.getBytes()));
    p.run();
  }

  /** An {@link InputReader} backed by a byte array. */
  public static class ByteArrayInputReader implements InputReader, Serializable {
    final byte[] arr;

    ByteArrayInputReader(byte[] array) {
      this.arr = array;
    }

    @Override
    public String getContentType() {
      return "";
    }

    @Override
    public ReadableByteChannel getReadChannel() {
      return new SeekableInMemoryByteChannel(arr);
    }

    @Override
    public long getSize() {
      return arr.length;
    }

    @Nullable
    @Override
    public String getName() {
      return "";
    }
  }
}