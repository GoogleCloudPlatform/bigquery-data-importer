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

import com.google.cloud.healthcare.config.GcpConfiguration;
import com.google.cloud.healthcare.process.schema.GcpUtil;
import com.google.common.collect.Lists;
import com.google.common.io.ByteStreams;
import java.io.IOException;
import java.io.InputStream;
import java.nio.channels.Channels;
import java.nio.channels.ReadableByteChannel;
import java.util.Collections;
import java.util.List;
import java.util.Set;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.values.KV;

/** A beam {@link DoFn} that reads chunked data from GCS according to split points. */
public class GcsReadChunksFn extends DoFn<KV<String, Set<Long>>, KV<String, byte[]>> {

  private final GcpConfiguration config;

  public GcsReadChunksFn(GcpConfiguration config) {
    this.config = config;
  }

  @ProcessElement
  public void generate(ProcessContext ctx) {
    KV<String, Set<Long>> input = ctx.element();

    List<Long> splitPoints = Lists.newArrayList(input.getValue());
    Collections.sort(splitPoints);

    String name = input.getKey();

    try (ReadableByteChannel channel = GcpUtil.openGcsFile(config.getCredentials(), name);
        InputStream is = Channels.newInputStream(channel)) {
      ByteStreams.skipFully(is, splitPoints.get(0));
      for (int i = 0; i < splitPoints.size() - 1; i++) {
        int len = (int) (splitPoints.get(i + 1) - splitPoints.get(i));
        byte[] content = new byte[len];
        ByteStreams.readFully(is, content, 0, len);
        ctx.output(KV.of(name, content));
      }
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }
}
