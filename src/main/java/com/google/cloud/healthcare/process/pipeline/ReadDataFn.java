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

import java.io.IOException;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.values.KV;

/**
 * A {@link DoFn} that reads all data given a {@link Chunk} representation.
 */
public class ReadDataFn extends DoFn<KV<String, Chunk>, KV<String, byte[]>> {

  @ProcessElement
  public void read(ProcessContext ctx) throws IOException {
    KV<String, Chunk> input = ctx.element();
    ctx.output(KV.of(input.getKey(), input.getValue().readAll()));
  }
}
