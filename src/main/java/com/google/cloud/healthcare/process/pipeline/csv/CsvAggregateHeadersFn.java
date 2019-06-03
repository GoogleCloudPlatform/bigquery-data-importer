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

package com.google.cloud.healthcare.process.pipeline.csv;

import com.google.common.collect.Maps;
import java.util.Map;
import org.apache.beam.sdk.transforms.Combine.CombineFn;
import org.apache.beam.sdk.values.KV;

/**
 * For aggregating all headers into a {@link Map} for later lookup.
 */
public class CsvAggregateHeadersFn extends
    CombineFn<KV<String, String[]>, Map<String, String[]>, Map<String, String[]>> {

  @Override
  public Map<String, String[]> createAccumulator() {
    return Maps.newHashMap();
  }

  @Override
  public Map<String, String[]> addInput(Map<String, String[]> accumulator,
      KV<String, String[]> input) {
    accumulator.put(input.getKey(), input.getValue());
    return accumulator;
  }

  @Override
  public Map<String, String[]> mergeAccumulators(Iterable<Map<String, String[]>> accumulators) {
    Map<String, String[]> result = Maps.newHashMap();
    for (Map<String, String[]> headers : accumulators) {
      result.putAll(headers);
    }
    return result;
  }

  @Override
  public Map<String, String[]> extractOutput(Map<String, String[]> accumulator) {
    return accumulator;
  }
}
