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

import com.google.cloud.healthcare.process.schema.FieldType;
import com.google.cloud.healthcare.process.schema.SchemaUtil;
import com.google.common.collect.Maps;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.stream.Collectors;
import org.apache.beam.sdk.transforms.Combine.CombineFn;
import org.apache.beam.sdk.values.KV;

/**
 * A {@link CombineFn} which merges all pieces of schemas and generates a complete schema for
 * importing to BigQuery later.
 */
public class CsvMergeSchemaFn extends CombineFn<KV<String, List<FieldType>>,
    Map<String, List<FieldType>>, Map<String, FieldType[]>> {

  @Override
  public Map<String, List<FieldType>> createAccumulator() {
    return Maps.newHashMap();
  }

  @Override
  public Map<String, List<FieldType>> addInput(Map<String, List<FieldType>> accumulator,
      KV<String, List<FieldType>> input) {
    addOrMerge(accumulator, input.getKey(), input.getValue());
    return accumulator;
  }

  @Override
  public Map<String, List<FieldType>> mergeAccumulators(Iterable<Map<String,
      List<FieldType>>> accumulators) {
    Map<String, List<FieldType>> result = Maps.newHashMap();
    for (Map<String, List<FieldType>> acc : accumulators) {
      for (String name : acc.keySet()) {
        addOrMerge(result, name, acc.get(name));
      }
    }
    return result;
  }

  private static void addOrMerge(Map<String, List<FieldType>> result, String name,
      List<FieldType> value) {
    List<FieldType> schema = result.get(name);
    if (schema == null) {
      result.put(name, value);
    } else {
      result.put(name, SchemaUtil.merge(schema, value));
    }
  }

  @Override
  public Map<String, FieldType[]> extractOutput(Map<String, List<FieldType>> accumulator) {
    return accumulator.entrySet()
        .stream()
        .collect(Collectors.toMap(
            Entry::getKey,
            e -> e.getValue().toArray(new FieldType[e.getValue().size()])));
  }
}
