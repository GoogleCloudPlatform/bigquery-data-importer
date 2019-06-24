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
import com.google.common.collect.Lists;
import com.google.common.collect.Streams;
import java.util.List;
import org.apache.beam.sdk.transforms.Combine.CombineFn;

/**
 * A {@link CombineFn} which merges all pieces of schemas and generates a complete schema for
 * importing to BigQuery later.
 */
public class CsvMergeSchemaFn extends CombineFn<List<FieldType>, List<FieldType>, FieldType[]> {

  @Override
  public List<FieldType> createAccumulator() {
    return Lists.newArrayList();
  }

  @Override
  public List<FieldType> addInput(List<FieldType> accumulator, List<FieldType> input) {
    return SchemaUtil.merge(accumulator, input);
  }

  @Override
  public List<FieldType> mergeAccumulators(Iterable<List<FieldType>> accumulators) {
    return Streams.stream(accumulators).reduce(SchemaUtil::merge).get();
  }

  @Override
  public FieldType[] extractOutput(List<FieldType> accumulator) {
    return accumulator.toArray(new FieldType[0]);
  }
}
