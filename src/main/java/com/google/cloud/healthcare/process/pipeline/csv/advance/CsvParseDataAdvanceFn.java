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

package com.google.cloud.healthcare.process.pipeline.csv.advance;

import com.google.cloud.healthcare.config.CsvConfiguration;
import java.util.Arrays;
import java.util.regex.Pattern;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.values.KV;

/**
 * Parses a chunk of data with the regular expressions. Only the default encoding on JVM (UTF-8) is
 * supported.
 */
public class CsvParseDataAdvanceFn extends DoFn<KV<String, byte[]>, KV<String, String[]>> {
  private final CsvConfiguration config;

  public CsvParseDataAdvanceFn(CsvConfiguration config) {
    this.config = config;
  }

  @ProcessElement
  public void parse(ProcessContext ctx) {
    Pattern recordSplitPattern = config.getRecordSeparatorRegex();
    Pattern fieldSplitPattern = config.getDelimiterRegex();

    KV<String, byte[]> input = ctx.element();
    String name = input.getKey();
    byte[] bytes = input.getValue();

    // TODO(b/123357928): Support other encodings.
    String content = new String(bytes);
    String[] records = recordSplitPattern.split(content);
    Arrays.stream(records)
        .map(fieldSplitPattern::split).forEach(fields -> ctx.output(KV.of(name, fields)));
  }
}
