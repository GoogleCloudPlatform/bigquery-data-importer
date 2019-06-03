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
import com.google.cloud.healthcare.io.ByteReader;
import com.google.cloud.healthcare.process.pipeline.csv.CsvSplitFn;
import java.io.IOException;
import java.nio.channels.SeekableByteChannel;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import javax.annotation.Nullable;
import org.apache.beam.sdk.values.PCollectionView;

/**
 * Splits a non-standard CSV file into chunks. Users of the pipeline need to provide two
 * regular expression, for record separator and line delimiter respectively.
 */
public class GcsSplitCsvAdvanceFn extends CsvSplitFn {

  public GcsSplitCsvAdvanceFn(CsvConfiguration config,
      PCollectionView<Map<String, String[]>> headersView) {
    super(config, headersView);
  }

  @Nullable
  @Override
  protected Long calcSplitPoint(SeekableByteChannel ch, long start, long end) throws IOException {
    Pattern pattern = config.getRecordSeparatorRegex();
    ch.position(start);
    ByteReader reader = new ByteReader(ch);

    byte[] bytes = reader.readUntil(end);
    // TODO(b/123357928): Support encodings other than UTF-8.
    String content = new String(bytes);
    Matcher matcher = pattern.matcher(content);
    if (matcher.find()) {
      return start + content.substring(0, matcher.end()).getBytes().length;
    }

    return null;
  }
}
