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

import com.google.cloud.healthcare.config.CsvConfiguration;
import com.google.cloud.healthcare.io.ByteReader;
import com.google.common.collect.Sets;
import java.io.IOException;
import java.nio.channels.SeekableByteChannel;
import java.util.Map;
import java.util.Set;
import javax.annotation.Nullable;
import org.apache.beam.sdk.io.FileIO.ReadableFile;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollectionView;

/**
 * Base class for splitting CSV files.
 */
public abstract class CsvSplitFn extends DoFn<ReadableFile, KV<String, Set<Long>>> {
  protected static int CHUNK_SIZE = 2 * 1024 * 1024; // 2MB.

  private PCollectionView<Map<String, String[]>> headersView;
  protected final CsvConfiguration config;
  protected String[] headers;

  public CsvSplitFn(CsvConfiguration config,
      PCollectionView<Map<String, String[]>> headersView) {
    this.config = config;
    this.headersView = headersView;
  }

  // TODO(b/122103201): Parallelize the split process.
  @ProcessElement
  public void split(ProcessContext ctx) throws IOException {
    ReadableFile file = ctx.element();
    String name = file.getMetadata().resourceId().toString();
    headers = ctx.sideInput(headersView).get(name);

    SeekableByteChannel ch = file.openSeekable();

    long size = file.getMetadata().sizeBytes();
    int splitPoints = (int) (size / CHUNK_SIZE);

    Set<Long> points = Sets.newHashSet();
    seekStartPoint(ch);
    points.add(ch.position());

    for (int i = 0; i < splitPoints; i++) {
      Long splitPoint = calcSplitPoint(ch,
          startSplitCheckPosition(i),
          Math.min(startSplitCheckPosition(i + 1), size));
      if (splitPoint != null) {
        points.add(splitPoint);
      }
    }

    points.add(size);
    ctx.output(KV.of(name, points));
  }

  // Calculates the split point in the range specified as parameters (start inclusive, end
  // exclusive). If no valid split point is found, null will be returned.
  @Nullable
  protected abstract Long calcSplitPoint(SeekableByteChannel ch, long start, long end)
      throws IOException;

  /**
   * Returns the position of the first byte on second row, since we require first row to be headers.
   */
  private void seekStartPoint(SeekableByteChannel ch) throws IOException {
    if (ch.position() != 0L) {
      throw new IllegalStateException("Position of the cursor has to be at the beginning of file");
    }
    new ByteReader(ch).readLine();
  }

  /** Calculates the starting search position for each split point. */
  private long startSplitCheckPosition(int i) {
    return (i + 1) * (long) CHUNK_SIZE;
  }
}
