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
import com.google.common.base.Preconditions;
import com.google.common.io.ByteStreams;
import java.io.IOException;
import java.io.InputStream;
import java.io.Serializable;
import java.nio.channels.Channels;
import java.nio.channels.ReadableByteChannel;

/**
 * Chunk represents a chunk of a larger file.
 */
public class Chunk implements Serializable {
  private final InputReader reader;
  private final Range range;

  public Chunk(InputReader reader, Range range) {
    this.reader = reader;
    this.range = range;
  }

  byte[] readAll() throws IOException {
    if (range.getSize() > Integer.MAX_VALUE) {
      throw new IllegalArgumentException(
          String.format("Chunk is too large to read, size: %d", range.getSize()));
    }

    int size = (int) range.getSize();
    byte[] content = new byte[size];

    try (ReadableByteChannel readCh = reader.getReadChannel()) {
      InputStream is = Channels.newInputStream(readCh);
      ByteStreams.skipFully(is, range.getStart());
      ByteStreams.readFully(is, content, 0, size);
    }
    return content;
  }

  @Override
  public boolean equals(Object obj) {
    if (obj == null) {
      return false;
    }

    if (!(obj instanceof Chunk)) {
      return false;
    }

    Chunk chunk = (Chunk) obj;
    return reader.equals(chunk.reader) && range.equals(chunk.range);
  }

  @Override
  public int hashCode() {
    return 31 * reader.hashCode() + range.hashCode();
  }

  /**
   * Represents a range of bytes to read..
   */
  public static class Range implements Serializable {
    // Start position, inclusive.
    private long start;
    // End position, exclusive.
    private long end;

    public Range(long start, long end) {
      Preconditions.checkArgument(end > start, "end has to be greater than start.");
      this.start = start;
      this.end = end;
    }

    long getSize() {
      return end - start;
    }

    long getStart() {
      return start;
    }

    long getEnd() {
      return end;
    }

    @Override
    public boolean equals(Object obj) {
      if (obj == null) {
        return false;
      }

      if (!(obj instanceof Range)) {
        return false;
      }

      Range range = (Range) obj;
      return start == range.getStart() && end == range.getEnd();
    }

    @Override
    public int hashCode() {
      return 31 * Long.valueOf(start).hashCode() + Long.valueOf(end).hashCode();
    }
  }
}
