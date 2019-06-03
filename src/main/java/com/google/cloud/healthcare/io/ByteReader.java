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

package com.google.cloud.healthcare.io;

import com.google.common.collect.Lists;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.SeekableByteChannel;
import java.util.List;

/**
 * Wrapper around a {@link SeekableByteChannel} with some useful methods.
 */
public class ByteReader {

  private static final int BUFFER_SIZE = 8192;

  public static final byte NEWLINE_FEED = 0xA;
  public static final byte CARRIAGE_RETURN = 0XD;

  private final SeekableByteChannel channel;

  public ByteReader(SeekableByteChannel ch) {
    this.channel = ch;
  }

  /**
   * Reads a line from the channel, the read byte array doesn't include line feed or carriage
   * return, the position of the channel is set to after the line terminators.
   */
  public byte[] readLine() throws IOException {
    long currPos = channel.position();

    ByteBuffer byteBuffer = ByteBuffer.allocate(BUFFER_SIZE);
    List<Byte> readBytes = Lists.newArrayList();

    boolean carriageReturn = false;
    while (channel.read(byteBuffer) > 0) {
      byteBuffer.flip();

      while (byteBuffer.hasRemaining()) {
        byte b = byteBuffer.get();

        if (b == NEWLINE_FEED || carriageReturn) {
          long nextPos = currPos + readBytes.size();
          // Exclude the line feed and previous carriage return.
          if (b == NEWLINE_FEED) {
            nextPos++;
          }
          if (carriageReturn) {
            nextPos++;
          }

          channel.position(nextPos);
          return toArray(readBytes);
        } else if (b == CARRIAGE_RETURN) {
          // Mark carriage return, check if next byte is line feed.
          carriageReturn = true;
        } else {
          readBytes.add(b);
        }
      }

      byteBuffer.clear();
    }

    return toArray(readBytes);
  }

  /**
   * Reads all the bytes until end (exclusive).
   */
  public byte[] readUntil(long end) throws IOException {
    long currPos = channel.position();

    ByteBuffer byteBuffer = ByteBuffer.allocate(BUFFER_SIZE);
    List<Byte> readBytes = Lists.newArrayList();

    while (channel.read(byteBuffer) > 0) {
      byteBuffer.flip();

      while (byteBuffer.hasRemaining()) {
        if (readBytes.size() >= end - currPos) {
          return toArray(readBytes);
        }

        readBytes.add(byteBuffer.get());
      }

      byteBuffer.clear();
    }

    return toArray(readBytes);
  }

  private static byte[] toArray(List<Byte> bytes) {
    byte[] result = new byte[bytes.size()];
    for (int i = 0; i < bytes.size(); i++) {
      result[i] = bytes.get(i);
    }
    return result;
  }
}
