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

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;

import java.io.IOException;
import java.nio.channels.SeekableByteChannel;
import org.apache.commons.compress.utils.SeekableInMemoryByteChannel;
import org.junit.Test;

/** Test for ByteReader */
public class ByteReaderTest {

  private static final String LINE_FEED_SEPARATED = "abc, cbd\ncab, abc\n, abc";
  private static final String LINE_FEED_AT_FIRST = "\ncab, abc\n, abc\n";
  private static final String CARRIAGE_RETURN_SEPARATED = "abc, cbd\r\rcab, abc\r";
  private static final String CARRIAGE_RETURN_AND_LINE_FEED_SEPARATED =
      "\r\nabc,\r\ncbd\r\ncab, abc\r\n";
  private static final String MIXED_SEPARATED =
      "\rabc,\ncbd\r\ncab, abc\r\n";
  private static final String ALL_EMPTY = "\r\r\r\r\r";

  @Test
  public void readUntil_endBeforeCurrPos_noneReturned() throws IOException {
    SeekableByteChannel channel = new SeekableInMemoryByteChannel(LINE_FEED_SEPARATED.getBytes());
    channel.position(10);
    ByteReader reader = new ByteReader(channel);
    assertEquals("None was returned", 0, reader.readUntil(5).length);
  }

  @Test
  public void readUntil_endSameAsCurrPos_noneReturned() throws IOException {
    SeekableByteChannel channel = new SeekableInMemoryByteChannel(LINE_FEED_SEPARATED.getBytes());
    channel.position(10);
    ByteReader reader = new ByteReader(channel);
    assertEquals("None was returned", 0, reader.readUntil(10).length);
  }

  @Test
  public void readUntil_expectedResult() throws IOException {
    SeekableByteChannel channel = new SeekableInMemoryByteChannel(LINE_FEED_SEPARATED.getBytes());
    channel.position(3);
    ByteReader reader = new ByteReader(channel);
    assertArrayEquals("None was returned", ", cbd\nc".getBytes(), reader.readUntil(10));
  }

  @Test
  public void readUntil_eof_expectedResult() throws IOException {
    SeekableByteChannel channel = new SeekableInMemoryByteChannel(LINE_FEED_SEPARATED.getBytes());
    channel.position(3);
    ByteReader reader = new ByteReader(channel);
    assertArrayEquals("None was returned", ", cbd\ncab, abc\n, abc".getBytes(),
        reader.readUntil(100));
  }

  @Test(expected = IOException.class)
  public void readLine_closedChannel_throwException() throws IOException {
    SeekableByteChannel channel = new SeekableInMemoryByteChannel();
    channel.close();
    ByteReader reader = new ByteReader(channel);
    reader.readLine();
  }

  @Test
  public void readLine_returnExpectedContent() throws IOException {
    try (SeekableByteChannel channel =
        new SeekableInMemoryByteChannel(LINE_FEED_SEPARATED.getBytes())) {

      ByteReader reader = new ByteReader(channel);
      assertEquals("First line matches.", "abc, cbd", new String(reader.readLine()));
      assertEquals("Second line matches.", "cab, abc", new String(reader.readLine()));
      assertEquals("Last line matches.", ", abc", new String(reader.readLine()));
    }
  }

  @Test
  public void readLine_returnExpectedContent2() throws IOException {
    try (SeekableByteChannel channel =
        new SeekableInMemoryByteChannel(LINE_FEED_AT_FIRST.getBytes())) {

      ByteReader reader = new ByteReader(channel);
      assertEquals("First line matches.", "", new String(reader.readLine()));
      assertEquals("Second line matches.", "cab, abc", new String(reader.readLine()));
      assertEquals("Last line matches.", ", abc", new String(reader.readLine()));
    }
  }

  @Test
  public void readLine_returnExpectedContent3() throws IOException {
    try (SeekableByteChannel channel =
        new SeekableInMemoryByteChannel(CARRIAGE_RETURN_SEPARATED.getBytes())) {

      ByteReader reader = new ByteReader(channel);
      assertEquals("First line matches.", "abc, cbd", new String(reader.readLine()));
      assertEquals("Second line matches.", "", new String(reader.readLine()));
      assertEquals("Last line matches.", "cab, abc", new String(reader.readLine()));
    }
  }

  @Test
  public void readLine_returnExpectedContent4() throws IOException {
    try (SeekableByteChannel channel =
        new SeekableInMemoryByteChannel(CARRIAGE_RETURN_AND_LINE_FEED_SEPARATED.getBytes())) {

      ByteReader reader = new ByteReader(channel);
      assertEquals("First line matches.", "", new String(reader.readLine()));
      assertEquals("Second line matches.", "abc,", new String(reader.readLine()));
      assertEquals("Third line matches.", "cbd", new String(reader.readLine()));
      assertEquals("Last line matches.", "cab, abc", new String(reader.readLine()));
    }
  }

  @Test
  public void readLine_returnExpectedContent5() throws IOException {
    try (SeekableByteChannel channel =
        new SeekableInMemoryByteChannel(MIXED_SEPARATED.getBytes())) {

      ByteReader reader = new ByteReader(channel);
      assertEquals("First line matches.", "", new String(reader.readLine()));
      assertEquals("Second line matches.", "abc,", new String(reader.readLine()));
      assertEquals("Third line matches.", "cbd", new String(reader.readLine()));
      assertEquals("Last line matches.", "cab, abc", new String(reader.readLine()));
    }
  }

  @Test
  public void readLine_returnExpectedContent6() throws IOException {
    try (SeekableByteChannel channel =
        new SeekableInMemoryByteChannel(ALL_EMPTY.getBytes())) {

      ByteReader reader = new ByteReader(channel);
      assertEquals("First line matches.", "", new String(reader.readLine()));
      assertEquals("Second line matches.", "", new String(reader.readLine()));
      assertEquals("Third line matches.", "", new String(reader.readLine()));
      assertEquals("Fourth line matches.", "", new String(reader.readLine()));
      assertEquals("Last line matches.", "", new String(reader.readLine()));
    }
  }

  @Test
  public void readLine_returnExpectedContent7() throws IOException {
    try (SeekableByteChannel channel =
        new SeekableInMemoryByteChannel(MIXED_SEPARATED.getBytes())) {

      ByteReader reader = new ByteReader(channel);
      channel.position(2);
      assertEquals("Line matches.", "bc,", new String(reader.readLine()));
      channel.position(5);
      assertEquals("Line matches.", "", new String(reader.readLine()));
      assertEquals("Line matches.", "cbd", new String(reader.readLine()));
      channel.position(5);
      assertEquals("Line matches.", "", new String(reader.readLine()));
      assertEquals("Line matches.", "cbd", new String(reader.readLine()));
      assertEquals("Last line matches.", "cab, abc", new String(reader.readLine()));
    }
  }
}