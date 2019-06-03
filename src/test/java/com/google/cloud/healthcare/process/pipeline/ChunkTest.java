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

import static org.junit.Assert.assertEquals;

import com.google.cloud.healthcare.io.InputReader;
import com.google.cloud.healthcare.process.pipeline.Chunk.Range;
import com.google.common.base.Strings;
import java.io.ByteArrayInputStream;
import java.io.EOFException;
import java.io.IOException;
import java.nio.channels.Channels;
import java.nio.channels.ReadableByteChannel;
import javax.annotation.Nullable;
import org.junit.Test;

/** Test for Chunk */
public class ChunkTest {

  private static final String DATA = "Google Cloud Platform, offered by Google, is a suite of cloud"
      + " computing services that runs on the same infrastructure that Google uses internally for"
      + " its end-user products, such as Google Search and YouTube.";

  private static final String LARGE_DATA = Strings.repeat(DATA, 1000_000);

  @Test
  public void readAll_returnExpectedRange() throws IOException {
    Chunk chunk = new Chunk(new InputReader() {
      @Override
      public String getContentType() {
        return "";
      }

      @Override
      public ReadableByteChannel getReadChannel() {
        return Channels.newChannel(new ByteArrayInputStream(DATA.getBytes()));
      }

      @Override
      public long getSize() {
        return 0;
      }

      @Nullable
      @Override
      public String getName() {
        return null;
      }
    }, new Range(20, 30));

    assertEquals("Read content should match.", "m, offered", new String(chunk.readAll()));
  }

  @Test(expected = EOFException.class)
  public void readAll_outOfRange_throwException() throws IOException {
    Chunk chunk = new Chunk(new InputReader() {
      @Override
      public String getContentType() {
        return "";
      }

      @Override
      public ReadableByteChannel getReadChannel() {
        return Channels.newChannel(new ByteArrayInputStream(DATA.getBytes()));
      }

      @Override
      public long getSize() {
        return 0;
      }

      @Nullable
      @Override
      public String getName() {
        return null;
      }
    }, new Range(2000, 3000));

    chunk.readAll();
  }

  @Test
  public void readAll_largeInput_returnExpectedContent() throws IOException {
    Chunk chunk = new Chunk(new InputReader() {
      @Override
      public String getContentType() {
        return "";
      }

      @Override
      public ReadableByteChannel getReadChannel() {
        return Channels.newChannel(new ByteArrayInputStream(LARGE_DATA.getBytes()));
      }

      @Override
      public long getSize() {
        return 0;
      }

      @Nullable
      @Override
      public String getName() {
        return null;
      }
    }, new Range(1005000, 1005500));

    String expected = "atform, offered by Google, is a suite of cloud computing services that runs "
        + "on the same infrastructure that Google uses internally for its end-user products, such "
        + "as Google Search and YouTube.Google Cloud Platform, offered by Google, is a suite of "
        + "cloud computing services that runs on the same infrastructure that Google uses "
        + "internally for its end-user products, such as Google Search and YouTube.Google Cloud "
        + "Platform, offered by Google, is a suite of cloud computing services that runs on the "
        + "sam";
    assertEquals("Read content should match.", expected, new String(chunk.readAll()));
  }
}