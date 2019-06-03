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

package com.google.cloud.healthcare.decompress;

import static org.junit.Assert.assertEquals;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.mockito.MockitoAnnotations.initMocks;

import com.google.cloud.healthcare.io.InputReader;
import com.google.cloud.healthcare.io.OutputWriter;
import com.google.cloud.healthcare.io.OutputWriterFactory;
import com.google.common.collect.Lists;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.channels.Channels;
import java.util.List;
import org.apache.commons.compress.archivers.tar.TarArchiveEntry;
import org.apache.commons.compress.archivers.tar.TarArchiveOutputStream;
import org.apache.commons.compress.compressors.gzip.GzipCompressorOutputStream;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mock;

/** Test for TarHandler. */
public class TarHandlerTest {

  private static final String[] CONTENT = new String[] {
    "first_file", "second_file", "folder/", "folder/first_file", "folder/second_file"
  };

  @Mock
  private OutputWriterFactory factory;

  @Mock
  private InputReader reader;

  private byte[] archivedData;

  @Before
  public void setUp() throws IOException {
    initMocks(this);
    ByteArrayOutputStream os = new ByteArrayOutputStream();
    TarArchiveOutputStream tos = new TarArchiveOutputStream(new GzipCompressorOutputStream(os));
    for (int i = 0; i < CONTENT.length; i++) {
        TarArchiveEntry entry = new TarArchiveEntry(CONTENT[i]);
        entry.setSize(!CONTENT[i].endsWith("/") ? CONTENT[i].length() : 0);
        tos.putArchiveEntry(entry);
        if (!CONTENT[i].endsWith("/")) {
          tos.write(CONTENT[i].getBytes());
        }
        tos.closeArchiveEntry();
    }
    tos.flush();
    tos.close();
    archivedData = os.toByteArray();
  }

  @Test
  public void handleTarEntries_contentMatch() throws IOException {
    when(reader.getReadChannel()).thenReturn(
        Channels.newChannel(new ByteArrayInputStream(archivedData)));

    ByteArrayOutputStream[] oss = new ByteArrayOutputStream[] {
        new ByteArrayOutputStream(),
        new ByteArrayOutputStream(),
        new ByteArrayOutputStream(),
        new ByteArrayOutputStream(),
        new ByteArrayOutputStream()
    };

    for (int i = 0; i < CONTENT.length; i++) {
      OutputWriter ow = mock(OutputWriter.class);
      when(ow.getName()).thenReturn(CONTENT[i]);
      when(ow.getWriteChannel()).thenReturn(Channels.newChannel(oss[i]));
      when(factory.getOutputWriter(CONTENT[i])).thenReturn(ow);
    }

    List<String> files = new TarHandler(factory).handle(reader);
    assertEquals("Paths should match",
        Lists.newArrayList("first_file", "second_file", "folder/first_file", "folder/second_file"),
        files);

    for (int i = 0; i < CONTENT.length; i++) {
      if (!CONTENT[i].endsWith("/")) {
        assertEquals("Individual entry content should match",
            CONTENT[i], new String(oss[i].toByteArray()));
      }
    }
  }
}