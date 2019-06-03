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
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.channels.Channels;
import java.nio.channels.WritableByteChannel;
import java.util.zip.ZipEntry;
import java.util.zip.ZipOutputStream;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mock;

public class ZipHandlerTest {

  private static final String[] CONTENT = new String[] {
      "first_entry", "second_entry", "third_entry"
  };

  @Mock
  private OutputWriterFactory factory;

  @Mock
  private InputReader reader;

  private byte[] zipCompressedData;

  @Before
  public void setUp() throws Exception {
    initMocks(this);

    ByteArrayOutputStream os = new ByteArrayOutputStream();
    ZipOutputStream zos = new ZipOutputStream(os);

    for (String entry : CONTENT) {
      zos.putNextEntry(new ZipEntry(entry));
      zos.write(entry.getBytes());
    }
    zos.closeEntry();
    zos.flush();
    zos.close();

    zipCompressedData = os.toByteArray();
  }

  @Test
  public void handleZipEntries_contentMatch() throws IOException {
    when(reader.getReadChannel()).thenReturn(
        Channels.newChannel(new ByteArrayInputStream(zipCompressedData)));

    ByteArrayOutputStream[] oss = new ByteArrayOutputStream[] {
        new ByteArrayOutputStream(), new ByteArrayOutputStream(), new ByteArrayOutputStream()
    };

    WritableByteChannel[] wbc = new WritableByteChannel[] {
        Channels.newChannel(oss[0]),
        Channels.newChannel(oss[1]),
        Channels.newChannel(oss[2])
    };

    for (int i = 0; i < CONTENT.length; i++) {
      OutputWriter ow = mock(OutputWriter.class);
      when(ow.getWriteChannel()).thenReturn(wbc[i]);
      when(factory.getOutputWriter(CONTENT[i])).thenReturn(ow);
    }

    new ZipHandler(factory).handle(reader);

    for (int i = 0; i < CONTENT.length; i++) {
      assertEquals("Individual entry content should match",
          CONTENT[i], new String(oss[i].toByteArray()));
    }
  }
}