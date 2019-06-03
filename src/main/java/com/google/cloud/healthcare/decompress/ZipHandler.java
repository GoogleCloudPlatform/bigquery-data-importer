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

import com.google.cloud.healthcare.io.InputReader;
import com.google.cloud.healthcare.io.OutputWriter;
import com.google.cloud.healthcare.io.OutputWriterFactory;
import com.google.common.collect.Lists;
import com.google.common.io.ByteStreams;
import java.io.IOException;
import java.nio.channels.Channels;
import java.nio.channels.WritableByteChannel;
import java.nio.file.Paths;
import java.util.List;
import java.util.zip.ZipEntry;
import java.util.zip.ZipInputStream;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Handles ZIP compressed files. ZIP is special because it is also an archive format.
 */
class ZipHandler extends BaseHandler {
  private static final Logger LOG = LoggerFactory.getLogger(ZipHandler.class);

  ZipHandler(OutputWriterFactory factory) {
    super(factory);
  }

  @Override
  public List<String> handle(InputReader reader) throws IOException {
    ZipInputStream zis = new ZipInputStream(Channels.newInputStream(reader.getReadChannel()));

    List<String> files = Lists.newArrayList();
    ZipEntry entry = zis.getNextEntry();
    while (entry != null) {
      if (!entry.isDirectory()) {
        OutputWriter writer = factory.getOutputWriter(Paths.get(entry.getName()).toString());
        try (WritableByteChannel writeChannel = writer.getWriteChannel()) {
          ByteStreams.copy(Channels.newChannel(zis), writeChannel);
          files.add(writer.getName());
        }
      }

      entry = zis.getNextEntry();
    }

    zis.closeEntry();
    zis.close();
    return files;
  }
}
