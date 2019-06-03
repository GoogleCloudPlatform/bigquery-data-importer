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
import org.apache.commons.compress.archivers.tar.TarArchiveEntry;
import org.apache.commons.compress.archivers.tar.TarArchiveInputStream;
import org.apache.commons.compress.compressors.gzip.GzipCompressorInputStream;

class TarHandler extends BaseHandler {

  TarHandler(OutputWriterFactory factory) {
    super(factory);
  }

  @Override
  public List<String> handle(InputReader reader) throws IOException {
    List<String> files = Lists.newArrayList();

    // TODO(b/121029418): Not all TAR archives are compressed, we need to check the filenames to be
    // sure. Also consider refactoring the handlers to adopt the decorator pattern.
    try (TarArchiveInputStream tis = new TarArchiveInputStream(
        new GzipCompressorInputStream(Channels.newInputStream(reader.getReadChannel())))) {

      TarArchiveEntry entry;
      while ((entry = tis.getNextTarEntry()) != null) {
        if (!entry.isDirectory()) {
          OutputWriter writer = factory.getOutputWriter(Paths.get(entry.getName()).toString());
          try (WritableByteChannel writeChannel = writer.getWriteChannel()) {
            ByteStreams.copy(Channels.newChannel(tis), writeChannel);
            files.add(writer.getName());
          }
        }
      }
    }

    return files;
  }
}
