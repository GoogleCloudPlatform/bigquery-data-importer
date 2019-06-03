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
import com.google.common.base.Strings;
import com.google.common.collect.Lists;
import com.google.common.io.ByteStreams;
import java.io.IOException;
import java.nio.channels.Channels;
import java.nio.channels.WritableByteChannel;
import java.nio.file.Paths;
import java.util.List;
import java.util.zip.GZIPInputStream;
import org.apache.commons.io.FilenameUtils;

class GZipHandler extends BaseHandler {

  GZipHandler(OutputWriterFactory factory) {
    super(factory);
  }

  @Override
  public List<String> handle(InputReader reader) throws IOException {
    String origName = reader.getName();
    if (Strings.isNullOrEmpty(origName)) {
      throw new IllegalStateException("Cannot get the name of the input file.");
    }

    String outputName = FilenameUtils.removeExtension(Paths.get(origName).getFileName().toString());
    OutputWriter writer = factory.getOutputWriter(outputName);
    try (GZIPInputStream gis =
        new GZIPInputStream(Channels.newInputStream(reader.getReadChannel()));
        WritableByteChannel writeChannel = writer.getWriteChannel()) {
      ByteStreams.copy(Channels.newChannel(gis), writeChannel);
      return Lists.newArrayList(writer.getName());
    }
  }
}
