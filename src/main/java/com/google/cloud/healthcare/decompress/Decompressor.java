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
import java.io.IOException;
import java.util.List;
import java.util.Optional;

/**
 * Decompressor tries to decompress content from an {@link InputReader} and writes decompressed
 * data to a different location.
 */
public class Decompressor {

  private OutputWriterFactory factory;

  public Decompressor(OutputWriterFactory factory) {
    this.factory = factory;
  }

  /**
   * Decompresses the content provided by the {@link InputReader} and writes the decompressed data
   * to a location specified by the {@link OutputWriter}.
   *
   * @return a boolean indicating whether the content is decompressed.
   * @throws IOException
   */
  public List<String> decompress(InputReader reader) throws IOException {
    String contentType = reader.getContentType();
    Optional<CompressionAlgorithm> algoOpt = CompressionAlgorithm.valueOfMimeType(contentType);
    // Check if the file is compressed.
    if (!algoOpt.isPresent()) {
      return Lists.newArrayList(reader.getName());
    }

    CompressionAlgorithm algo = algoOpt.get();
    if (!algo.isSupported()) {
      throw new RuntimeException(contentType + " is not supported.");
    }

    switch (algo) {
      case GZIP:
        return new GZipHandler(factory).handle(reader);
      case ZIP:
        return new ZipHandler(factory).handle(reader);
      case TAR:
        return new TarHandler(factory).handle(reader);
      case LZ4:
        return new LZ4Handler(factory).handle(reader);
      default:
        // Should not reach here.
        throw new RuntimeException(
            String.format("Reached a branch %s which should not be visited.", algo));
    }
  }
}
