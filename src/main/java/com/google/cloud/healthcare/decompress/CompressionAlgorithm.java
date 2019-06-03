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

import java.util.Arrays;
import java.util.HashSet;
import java.util.Optional;
import java.util.Set;

/**
 * Here we are using the MIME type from GCS to determine if the file is compressed.
 *
 * Those mime types are extracted from IANA and Wikipedia:
 * https://www.iana.org/assignments/media-types/media-types.xhtml and
 * https://en.wikipedia.org/wiki/List_of_archive_formats
 */
enum CompressionAlgorithm {
  // Supported
  GZIP(true, "application/gzip", "application/x-gzip"),
  ZIP(true, "application/zip"),
  TAR(true, "application/x-tar"),
  LZ4(true, "application/x-lz4"),
  // Unsupported.
  RAR(false, "application/vnd.rar", "application/x-rar-compressed"),
  SEVEN_Z(false, "application/x-7z-compressed"),
  BZIP(false, "application/x-bzip"),
  BZIP2(false, "application/x-bzip2"),
  LZ(false, "application/x-lzip"),
  LZMA(false, "application/x-lzma"),
  LZO(false, "application/x-lzop"),
  XZ(false, "application/x-xz"),
  Z(false, "application/x-compress");

  private final String[] mimeTypes;
  private final boolean supported;

  CompressionAlgorithm(boolean supported, String... mimeTypes) {
    this.mimeTypes = mimeTypes;
    this.supported = supported;
  }

  boolean isSupported() {
    return supported;
  }

  Set<String> getMimeTypes() {
    return new HashSet<>(Arrays.asList(mimeTypes));
  }

  static Optional<CompressionAlgorithm> valueOfMimeType(String mimeType) {
    return Arrays.stream(values())
        .filter(a -> a.getMimeTypes().contains(mimeType))
        .findAny();
  }
}
