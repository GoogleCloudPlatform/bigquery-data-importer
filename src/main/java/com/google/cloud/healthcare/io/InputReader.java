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

import java.nio.channels.ReadableByteChannel;
import javax.annotation.Nullable;

/**
 * InputReader abstracts the input and provide useful methods for reading the content.
 */
public interface InputReader {

  int CHUNK_SIZE = 4 * 1024 * 1024; // 4MB.

  /**
   * @return the content type of the input.
   */
  String getContentType();

  /**
   * @return an {@link ReadableByteChannel} to read the content of the input.
   */
  ReadableByteChannel getReadChannel();

  /**
   * @return the size of the input. -1 is returned if the concrete size is unknown.
   */
  long getSize();

  /**
   * @return the original name of the input, null if not available.
   */
  @Nullable String getName();
}
