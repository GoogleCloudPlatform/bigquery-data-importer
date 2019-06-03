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

import com.google.auth.oauth2.GoogleCredentials;
import com.google.cloud.healthcare.util.StringUtil;
import com.google.common.base.Strings;

/**
 * Produces {@link GcsOutputWriter}s to store data on GCS.
 */
public class GcsOutputWriterFactory implements OutputWriterFactory {

  private final String bucket;
  private final String path;
  private final GoogleCredentials credentials;

  /**
   * Note here the path represents a logical folder, all {@link OutputWriter} created will point to
   * a file within the folder.
   */
  public GcsOutputWriterFactory(GoogleCredentials credentials, String bucket, String path) {
    this.bucket = bucket;
    this.path = path;
    this.credentials = credentials;
  }

  public GcsOutputWriterFactory(GoogleCredentials credentials, String gcsUri) {
    String[] parts = StringUtil.splitGcsUri(gcsUri);
    bucket = parts[0];
    path = parts[1];
    this.credentials = credentials;
  }

  @Override
  public OutputWriter getOutputWriter(String name) {
    String completePath = String.format("%s/%s", path, name).replaceAll("^/+", "");
    if (Strings.isNullOrEmpty(completePath)) {
      throw new IllegalArgumentException("Complete path cannot be empty.");
    }
    return new GcsOutputWriter(credentials, bucket, completePath);
  }
}
