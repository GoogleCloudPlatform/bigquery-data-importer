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
import com.google.cloud.healthcare.process.schema.GcpUtil;
import com.google.cloud.healthcare.util.StringUtil;
import com.google.cloud.storage.Blob;
import com.google.cloud.storage.BlobInfo;
import com.google.cloud.storage.Storage;
import java.nio.channels.WritableByteChannel;

/**
 * GcsOutputWriter is a wrapper of a GCS object that makes it easier to write.
 */
public class GcsOutputWriter implements OutputWriter {

  private final String bucket;
  private final String path;
  private final GoogleCredentials credentials;
  private Storage storage;

  public GcsOutputWriter(GoogleCredentials credentials, String bucket, String path) {
    this.bucket = bucket;
    this.path = path;
    this.credentials = credentials;
    initStorage();
  }

  public GcsOutputWriter(GoogleCredentials credentials, String gcsUri) {
    String[] parts = StringUtil.splitGcsUri(gcsUri);
    bucket = parts[0];
    path = parts[1];
    this.credentials = credentials;
    initStorage();
  }

  @Override
  public WritableByteChannel getWriteChannel() {
    Blob blob = storage.create(BlobInfo.newBuilder(bucket, path).build());
    return blob.writer();
  }

  @Override
  public String getName() {
    return StringUtil.generateGcsUri(bucket, path);
  }

  private void initStorage() {
    storage = GcpUtil.getGcsClient(credentials);
  }
}
