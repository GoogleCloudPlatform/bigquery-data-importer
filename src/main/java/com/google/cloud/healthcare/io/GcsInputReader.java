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
import com.google.cloud.ReadChannel;
import com.google.cloud.healthcare.process.schema.GcpUtil;
import com.google.cloud.healthcare.util.StringUtil;
import com.google.cloud.storage.Blob;
import com.google.cloud.storage.BlobId;
import com.google.cloud.storage.Storage;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.Serializable;
import java.nio.channels.ReadableByteChannel;

/**
 * GcsInputReader is a wrapper of a Google Cloud Storage (GCS) object that makes it easier to read
 * the content.
 */
public class GcsInputReader implements InputReader, Serializable {

  private String bucket;
  private String path;
  private GoogleCredentials credentials;
  private transient Blob blob;

  public GcsInputReader(GoogleCredentials credentials, String bucket, String path) {
    this.bucket = bucket;
    this.path = path;
    this.credentials = credentials;
    initBlob();
  }

  public GcsInputReader(GoogleCredentials credentials, String gcsUri) {
    String[] parts = StringUtil.splitGcsUri(gcsUri);
    this.bucket = parts[0];
    this.path = parts[1];
    this.credentials = credentials;
    initBlob();
  }

  @Override
  public String getContentType() {
    return blob.getContentType();
  }

  @Override
  public ReadableByteChannel getReadChannel() {
    ReadChannel readCh = blob.reader();
    readCh.setChunkSize(CHUNK_SIZE);
    return readCh;
  }

  @Override
  public String getName() {
    return StringUtil.generateGcsUri(bucket, path);
  }

  @Override
  public long getSize() {
    return blob.getSize();
  }

  public String getBucket() {
    return bucket;
  }

  public String getPath() {
    return path;
  }

  private void readObject(ObjectInputStream in) throws IOException, ClassNotFoundException {
    in.defaultReadObject();
    initBlob();
  }

  private void initBlob() {
    Storage storage = GcpUtil.getGcsClient(credentials);
    blob = storage.get(BlobId.of(bucket, path));
  }

  @Override
  public boolean equals(Object obj) {
    if (obj == null) {
      return false;
    }

    if (!(obj instanceof GcsInputReader)) {
      return false;
    }

    GcsInputReader anotherReader = (GcsInputReader) obj;
    return bucket.equals(anotherReader.getBucket()) && path.equals(anotherReader.getPath());
  }

  @Override
  public int hashCode() {
    return 31 * bucket.hashCode() + path.hashCode();
  }
}
