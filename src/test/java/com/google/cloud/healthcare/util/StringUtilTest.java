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

package com.google.cloud.healthcare.util;

import static org.junit.Assert.assertEquals;

import org.junit.Test;

/** Test for {@link StringUtil}. */
public class StringUtilTest {

  @Test(expected = IllegalArgumentException.class)
  public void splitGcsUri_emptyUri_exception() {
    StringUtil.splitGcsUri("");
  }

  @Test(expected = IllegalArgumentException.class)
  public void splitGcsUri_invalidUri_exception() {
    StringUtil.splitGcsUri("http://example.com/my.txt");
  }

  @Test(expected = IllegalArgumentException.class)
  public void splitGcsUri_invalidUri_exception2() {
    StringUtil.splitGcsUri("gs://bucket"); // No path.
  }

  @Test
  public void splitGcsUri_expectedBucketAndPath() {
    String[] parts = StringUtil.splitGcsUri("gs://bucket/path/to/file");
    assertEquals("Bucket should match.", "bucket", parts[0]);
    assertEquals("Path should match.", "path/to/file", parts[1]);
  }

  @Test(expected = IllegalArgumentException.class)
  public void generateGcsUri_emptyBucket_exception() {
    StringUtil.generateGcsUri("", "test");
  }

  @Test(expected = IllegalArgumentException.class)
  public void generateGcsUri_emptyPath_exception() {
    StringUtil.generateGcsUri("bucket", "");
  }

  @Test
  public void generateGcsUri_validGcsUri() {
    assertEquals("Should return expected GCS URI.",
        "gs://bucket/path/to/file",
        StringUtil.generateGcsUri("bucket", "path/to/file"));
  }

  @Test
  public void getGcsDecompressUri_expected() {
    assertEquals("Return expected uri.",
        "gs://bucket/temp/decompress",
        StringUtil.getGcsDecompressUri("bucket"));
  }

  @Test(expected = IllegalArgumentException.class)
  public void getGcsDecompressUri_exception() {
    StringUtil.getGcsDecompressUri("");
  }

  @Test
  public void getBqSrcUri_expected() {
    assertEquals("Return expected uri.",
        "gs://bucket/temp/avro/file-*.avro",
        StringUtil.getBqSrcUri("gs://bucket/path/to/file.csv"));
  }
}