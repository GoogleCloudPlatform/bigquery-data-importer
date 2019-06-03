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

package com.google.cloud.healthcare.process.schema;

import com.google.auth.oauth2.GoogleCredentials;
import com.google.cloud.bigquery.BigQuery;
import com.google.cloud.bigquery.BigQueryOptions;
import com.google.cloud.storage.Storage;
import com.google.cloud.storage.StorageOptions;
import javax.annotation.Nullable;

/**
 * Utility methods for GCP related operations.
 */
public class GcpUtil {

  /**
   * Instantiates a GCS client. If the credentials is null, then the default one is used.
   */
  public static Storage getGcsClient(@Nullable GoogleCredentials credentials) {
    if (credentials != null) {
      return StorageOptions.newBuilder().setCredentials(credentials).build().getService();
    } else {
      return StorageOptions.getDefaultInstance().getService();
    }
  }

  /**
   * Instantiates a GCS client. If the credentials is null, then the default one is used.
   */
  public static BigQuery getBqClient(@Nullable GoogleCredentials credentials) {
    if (credentials != null) {
      return BigQueryOptions.newBuilder().setCredentials(credentials).build().getService();
    } else {
      return BigQueryOptions.getDefaultInstance().getService();
    }
  }

}