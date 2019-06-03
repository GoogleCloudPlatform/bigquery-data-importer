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

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.mockito.MockitoAnnotations.initMocks;

import com.google.cloud.WriteChannel;
import com.google.cloud.storage.Blob;
import com.google.cloud.storage.BlobInfo;
import com.google.cloud.storage.Storage;
import com.google.cloud.storage.StorageOptions;
import java.nio.channels.Channels;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.powermock.api.mockito.PowerMockito;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;

@RunWith(PowerMockRunner.class)
@PrepareForTest({StorageOptions.class, Channels.class})
public class GcsOutputWriterTest {

  @Mock
  private Storage storage;

  @Mock
  private StorageOptions storageOptions;

  @Mock
  private Blob blob;

  @Mock
  private WriteChannel writer;

  @Before
  public void setUp() {
    initMocks(this);
    PowerMockito.mockStatic(StorageOptions.class, Channels.class);
    when(StorageOptions.getDefaultInstance()).thenReturn(storageOptions);
    when(storageOptions.getService()).thenReturn(storage);
    when(blob.writer()).thenReturn(writer);
    when(storage.create(any(BlobInfo.class))).thenReturn(blob);
  }

  @Test
  public void getWriteChannel_correctPath() {
    new GcsOutputWriter(null, "bucket", "path").getWriteChannel();
    verify(storage, times(1))
        .create(BlobInfo.newBuilder("bucket", "path").build());
  }
}