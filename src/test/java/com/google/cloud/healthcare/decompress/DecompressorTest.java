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

import static org.junit.Assert.assertEquals;
import static org.mockito.Matchers.anyString;
import static org.mockito.Mockito.when;
import static org.mockito.MockitoAnnotations.initMocks;

import com.google.cloud.healthcare.io.GcsOutputWriterFactory;
import com.google.cloud.healthcare.io.InputReader;
import com.google.cloud.healthcare.io.OutputWriter;
import com.google.common.collect.Lists;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.channels.Channels;
import java.util.List;
import java.util.zip.GZIPOutputStream;
import java.util.zip.ZipEntry;
import java.util.zip.ZipOutputStream;
import org.apache.commons.compress.compressors.lz4.FramedLZ4CompressorOutputStream;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mock;

/** Test for decompressor. */
public class DecompressorTest {

  private static final String TEXT = "Google Cloud Platform, offered by Google, is a suite of "
      + "cloud computing services that runs on the same infrastructure that Google uses internally "
      + "for its end-user products, such as Google Search and YouTube.";

  private static final String TXT_MIME_TYPE = "application/txt";
  private static final String RAR_MIME_TYPE =
      CompressionAlgorithm.RAR.getMimeTypes().iterator().next();
  private static final String GCS_NAME = "gs://bucket/path/to/file/compressed.file";
  private static final String DECOMPRESSED_NAME = "gs://bucket/path/to/file/decompressed.file";

  @Mock
  private GcsOutputWriterFactory factory;

  @Mock
  private InputReader reader;

  @Mock
  private OutputWriter writer;

  private ByteArrayOutputStream outputStream;

  private byte[] gzipCompressedData;
  private byte[] zipCompressedData;
  private byte[] lz4CompressedData;

  @Before
  public void setUp() throws Exception {
    initMocks(this);

    outputStream = new ByteArrayOutputStream();

    ByteArrayOutputStream os = new ByteArrayOutputStream();
    GZIPOutputStream gos = new GZIPOutputStream(os);
    gos.write(TEXT.getBytes());
    gos.flush();
    gos.close();

    gzipCompressedData = os.toByteArray();

    os = new ByteArrayOutputStream();
    ZipOutputStream zos = new ZipOutputStream(os);
    zos.putNextEntry(new ZipEntry("test.txt"));
    zos.write(TEXT.getBytes());
    zos.closeEntry();
    zos.flush();
    zos.close();

    zipCompressedData = os.toByteArray();

    os = new ByteArrayOutputStream();

    FramedLZ4CompressorOutputStream flz4os = new FramedLZ4CompressorOutputStream(os);
    flz4os.write(TEXT.getBytes());
    flz4os.flush();
    flz4os.close();

    lz4CompressedData = os.toByteArray();
  }

  @After
  public void tearDown() throws Exception {
    outputStream.close();
  }

  @Test
  public void decompressGZip_contentMatch() throws IOException {
    when(reader.getContentType()).thenReturn(
        CompressionAlgorithm.GZIP.getMimeTypes().iterator().next());
    when(reader.getReadChannel()).thenReturn(
        Channels.newChannel(new ByteArrayInputStream(gzipCompressedData)));
    when(reader.getName()).thenReturn(GCS_NAME);
    when(factory.getOutputWriter(anyString())).thenReturn(writer);
    when(writer.getWriteChannel()).thenReturn(Channels.newChannel(outputStream));
    when(writer.getName()).thenReturn(DECOMPRESSED_NAME);

    List<String> files = new Decompressor(factory).decompress(reader);
    assertEquals(
        "File paths should match.",
        Lists.newArrayList(DECOMPRESSED_NAME),
        files);
    assertEquals(
        "Decompressed content should match the original.",
        TEXT,
        new String(outputStream.toByteArray()));
  }

  @Test
  public void decompressZip_contentMatch() throws IOException {
    when(reader.getContentType()).thenReturn(
        CompressionAlgorithm.ZIP.getMimeTypes().iterator().next());
    when(reader.getReadChannel()).thenReturn(
        Channels.newChannel(new ByteArrayInputStream(zipCompressedData)));
    when(reader.getName()).thenReturn(GCS_NAME);
    when(factory.getOutputWriter(anyString())).thenReturn(writer);
    when(writer.getWriteChannel()).thenReturn(Channels.newChannel(outputStream));
    when(writer.getName()).thenReturn(DECOMPRESSED_NAME);

    List<String> files = new Decompressor(factory).decompress(reader);
    assertEquals(
        "File paths should match.",
        Lists.newArrayList(DECOMPRESSED_NAME),
        files);
    assertEquals(
        "Decompressed content should match the original.",
        TEXT,
        new String(outputStream.toByteArray()));
  }

  @Test
  public void decompressLZ4_contentMatch() throws IOException {
    when(reader.getContentType()).thenReturn(
        CompressionAlgorithm.LZ4.getMimeTypes().iterator().next());
    when(reader.getReadChannel()).thenReturn(
        Channels.newChannel(new ByteArrayInputStream(lz4CompressedData)));
    when(reader.getName()).thenReturn(GCS_NAME);
    when(factory.getOutputWriter(anyString())).thenReturn(writer);
    when(writer.getWriteChannel()).thenReturn(Channels.newChannel(outputStream));
    when(writer.getName()).thenReturn(DECOMPRESSED_NAME);

    List<String> files = new Decompressor(factory).decompress(reader);
    assertEquals(
        "File paths should match.",
        Lists.newArrayList(DECOMPRESSED_NAME),
        files);
    assertEquals(
        "Decompressed content should match the original.",
        TEXT,
        new String(outputStream.toByteArray()));
  }

  @Test
  public void notCompressed_notDecompressed() throws IOException {
    when(reader.getContentType()).thenReturn(TXT_MIME_TYPE);
    when(reader.getName()).thenReturn(GCS_NAME);
    assertEquals(
        "Decompress should return false.",
        Lists.newArrayList(GCS_NAME),
        new Decompressor(new GcsOutputWriterFactory(null, "bucket", "path")).decompress(reader));
  }

  @Test(expected = RuntimeException.class)
  public void notSupportCompression_notDecompressed() throws IOException {
    when(reader.getContentType()).thenReturn(RAR_MIME_TYPE);
    new Decompressor(new GcsOutputWriterFactory(null, "bucket", "path")).decompress(reader);
  }
}