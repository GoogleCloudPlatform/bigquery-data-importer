package com.google.cloud.healthcare.decompress;

import static org.junit.Assert.assertEquals;
import static org.mockito.Mockito.mock;
import static org.mockito.MockitoAnnotations.initMocks;
import static org.powermock.api.mockito.PowerMockito.when;

import com.google.cloud.healthcare.io.InputReader;
import com.google.cloud.healthcare.io.OutputWriter;
import com.google.cloud.healthcare.io.OutputWriterFactory;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.channels.Channels;
import org.apache.commons.compress.compressors.lz4.FramedLZ4CompressorOutputStream;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mock;

/** Test for LZ4Handler. */
public class LZ4HandlerTest {

  private static final String TEXT = "LZ4 is lossless compression algorithm, providing "
      + "compression speed > 500 MB/s per core, scalable with multi-cores CPU. It features "
      + "an extremely fast decoder, with speed in multiple GB/s per core, typically reaching RAM "
      + "speed limits on multi-core systems.";

  @Mock
  private OutputWriterFactory factory;

  @Mock
  private InputReader reader;

  private byte[] lz4CompressedData;

  @Before
  public void setUp() throws Exception {
    initMocks(this);

    ByteArrayOutputStream os = new ByteArrayOutputStream();

    FramedLZ4CompressorOutputStream flz4os = new FramedLZ4CompressorOutputStream(os);
    flz4os.write(TEXT.getBytes());
    flz4os.flush();
    flz4os.close();

    lz4CompressedData = os.toByteArray();
  }

  @Test
  public void handleLZ4_contentMatch() throws IOException {
    when(reader.getReadChannel()).thenReturn(
        Channels.newChannel(new ByteArrayInputStream(lz4CompressedData)));
    when(reader.getName()).thenReturn("data");

    ByteArrayOutputStream os = new ByteArrayOutputStream();

    OutputWriter ow = mock(OutputWriter.class);
    when(ow.getWriteChannel()).thenReturn(Channels.newChannel(os));
    when(factory.getOutputWriter("data")).thenReturn(ow);

    new LZ4Handler(factory).handle(reader);

    assertEquals("Content should match.", TEXT, new String(os.toByteArray()));
  }

}
