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

package com.google.cloud.healthcare.process.pipeline.csv;

import com.google.cloud.healthcare.config.CsvConfiguration;
import com.google.cloud.healthcare.io.ByteReader;
import com.google.common.base.Strings;
import com.google.common.collect.Lists;
import java.io.IOException;
import java.nio.channels.SeekableByteChannel;
import java.util.List;
import java.util.Map;
import javax.annotation.Nullable;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.values.PCollectionView;

/**
 * An Apache Beam {@link DoFn} which splits a large file into smaller chunks. This is done by first
 * choosing a few positions to start the splitting process, then looking for valid split points in
 * each block. The rule for determining a valid split point is:
 *
 *  1. If we know for sure based on the categorization of quotes (see {@link #detectQuotes(byte[])}
 *  and {@link #splitOffset(List)}), we record it;
 *  2. Otherwise, e.g. there is no quote on one line, we count the number of columns for each
 *  adjacent lines, if the number is over count of headers plus one, we record it.
 *
 * Note that in rare cases a valid split point cannot be found in the block, in which case we will
 * not split.
 */
public class GcsSplitCsvFn extends CsvSplitFn {

  public GcsSplitCsvFn(CsvConfiguration config,
      PCollectionView<Map<String, String[]>> headersView) {
    super(config, headersView);
  }

  @Nullable
  @Override
  protected Long calcSplitPoint(SeekableByteChannel ch, long start, long end) throws IOException {
    ch.position(start);

    ByteReader reader = new ByteReader(ch);
    // Discard the first line since the start byte likely lies in the middle of a complete line.
    reader.readLine();

    long currPos = ch.position();
    String prevLine = null;
    while (currPos < end) {
      byte[] line = reader.readLine();
      List<QuoteType> quotes = detectQuotes(line);
      // No quotes in this line.
      if (quotes.isEmpty()) {
        String currLine = new String(line);
        if (!Strings.isNullOrEmpty(prevLine) && validSplitPoint(prevLine, currLine)) {
          // The fields in this and previous line determines a valid split point.
          return currPos;
        } else {
          prevLine = currLine;
        }
      } else {
        // Set the prevLine mark as null since we have quotes.
        prevLine = null;
      }

      Offset offset = splitOffset(quotes);
      // If the first quote in the line is open.
      if (offset == Offset.START) {
        return currPos;
      } else if (offset == Offset.END) { // If the last quote in the line is closed.
        return ch.position();
      }
      currPos = ch.position();
    }

    return null;
  }

  /**
   * Checks whether the point between two lines is a valid split point. This is only done if neither
   * line has quote characters.
   */
  private boolean validSplitPoint(String prevLine, String currLine) {
    String delimiter = String.valueOf(config.getDelimiter());
    return prevLine.split(delimiter).length + currLine.split(delimiter).length
        > headers.length + 1;
  }

  /**
   * Detects the type of quotes in a line.
   *
   * The rules are:
   * - Any quote that is directly preceded or directly followed by a quote is potentially an escaped
   *   quote;
   * - Any quote that is directly preceded by a separator character, and not directly followed by a
   *   separator character is an open quote;
   * - Any quote that is not directly preceded by a separator character, but that is directly
   *   followed by a separator character is a close quote;
   * - All rest is unknown.
   *
   * Example:
   * 1999,Chevy,"Venture ""Extended Edition"""
   * 1997,Ford,E350,"Super, luxurious truck"
   *
   * The first quote on first line is an opening quote since it follows a delimiter (,) and not
   * followed by another separator. The second and third quotes are of type unknown, since they are
   * escaping and escaped quotes. Next three are the same (Note the last quote should have been a
   * closing quote, but we are unable to detect it).
   *
   * On the second line, the second quote is a closing quote because it doesn't follow another quote
   * or a separator, but is followed by a separator (new line or EOF).
   */
  private List<QuoteType> detectQuotes(byte[] bytes) {
    char quote = CsvConfiguration.getInstance().getQuote();

    List<QuoteType> quotes = Lists.newArrayList();
    for (int i = 0; i < bytes.length; i++) {
      if (bytes[i] != quote) {
        continue;
      }

      byte prev;
      if (i == 0) {
        // For first byte, treat the previous byte as separator.
        prev = '\n';
      } else {
        prev = bytes[i - 1];
      }

      byte next;
      if (i == bytes.length - 1) {
        // For last byte, treat the next byte as separator.
        next = '\n';
      } else {
        next = bytes[i + 1];
      }

      if (prev == quote || next == quote) {
        quotes.add(QuoteType.UNKNOWN);
      } else if (isSeparator(prev) && !isSeparator(next)) {
        quotes.add(QuoteType.OPEN);
      } else if (!isSeparator(prev) && isSeparator(next)) {
        quotes.add(QuoteType.CLOSED);
      } else {
        quotes.add(QuoteType.UNKNOWN);
      }
    }

    return quotes;
  }

  /**
   * Determines what offset relative to current line is a split point. This is based on the fact
   * that if the last quote on a line is a close quote, the end of this line should be valid split
   * point, for same reason, if the first quote is an open quote, the start of this line should be
   * a valid split point. In all rest cases, we are not sure if the start or end of line is valid.
   */
  private Offset splitOffset(List<QuoteType> quotes) {
    int size = quotes.size();

    if (size == 0) {
      return Offset.UNKNOWN;
    }

    if (quotes.get(0) == QuoteType.OPEN) {
      return Offset.START;
    }

    if (quotes.get(size - 1) == QuoteType.CLOSED) {
      return Offset.END;
    }

    return Offset.UNKNOWN;
  }

  // TODO(b/123358409): handle multiple bytes as delimiter.
  private boolean isSeparator(byte b) {
    return b == config.getDelimiter()
        || b == ByteReader.NEWLINE_FEED
        || b == ByteReader.CARRIAGE_RETURN;
  }

  private enum Offset {
    START,
    END,
    UNKNOWN;
  }
}
