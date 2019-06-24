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
import com.univocity.parsers.csv.CsvFormat;
import com.univocity.parsers.csv.CsvParser;
import com.univocity.parsers.csv.CsvParserSettings;
import java.io.ByteArrayInputStream;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.values.KV;

/** A {@link DoFn} which parses a chunk of data into CSV. */
public class CsvParseDataFn extends DoFn<KV<String, byte[]>, KV<String, String[]>> {
  // The maximum number of rows in the CSV file, increasing this number will cause the job to
  // use more memory.
  private static final int MAX_COLUMNS = 8192;

  private final CsvConfiguration config;
  private CsvParser parser;

  public CsvParseDataFn(CsvConfiguration config) {
    this.config = config;
  }

  @Setup
  public void setUp() {
    CsvFormat format = new CsvFormat();
    format.setDelimiter(config.getDelimiter());
    format.setQuote(config.getQuote());
    format.setQuoteEscape(config.getQuote());
    format.setLineSeparator(config.getRecordSeparator());
    CsvParserSettings settings = new CsvParserSettings();
    settings.setFormat(format);
    settings.setMaxColumns(MAX_COLUMNS);
    settings.setMaxCharsPerColumn(-1);
    settings.setNormalizeLineEndingsWithinQuotes(false);

    parser = new CsvParser(settings);
  }

  @ProcessElement
  public void parse(ProcessContext ctx) {
    KV<String, byte[]> input = ctx.element();
    parser
        .parseAll(new ByteArrayInputStream(input.getValue()))
        .forEach(row -> ctx.output(KV.of(input.getKey(), row)));
  }
}
