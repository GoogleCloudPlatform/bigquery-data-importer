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

package com.google.cloud.healthcare.config;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import org.junit.Test;

public class CsvConfigurationTest {

  @Test(expected = IllegalArgumentException.class)
  public void build_noQuote_throwException() {
    CsvConfiguration.getInstance().withQuote(null);
  }

  @Test
  public void build_fieldsMatch() {
    Character delimiter = ',';
    String[] headers = new String[] {"name"};
    Character quote = '"';
    String recordSeparator = "\r\n";
    boolean ignoreSurroundingSpaces = true;
    CsvConfiguration conf = CsvConfiguration.getInstance()
        .withDelimiter(delimiter)
        .withQuote(quote)
        .withRecordSeparator(recordSeparator)
        .withIgnoreSurroundingSpaces(ignoreSurroundingSpaces);

    assertEquals("Fields should match original values.", delimiter, conf.getDelimiter());
    assertEquals("Fields should match original values.", quote, conf.getQuote());
    assertEquals("Fields should match original values.", recordSeparator, conf.getRecordSeparator());
    assertTrue("Fields should match original values.", ignoreSurroundingSpaces);
  }
}