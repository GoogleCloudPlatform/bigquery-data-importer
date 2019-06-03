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

import com.google.common.base.Preconditions;
import com.google.common.base.Strings;
import java.io.Serializable;
import java.util.regex.Pattern;
import java.util.regex.PatternSyntaxException;

/**
 * This singleton class stores all configurations at run-time.
 */
public class CsvConfiguration implements Serializable {

  private static CsvConfiguration INSTANCE = new CsvConfiguration();

  // Configurations for CSV-like inputs.
  // The character(s) used to separate fields, typically comma for standard CSV.
  private Character delimiter = ',';

  // In standard CSV files, quotes are added around fields with special characters such as newlines
  // and delimiters. This is required.
  private Character quote = '"';

  // Character(s) used to separate records.
  private String recordSeparator = "\n";

  // Regular expression for line delimiter. This takes higher precedence over delimiter if
  // specified.
  private Pattern delimiterRegex;

  // Regular expression for record separator. This takes higher precedence over recordSeparator if
  // specified.
  private Pattern recordSeparatorRegex;

  // Whether to ignore surrounding spaces.
  private boolean ignoreSurroundingSpaces = true;

  private CsvConfiguration() {
  }

  public static CsvConfiguration getInstance() {
    return INSTANCE;
  }

  public CsvConfiguration withDelimiter(Character delimiter) {
    this.delimiter = delimiter;
    return this;
  }

  public Character getDelimiter() {
    return delimiter;
  }

  public CsvConfiguration withQuote(Character quote) {
    Preconditions.checkArgument(quote != null, "Quote is required.");
    this.quote = quote;
    return this;
  }

  /**
   * Returns the character(s) used in input to enclose fields that have special characters.
   */
  public Character getQuote() {
    return quote;
  }

  public CsvConfiguration withRecordSeparator(String recordSeparator) {
    Preconditions.checkArgument(!Strings.isNullOrEmpty(recordSeparator),
        "Record separator cannot be null or empty.");
    this.recordSeparator = recordSeparator;
    return this;
  }

  public String getRecordSeparator() {
    return recordSeparator;
  }

  public CsvConfiguration withIgnoreSurroundingSpaces(boolean ignoreSurroundingSpaces) {
    this.ignoreSurroundingSpaces = ignoreSurroundingSpaces;
    return this;
  }

  public boolean isIgnoreSurroundingSpaces() {
    return ignoreSurroundingSpaces;
  }

  public Pattern getDelimiterRegex() {
    return delimiterRegex;
  }

  public CsvConfiguration withDelimiterRegex(String delimiterRegex) {
    try {
      this.delimiterRegex = Pattern.compile(delimiterRegex);
    } catch (PatternSyntaxException e) {
      throw new IllegalArgumentException(e);
    }
    return this;
  }

  public Pattern getRecordSeparatorRegex() {
    return recordSeparatorRegex;
  }

  public CsvConfiguration withRecordSeparatorRegex(String recordSeparatorRegex) {
    try {
      this.recordSeparatorRegex = Pattern.compile(recordSeparatorRegex);
    } catch (PatternSyntaxException e) {
      throw new IllegalArgumentException(e);
    }
    return this;
  }
}
