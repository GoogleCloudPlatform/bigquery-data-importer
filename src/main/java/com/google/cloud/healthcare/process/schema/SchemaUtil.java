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

import com.google.api.services.bigquery.model.TableFieldSchema;
import com.google.api.services.bigquery.model.TableSchema;
import com.google.cloud.healthcare.util.PrettyPrinter;
import com.google.common.base.Preconditions;
import com.google.common.base.Strings;
import com.google.common.collect.Streams;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.OffsetDateTime;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeParseException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

/** Utility class for handling schemas. */
public class SchemaUtil {

  /**
   * Infer schema from a list of values. The typical input is a row in a CSV file.
   *
   * @param input the values to infer schema from
   * @return inferred schema for a record
   */
  public static List<FieldType> infer(List<String> input) {
    return input.stream().map(SchemaUtil::infer).collect(Collectors.toList());
  }

  private static FieldType infer(String value) {
    // Note that the order of conversion attempts matter here.
    if (Strings.isNullOrEmpty(value)) {
      return FieldType.UNKNOWN;
    }

    if (convertToBoolean(value)) {
      return FieldType.BOOLEAN;
    }

    if (convertToInteger(value) != null) {
      return FieldType.INT;
    }

    if (convertToLong(value) != null) {
      return FieldType.LONG;
    }

    if (convertToDouble(value) != null) {
      return FieldType.DOUBLE;
    }

    if (convertToDate(value) != null) {
      return FieldType.DATE;
    }

    if (convertToTime(value) != null) {
      return FieldType.TIME;
    }

    if (convertToDateTime(value) != null) {
      return FieldType.DATETIME;
    }

    return FieldType.STRING;
  }

  public static TableSchema generateBigQueryTableSchema(String[] headers, FieldType[] types) {
    // TODO(b/121042931): We should support ignoring bad rows to some number.
    if (headers.length != types.length) {
      throw new IllegalArgumentException(
          String.format(
              "Encountered invalid input:\nheaders: %s\ntypes: %s",
              PrettyPrinter.print(Arrays.asList(headers)),
              PrettyPrinter.print(Arrays.asList(types))));
    }
    List<TableFieldSchema> fields = new ArrayList<>();
    for (int i = 0; i < types.length; i++) {
      TableFieldSchema field = new TableFieldSchema().setName(headers[i]).setMode("NULLABLE");
      switch (types[i]) {
        case BOOLEAN:
          field.setType("BOOL");
          break;
        case INT:
        case LONG:
          field.setType("INT64");
          break;
        case DOUBLE:
          field.setType("FLOAT64");
          break;
        case DATE:
          field.setType("DATE");
          break;
        case TIME:
          field.setType("TIME");
          break;
        case DATETIME:
          field.setType("DATETIME");
          break;
        default:
          field.setType("STRING");
          break;
      }
      fields.add(field);
    }
    return new TableSchema().setFields(fields);
  }

  /**
   * Merges a series of AVRO schema. The rule is simple: we choose the most generic type, e.g. we
   * choose string between int and string.
   *
   * @param types a list of schema to merge
   * @return the merged schema
   */
  public static List<FieldType> merge(Iterable<List<FieldType>> types) {
    // The results must always present, so we skip isPresent() check.
    return Streams.stream(types).reduce(SchemaUtil::merge).get();
  }

  public static List<FieldType> merge(List<FieldType> s, List<FieldType> t) {
    if (s.isEmpty()) {
      return t;
    }
    if (t.isEmpty()) {
      return s;
    }
    Preconditions.checkArgument(
        s.size() == t.size(), "Number of fields in both schema should match.");
    return IntStream.range(0, s.size())
        .mapToObj(i -> FieldType.getCommonType(s.get(i), t.get(i)))
        .collect(Collectors.toList());
  }

  public static Integer convertToInteger(String value) {
    try {
      return Integer.parseInt(value);
    } catch (NumberFormatException e) {
      return null;
    }
  }

  public static Long convertToLong(String value) {
    try {
      return Long.parseLong(value);
    } catch (NumberFormatException e) {
      return null;
    }
  }

  public static Double convertToDouble(String value) {
    try {
      return Double.parseDouble(value);
    } catch (NumberFormatException e) {
      return null;
    }
  }

  /**
   * Boolean is different because {@link Boolean#parseBoolean(String)} always return true or false.
   */
  private static boolean convertToBoolean(String value) {
    return "True".equalsIgnoreCase(value)
        || "False".equalsIgnoreCase(value)
        || "Yes".equalsIgnoreCase(value)
        || "No".equalsIgnoreCase(value)
        || "Y".equalsIgnoreCase(value)
        || "N".equalsIgnoreCase(value)
        || "T".equalsIgnoreCase(value)
        || "F".equalsIgnoreCase(value);
  }

  public static boolean isTrue(String value) {
    return "True".equalsIgnoreCase(value)
        || "Yes".equalsIgnoreCase(value)
        || "Y".equalsIgnoreCase(value)
        || "T".equalsIgnoreCase(value);
  }

  public static String convertToDate(String value) {
    try {
      return LocalDate.parse(value, DateTimeFormatter.ISO_DATE)
          .format(DateTimeFormatter.ISO_LOCAL_DATE);
    } catch (DateTimeParseException e) {
      return null;
    }
  }

  public static String convertToTime(String value) {
    try {
      return LocalTime.parse(value, DateTimeFormatter.ISO_TIME)
          .format(DateTimeFormatter.ISO_LOCAL_TIME);
    } catch (DateTimeParseException e) {
      return null;
    }
  }

  // TODO(b/121042936): Support customized format.
  public static String convertToDateTime(String value) {
    LocalDateTime localDateTime = convertToLocalDateTime(value);
    if (localDateTime != null) {
      return localDateTime.format(DateTimeFormatter.ISO_LOCAL_DATE_TIME);
    }

    OffsetDateTime offsetDateTime = convertToOffsetDateTime(value);
    if (offsetDateTime != null) {
      return offsetDateTime.format(DateTimeFormatter.ISO_LOCAL_DATE_TIME);
    }

    ZonedDateTime zonedDateTime = convertToZonedDateTime(value);
    if (zonedDateTime != null) {
      return zonedDateTime.format(DateTimeFormatter.ISO_LOCAL_DATE_TIME);
    }

    return null;
  }

  private static LocalDateTime convertToLocalDateTime(String value) {
    try {
      return LocalDateTime.parse(value, DateTimeFormatter.ISO_LOCAL_DATE_TIME);
    } catch (DateTimeParseException e) {
      return null;
    }
  }

  private static OffsetDateTime convertToOffsetDateTime(String value) {
    try {
      return OffsetDateTime.parse(value, DateTimeFormatter.ISO_OFFSET_DATE_TIME);
    } catch (DateTimeParseException e) {
      return null;
    }
  }

  private static ZonedDateTime convertToZonedDateTime(String value) {
    try {
      return ZonedDateTime.parse(value, DateTimeFormatter.ISO_ZONED_DATE_TIME);
    } catch (DateTimeParseException e) {
      return null;
    }
  }
}
