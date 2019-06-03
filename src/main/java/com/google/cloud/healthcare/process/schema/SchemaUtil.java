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
import java.time.temporal.Temporal;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import org.apache.avro.LogicalTypes;
import org.apache.avro.Schema;
import org.apache.avro.SchemaBuilder;
import org.apache.avro.SchemaBuilder.FieldAssembler;
import org.apache.avro.SchemaBuilder.FieldBuilder;

/**
 * Utility class for handling schemas.
 */
public class SchemaUtil {

  // TODO(b/121042145): Support customized namespace and name.
  private static final String SCHEMA_NAMESPACE = "com.google.cloud.healthcare";
  private static final String RECORD_NAME = "healthcare";

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

  public static Schema generateAvroSchema(String[] headers, List<FieldType> types) {
    // TODO(b/121042931): We should support ignoring bad rows to some number.
    if (headers.length != types.size()) {
      throw new IllegalArgumentException(
          String.format(
              "Encountered invalid input:\nheaders: %s\ntypes: %s",
              PrettyPrinter.print(Arrays.asList(headers)), PrettyPrinter.print(types)));
    }

    FieldAssembler<Schema> builder = SchemaBuilder
        .record(RECORD_NAME)
        .namespace(SCHEMA_NAMESPACE).fields();
    for (int i = 0; i < types.size(); i++) {
      FieldBuilder<Schema> fieldBuilder = builder.name(headers[i]);

      FieldType type = types.get(i);
      switch (type) {
        case BOOLEAN:
          builder = fieldBuilder.type().nullable().booleanType().noDefault();
          break;
        case INT:
          builder = fieldBuilder.type().nullable().intType().noDefault();
          break;
        case LONG:
          builder = fieldBuilder.type().nullable().longType().noDefault();
          break;
        case DOUBLE:
          builder = fieldBuilder.type().nullable().doubleType().noDefault();
          break;
        case DATE:
          builder = fieldBuilder.type(LogicalTypes.date()
              .addToSchema(Schema.create(Schema.Type.INT))).noDefault();
          break;
        case TIME:
          builder = fieldBuilder.type(LogicalTypes.timeMicros()
              .addToSchema(Schema.create(Schema.Type.LONG))).noDefault();
          break;
        case DATETIME:
          builder = fieldBuilder.type(LogicalTypes.timestampMillis()
              .addToSchema(Schema.create(Schema.Type.LONG))).noDefault();
          break;
        default:
          builder = fieldBuilder.type().nullable().stringType().noDefault();
          break;
      }
    }

    return builder.endRecord();
  }

  /**
   * Merges a series of AVRO schema. The rule is simple: we choose the most generic type, e.g.
   * we choose string between int and string.
   *
   * @param types a list of schema to merge
   * @return the merged schema
   */
  public static List<FieldType> merge(Iterable<List<FieldType>> types) {
    // The results must always present, so we skip isPresent() check.
    return Streams.stream(types).reduce(SchemaUtil::merge).get();
  }

  public static List<FieldType> merge(List<FieldType> s, List<FieldType> t) {
    Preconditions.checkArgument(s != null && t != null,
        "SchemaUtil input cannot be null.");
    Preconditions.checkArgument(s.size() == t.size(),
        "Number of fields in both schema should match.");
    return IntStream.range(0, s.size())
        .mapToObj(i -> FieldType.getCommonType(s.get(i), t.get(i))).collect(Collectors.toList());
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

  public static LocalDate convertToDate(String value) {
    try {
      return LocalDate.parse(value, DateTimeFormatter.ISO_DATE);
    } catch (DateTimeParseException e) {
      return null;
    }
  }

  public static LocalTime convertToTime(String value) {
    try {
      return LocalTime.parse(value, DateTimeFormatter.ISO_TIME);
    } catch (DateTimeParseException e) {
      return null;
    }
  }

  // TODO(b/121042936): Support customized format.
  public static Temporal convertToDateTime(String value) {
    LocalDateTime localDateTime = convertToLocalDateTime(value);
    if (localDateTime != null) {
      return localDateTime;
    }

    OffsetDateTime offsetDateTime = convertToOffsetDateTime(value);
    if (offsetDateTime != null) {
      return offsetDateTime;
    }

    return convertToZonedDateTime(value);
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
