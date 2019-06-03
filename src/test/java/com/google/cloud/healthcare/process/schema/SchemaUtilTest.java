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

import static org.junit.Assert.assertEquals;

import java.util.Arrays;
import java.util.List;
import org.apache.avro.Schema;
import org.junit.Test;

public class SchemaUtilTest {

  private static final String TEST_SCHEMA =
      "{\n"
          + "  \"type\" : \"record\",\n"
          + "  \"name\" : \"healthcare\",\n"
          + "  \"namespace\" : \"com.google.cloud.healthcare\",\n"
          + "  \"fields\" : [ {\n"
          + "    \"name\" : \"name1\",\n"
          + "    \"type\" : [ \"int\", \"null\" ]\n"
          + "  }, {\n"
          + "    \"name\" : \"name2\",\n"
          + "    \"type\" : [ \"long\", \"null\" ]\n"
          + "  }, {\n"
          + "    \"name\" : \"name3\",\n"
          + "    \"type\" : [ \"double\", \"null\" ]\n"
          + "  }, {\n"
          + "    \"name\" : \"name4\",\n"
          + "    \"type\" : [ \"boolean\", \"null\" ]\n"
          + "  }, {\n"
          + "    \"name\" : \"name5\",\n"
          + "    \"type\" : {\n"
          + "      \"type\" : \"int\",\n"
          + "      \"logicalType\" : \"date\"\n"
          + "    }\n"
          + "  }, {\n"
          + "    \"name\" : \"name6\",\n"
          + "    \"type\" : {\n"
          + "      \"type\" : \"long\",\n"
          + "      \"logicalType\" : \"time-micros\"\n"
          + "    }\n"
          + "  }, {\n"
          + "    \"name\" : \"name7\",\n"
          + "    \"type\" : {\n"
          + "      \"type\" : \"long\",\n"
          + "      \"logicalType\" : \"timestamp-millis\"\n"
          + "    }\n"
          + "  }, {\n"
          + "    \"name\" : \"name8\",\n"
          + "    \"type\" : [ \"string\", \"null\" ]\n"
          + "  }, {\n"
          + "    \"name\" : \"name9\",\n"
          + "    \"type\" : [ \"string\", \"null\" ]\n"
          + "  } ]\n"
          + "}";

  @Test
  public void infer_returnTypes() {
    List<FieldType> types = SchemaUtil.infer(Arrays.asList(
        "123", "12345678901234", "2.31",
        "true", "Y", "No", "T",
        "2018-11-18", "11:25:33", "2018-11-18T11:25:33",
        "abc", ""));
    assertEquals("Should return expected types.", Arrays.asList(
        FieldType.INT, FieldType.LONG, FieldType.DOUBLE,
        FieldType.BOOLEAN, FieldType.BOOLEAN, FieldType.BOOLEAN, FieldType.BOOLEAN,
        FieldType.DATE, FieldType.TIME, FieldType.DATETIME,
        FieldType.STRING, FieldType.UNKNOWN), types);
  }

  @Test
  public void generate_expectedSchema() {
    Schema schema = SchemaUtil.generateAvroSchema(new String[] {
        "name1", "name2", "name3", "name4", "name5", "name6", "name7", "name8", "name9"},
        Arrays.asList(
            FieldType.INT, FieldType.LONG, FieldType.DOUBLE,
            FieldType.BOOLEAN,
            FieldType.DATE, FieldType.TIME, FieldType.DATETIME,
            FieldType.STRING, FieldType.UNKNOWN));
    assertEquals("Generates expected schema.", TEST_SCHEMA, schema.toString(true));
  }

  @Test
  public void merge_resultExpectedTypes() {
    List<FieldType> s = Arrays.asList(FieldType.INT, FieldType.DATE,
        FieldType.STRING, FieldType.BOOLEAN, FieldType.LONG, FieldType.DATE);
    List<FieldType> t = Arrays.asList(FieldType.LONG, FieldType.DATETIME,
        FieldType.DOUBLE, FieldType.TIME, FieldType.DOUBLE, FieldType.UNKNOWN);

    assertEquals("Should return expected types.",
        Arrays.asList(FieldType.LONG, FieldType.DATETIME, FieldType.STRING, FieldType.STRING,
            FieldType.DOUBLE, FieldType.DATE),
        SchemaUtil.merge(Arrays.asList(s, t)));
  }
}