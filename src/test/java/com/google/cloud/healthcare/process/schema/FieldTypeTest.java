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
import static org.junit.Assert.assertNull;

import org.junit.Test;

public class FieldTypeTest {

  @Test
  public void getCommonType_bothNull_returnNull() {
    assertNull("Should return null if both are null.", FieldType.getCommonType(null, null));
  }

  @Test
  public void getCommonType_same_returnOriginal() {
    assertEquals("Should return the original type if both are same.",
        FieldType.INT, FieldType.getCommonType(FieldType.INT, FieldType.INT));
    assertEquals("Should return the original type if both are same.",
        FieldType.LONG, FieldType.getCommonType(FieldType.LONG, FieldType.LONG));
    assertEquals("Should return the original type if both are same.",
        FieldType.DOUBLE, FieldType.getCommonType(FieldType.DOUBLE, FieldType.DOUBLE));
    assertEquals("Should return the original type if both are same.",
        FieldType.DATE, FieldType.getCommonType(FieldType.DATE, FieldType.DATE));
    assertEquals("Should return the original type if both are same.",
        FieldType.TIME, FieldType.getCommonType(FieldType.TIME, FieldType.TIME));
    assertEquals("Should return the original type if both are same.",
        FieldType.DATETIME, FieldType.getCommonType(FieldType.DATETIME, FieldType.DATETIME));
    assertEquals("Should return the original type if both are same.",
        FieldType.BOOLEAN, FieldType.getCommonType(FieldType.BOOLEAN, FieldType.BOOLEAN));
  }

  @Test
  public void getCommonType_unknownAndOther_returnOther() {
    assertEquals("Should return int if input are null and int.",
        FieldType.INT, FieldType.getCommonType(FieldType.UNKNOWN, FieldType.INT));
    assertEquals("Should return long if input are unknown and long.",
        FieldType.LONG, FieldType.getCommonType(FieldType.UNKNOWN, FieldType.LONG));
    assertEquals("Should return double if input are unknown and double.",
        FieldType.DOUBLE, FieldType.getCommonType(FieldType.UNKNOWN, FieldType.DOUBLE));
    assertEquals("Should return date if input are unknown and date.",
        FieldType.DATE, FieldType.getCommonType(FieldType.UNKNOWN, FieldType.DATE));
    assertEquals("Should return time if input are unknown and time.",
        FieldType.TIME, FieldType.getCommonType(FieldType.UNKNOWN, FieldType.TIME));
    assertEquals("Should return datetime if input are unknown and datetime.",
        FieldType.DATETIME, FieldType.getCommonType(FieldType.UNKNOWN, FieldType.DATETIME));
    assertEquals("Should return boolean if input are unknown and boolean.",
        FieldType.BOOLEAN, FieldType.getCommonType(FieldType.UNKNOWN, FieldType.BOOLEAN));
  }

  @Test
  public void getCommonType_stringAndOther_returnString() {
    assertEquals("Should return string if one of the input is string",
        FieldType.STRING, FieldType.getCommonType(FieldType.INT, FieldType.STRING));
    assertEquals("Should return string if one of the input is string",
        FieldType.STRING, FieldType.getCommonType(FieldType.LONG, FieldType.STRING));
    assertEquals("Should return string if one of the input is string",
        FieldType.STRING, FieldType.getCommonType(FieldType.DOUBLE, FieldType.STRING));
    assertEquals("Should return string if one of the input is string",
        FieldType.STRING, FieldType.getCommonType(FieldType.DATE, FieldType.STRING));
    assertEquals("Should return string if one of the input is string",
        FieldType.STRING, FieldType.getCommonType(FieldType.TIME, FieldType.STRING));
    assertEquals("Should return string if one of the input is string",
        FieldType.STRING, FieldType.getCommonType(FieldType.DATETIME, FieldType.STRING));
    assertEquals("Should return string if one of the input is string",
        FieldType.STRING, FieldType.getCommonType(FieldType.BOOLEAN, FieldType.STRING));
  }

  @Test
  public void getCommonType_dateOrTimeAndDateTime_returnDateTime() {
    assertEquals("Date and datetime should return datetime.",
        FieldType.DATETIME, FieldType.getCommonType(FieldType.DATETIME, FieldType.DATE));
    assertEquals("Time and datetime should return datetime.",
        FieldType.DATETIME, FieldType.getCommonType(FieldType.DATETIME, FieldType.TIME));
  }

  @Test
  public void getCommonType_dateAndTime_returnString() {
    assertEquals("Date and time should return string.",
        FieldType.STRING, FieldType.getCommonType(FieldType.TIME, FieldType.DATE));
  }

  @Test
  public void getCommonType_intAndLong_returnLong() {
    assertEquals("int and long should return long.",
        FieldType.LONG, FieldType.getCommonType(FieldType.LONG, FieldType.INT));
  }

  @Test
  public void getCommonType_intAndDouble_returnDouble() {
    assertEquals("int and double should return double.",
        FieldType.DOUBLE, FieldType.getCommonType(FieldType.DOUBLE, FieldType.INT));
  }

  @Test
  public void getCommonType_longAndDouble_returnDouble() {
    assertEquals("int and long should return double.",
        FieldType.DOUBLE, FieldType.getCommonType(FieldType.LONG, FieldType.DOUBLE));
  }

  @Test
  public void getCommonType_booleanAndOther_returnString() {
    assertEquals("boolean and other should return string.",
        FieldType.STRING, FieldType.getCommonType(FieldType.BOOLEAN, FieldType.INT));
    assertEquals("boolean and other should return string.",
        FieldType.STRING, FieldType.getCommonType(FieldType.BOOLEAN, FieldType.LONG));
    assertEquals("boolean and other should return string.",
        FieldType.STRING, FieldType.getCommonType(FieldType.BOOLEAN, FieldType.DOUBLE));
    assertEquals("boolean and other should return string.",
        FieldType.STRING, FieldType.getCommonType(FieldType.BOOLEAN, FieldType.DATE));
    assertEquals("boolean and other should return string.",
        FieldType.STRING, FieldType.getCommonType(FieldType.BOOLEAN, FieldType.TIME));
    assertEquals("boolean and other should return string.",
        FieldType.STRING, FieldType.getCommonType(FieldType.BOOLEAN, FieldType.DATETIME));
  }

  @Test
  public void getCommonType_numericAndDate_returnString() {
    assertEquals("boolean and other should return string.",
        FieldType.STRING, FieldType.getCommonType(FieldType.INT, FieldType.DATE));
    assertEquals("boolean and other should return string.",
        FieldType.STRING, FieldType.getCommonType(FieldType.LONG, FieldType.TIME));
    assertEquals("boolean and other should return string.",
        FieldType.STRING, FieldType.getCommonType(FieldType.DOUBLE, FieldType.DATETIME));
  }
}