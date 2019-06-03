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

/**
 * Definition of possible types. Note that FLOAT is not listed here because we always try to parse
 * to DOUBLE for better precision.
 */
public enum FieldType {
  INT,
  LONG,
  DOUBLE,
  BOOLEAN,
  DATE,
  TIME,
  DATETIME,
  STRING,
  UNKNOWN;

  // TODO(b/123357900): Use bitwise for calculation.
  /** Calculate the common super type of two types. */
  static FieldType getCommonType(FieldType s, FieldType t) {
    if (s == t) {
      return s;
    }

    if (s == UNKNOWN) {
      return t;
    }
    if (t == UNKNOWN) {
      return s;
    }

    if ((s == DATETIME && t == DATE)
        || (s == DATE && t == DATETIME)
        || (s == TIME && t == DATETIME)
        || (s == DATETIME && t == TIME)) {
      return DATETIME;
    }

    if ((s == DOUBLE && (t == LONG || t == INT))
        || (t == DOUBLE && (s == LONG || s == INT))) {
      return DOUBLE;
    }
    if ((s == LONG && t == INT) || (t == LONG && s == INT)) {
      return LONG;
    }

    return STRING;
  }
}
