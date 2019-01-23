/*
 * Copyright © 2017 Google Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.google.enterprise.cloudsearch.database;

import static org.junit.Assert.assertEquals;

import java.util.Arrays;
import java.util.HashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

/** Tests for the UniqueKey class. */
public class UniqueKeyTest {

  @Rule
  public ExpectedException thrown = ExpectedException.none();

  /**
   * Use in the encoding tests below: name of the column, encoded name, column type.
   */
  private static class KeyField {

    private final String value;
    private final String encoded;

    private KeyField(Builder builder) {
      this.value = builder.value;
      this.encoded = builder.encoded;
    }

    static class Builder {

      private String value;
      private String encoded;

      Builder() {
      }

      Builder setValue(String value) {
        this.value = value;
        return this;
      }

      Builder setEncoded(String encoded) {
        this.encoded = encoded;
        return this;
      }

      KeyField build() {
        return new KeyField(this);
      }
    }
  }

  @Test
  public void testNullKey() {
    Map<String, Object> keyValuePairs = new HashMap<>();
    keyValuePairs.put("name", "abc");
    keyValuePairs.put("id", "123");
    thrown.expect(NullPointerException.class);
    UniqueKey.makeUniqueId(null, keyValuePairs);
  }

  @Test
  public void testEmptyKey() {
    thrown.expect(IllegalArgumentException.class);
    UniqueKey.makeUniqueId(new LinkedHashSet<>(Arrays.asList("name", "id")), new HashMap<>());
  }

  @Test
  public void testNullKeyValues() {
    thrown.expect(NullPointerException.class);
    UniqueKey.makeUniqueId(new LinkedHashSet<>(Arrays.asList("name", "id")), null);
  }

  @Test
  public void testNullKeyDecode() {
    thrown.expect(IllegalArgumentException.class);
    UniqueKey.decodeUniqueId(null);
  }

  // The format of the following tests are to pass values for the "name" and "id" columns that
  // will be encoded to a unique key and then decoded back for verification. Arguments are triplets
  // for value, encoded value (dealing with slashes and underscores), and type.
  @Test
  public void testEncodeDecodeStringInt() {
    runTest(
        new KeyField.Builder().setValue("abc").setEncoded("abc").build(),
        new KeyField.Builder().setValue("345").setEncoded("345").build());
  }

  @Test
  public void testEncodeDecodeIntString() {
    runTest(
        new KeyField.Builder().setValue("123").setEncoded("123").build(),
        new KeyField.Builder().setValue("xyz").setEncoded("xyz").build());
  }

  @Test
  public void testEncodeDecodeIntInt() {
    runTest(
        new KeyField.Builder().setValue("123").setEncoded("123").build(),
        new KeyField.Builder().setValue("456").setEncoded("456").build());
  }

  @Test
  public void testEncodeDecodeSlashes() {
    runTest(
        new KeyField.Builder().setValue("5/5").setEncoded("5_/5").build(),
        new KeyField.Builder().setValue("6/6").setEncoded("6_/6").build());
  }

  @Test
  public void testEncodeDecodeDoubleSlash() {
    runTest(
        new KeyField.Builder().setValue("5/5//").setEncoded("5_/5_/_/_/").build(),
        new KeyField.Builder().setValue("//6/6").setEncoded("_/_/6_/6").build());
  }

  @Test
  public void testEncodeDecodeUnderscore() {
    runTest(
        new KeyField.Builder().setValue("5_").setEncoded("5___/").build(),
        new KeyField.Builder().setValue("6_").setEncoded("6___/").build());
  }

  @Test
  public void testEncodeDecodeNullIntString() {
    runTest(
        new KeyField.Builder().setValue(null).setEncoded("null").build(),
        new KeyField.Builder().setValue("xyz").setEncoded("xyz").build());
  }

  @Test
  public void testEncodeDecodeIntNullString() {
    runTest(
        new KeyField.Builder().setValue("123").setEncoded("123").build(),
        new KeyField.Builder().setValue(null).setEncoded("null").build());
  }

  @Test
  public void testEncodeDecodeEmptyString() {
    runTest(
        new KeyField.Builder().setValue("").setEncoded("").build(),
        new KeyField.Builder().setValue("xyz").setEncoded("xyz").build());
  }

  @Test
  public void testEncodeDecodeEmptyString2() {
    runTest(
        new KeyField.Builder().setValue("abc").setEncoded("abc").build(),
        new KeyField.Builder().setValue("").setEncoded("").build());
  }

  /**
   * Run a common test of building a temporary database partially defined by the passed arguments.
   *
   * @param field1 the "name" field values
   * @param field2 the "id" field values
   */
  private void runTest(KeyField field1, KeyField field2) {
    Map<String, Object> keyValuePairs = new HashMap<>();
    keyValuePairs.put("name", field1.value);
    keyValuePairs.put("id", field2.value);
    String key = UniqueKey
        .makeUniqueId(new LinkedHashSet<>(Arrays.asList("name", "id")), keyValuePairs);
    assertEquals(field1.encoded + "/" + field2.encoded, key);

    // check the decode
    List<String> targetCols = Arrays.asList(field1.value, field2.value);
    List<String> columns = UniqueKey.decodeUniqueId(key);
    assertEquals(targetCols.toString(), columns.toString());
  }
}
