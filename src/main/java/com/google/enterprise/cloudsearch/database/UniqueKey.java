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

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkNotNull;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Strings;
import java.util.Arrays;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * Utility class to create and decode distinct unique ID's for each database record.
 */
class UniqueKey {

  private static final String NULL_STRING = "" + null;
  private static final Logger log = Logger.getLogger(UniqueKey.class.getName());

  /**
   * Meant to be used as a utility and does not contain any state.
   */
  private UniqueKey() {
  }

  /**
   * Build a reproducible unique ID for a record.
   *
   * <p>The record's unique ID is created by concatenating each unique key column together with
   * a "/" separator between them. E.g. if the Unique Key columns are "name" and "id", then the
   * unique ID will be "name/id". See {@link #encodeSlashInData(Object, String)} for special cases
   * where column value contains '_' and/or '/' characters.</p>
   *
   * @param keyColumns unique key column definitions
   * @param keyColumnValues key/value pairs
   * @return the unique ID for this record or empty if errors
   */
  static String makeUniqueId(LinkedHashSet<String> keyColumns,
      Map<String, Object> keyColumnValues) {
    checkNotNull(keyColumns, "Error building Unique ID, unique key columns cannot be null.");
    checkArgument(keyColumns.size() > 0,
        "Error building Unique ID, unique key columns cannot be empty");
    checkNotNull(keyColumnValues,
        "Error building Unique ID, unique key column/values cannot be null.");
    checkArgument(keyColumnValues.size() > 0,
        "Error building Unique ID, unique key columns cannot be empty");
    StringBuilder sb = new StringBuilder();
    for (String ukColumn : keyColumns) {
      sb.append("/");
      sb.append(encodeSlashInData(keyColumnValues.get(ukColumn), ukColumn));
    }
    return sb.toString().substring(1); // skip leading slash
  }

  /**
   * Decode the encoded unique key into the list of unique key column values.
   *
   * @param key the encoded unique key
   * @return a list of decoded unique key column values
   */
  @VisibleForTesting
  static List<String> decodeUniqueId(String key) {
    checkArgument(!Strings.isNullOrEmpty(key), "Unique ID key cannot be empty.");
    List<String> columns = Arrays.asList(key.split("(?<!_)/", -1));
    columns.replaceAll(UniqueKey::decodeSlashInData);
    return columns;
  }

  /**
   * Encode away troublesome '_' and '/' characters in a unique ID string.
   *
   * <p>Note: copied directly from the GSA database adaptor code.</p>
   *
   * <p> Don't let data end with '_', because then a '_' would precede the separator '/'.
   * If column value ends with '_' then append a '/' and take it away when decoding.
   * Since, column value could also end with '/' that we wouldn't want taken away during decoding,
   * append a second '/' when column value ends with '/' too.</p>
   *
   * <p>For last character '_' case:
   * Suppose unique key values are "5_" and "6_". Without this code here the unique keys would be
   * encoded into DocId "5__/6__".  Then when parsing the unique ID, the slash would not be used as
   * a splitter because it is preceded by '_'.
   * For last character '/' case:
   * Suppose unique key values are "5/" and "6/". Without appending another '/', unique ID will be
   * "5_//6_/", which will be split and decoded as "5" and "6".</p>
   *
   * @param data the unique key column value
   * @param colName the column name associated with the data (for logging only)
   * @return an encoded string of the column value
   */
  private static String encodeSlashInData(Object data, String colName) {
    if ((data == null) || (data.equals(NULL_STRING))) {
      log.log(Level.WARNING, "Unique key column [{0}] returned a 'null' value from the database.",
          colName);
      return NULL_STRING; // allows for self-healing if unexpected data encountered
    }
    String value = data.toString();
    if (!value.contains("/") && !value.contains("_")) {
      return value;
    }
    char lastChar = value.charAt(value.length() - 1);

    if ('_' == lastChar || '/' == lastChar) {
      value += '/';
    }
    value = value.replace("_", "__");
    value = value.replace("/", "_/");
    return value;
  }

  /**
   * Decode a column value from the unique ID.
   *
   * <p>Note: copied directly from the GSA database adaptor code.</p>
   *
   * <p> If id value ends with '/' (encoded as "_/") we know that this last '/' was appended
   * because column value ended with either '_' or '/'. We take away this last added '/'
   * character.</p>
   *
   * @param id the unique column value split from the unique ID
   * @return the decoded column value
   */
  private static String decodeSlashInData(String id) {
    if (id.endsWith("_/")) {
      id = id.substring(0, id.length() - 2);
    }
    id = id.replace("_/", "/");
    id = id.replace("__", "_");
    return id;
  }
}
