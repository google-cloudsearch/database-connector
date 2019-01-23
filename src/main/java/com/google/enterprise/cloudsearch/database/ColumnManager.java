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
import static com.google.common.base.Preconditions.checkState;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Strings;
import com.google.common.collect.Sets;
import com.google.enterprise.cloudsearch.sdk.InvalidConfigurationException;
import com.google.enterprise.cloudsearch.sdk.config.Configuration;
import com.google.enterprise.cloudsearch.sdk.config.Configuration.Parser;
import com.google.enterprise.cloudsearch.sdk.indexing.Acl;
import com.google.enterprise.cloudsearch.sdk.indexing.ContentTemplate;
import com.google.enterprise.cloudsearch.sdk.indexing.UrlBuilder;
import com.google.enterprise.cloudsearch.sdk.indexing.template.RepositoryContext;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TimeZone;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * Manager of configuration file column definitions.
 *
 * <p>Manages unique ID creation, SQL query statements, and specific column definitions for content
 * and metadata based on database columns.
 *
 * <p>Required configuration parameters:
 * <ul>
 *   <li>"{@value #DB_ALL_COLUMNS}" - All the column names and aliases used in main SQL query that
 *   will be used in any other column definition.
 *   <li>"{@value #DB_UNIQUE_KEY_COLUMNS}" - One or more column heading names separated by commas
 *   that provide a unique identifier for a database query result.
 *   <li>"{@value #DB_ALL_RECORDS_SQL}" - Define a SQL query that will be used to retrieve all
 *   relevant columns of the record in the database.
 *   <li>"{@value UrlBuilder#CONFIG_COLUMNS}" - Specifies the column(s) of a SQL query that will
 *   be used to create a viewable URL for search results.
 * </ul>
 *
 * <p>Optional configuration parameters:
 * <ul>
 *   <li>"{@value #DB_PAGINATION}" - The pagination style of the provided
 *   {@value #DB_ALL_RECORDS_SQL} query. The default is "{@value Pagination#NONE}". Use
 *   "{@value Pagination#OFFSET}" if the query has a SQL parameter for a row offset.
 *   <li>"{@value #DB_CONTENT_COLUMNS}" - Define the columns of a SQL query that will be used
 *   retrieve database record content and used for content hash that will be used to determine if
 *   the data has been modified.
 *   <li>"{@value #DB_INC_UPDATE_SQL}" - Incremental update query to retrieve recently changed
 *   documents usually by their timestamp.
 *   <li>"{@value #DB_INC_UPDATE_TIMEZONE}" - Specifies the incremental update timestamp's timezone,
 *   if timestamp is being used. This is only necessary if the database timestamp data is of a
 *   different timezone than the connector execution.
 *   <li>"{@value UrlBuilder#CONFIG_FORMAT}" - Specifies the format of the URL columns.
 *   <li>"{@value UrlBuilder#CONFIG_COLUMNS_TO_ESCAPE}" - Specifies the column(s) of a SQL query
 *   that will be URL escaped and used to create a viewable URL for search results.
 *   <li>"{@value #DB_BLOB_COLUMN}" - Specifies the content is contained in a single Blob column.
 * </ul>
 */
class ColumnManager {

  @VisibleForTesting
  static final String DB_CONTENT_TEMPLATE_NAME = "db"; // contentTemplate.db.*

  // required
  static final String DB_ALL_COLUMNS = "db.allColumns";
  static final String DB_UNIQUE_KEY_COLUMNS = "db.uniqueKeyColumns";
  static final String DB_ALL_RECORDS_SQL = "db.allRecordsSql";

  // optional
  static final String DB_PAGINATION = "db.allRecordsSql.pagination";
  static final String DB_CONTENT_COLUMNS = "db.contentColumns";
  static final String DB_INC_UPDATE_SQL = "db.incrementalUpdateSql";
  static final String DB_INC_UPDATE_TIMEZONE = "db.timestamp.timezone";
  static final String DB_BLOB_COLUMN = "db.blobColumn";

  static final String ACL_READERS_USERS = "readers_users";
  static final String ACL_READERS_GROUPS = "readers_groups";
  static final String ACL_DENIED_USERS = "denied_users";
  static final String ACL_DENIED_GROUPS = "denied_groups";

  static final String TIMESTAMP_COLUMN = "timestamp_column";

  private static final String DEFAULT_TIMEZONE_GMT = "GMT";

  private static final List<String> ACL_COLUMNS =
      Collections.unmodifiableList(new ArrayList<>(Arrays.asList(
          ACL_READERS_USERS, ACL_READERS_GROUPS, ACL_DENIED_USERS, ACL_DENIED_GROUPS)));

  private static final Logger log = Logger.getLogger(ColumnManager.class.getName());

  private final ContentTemplate contentTemplate;
  private final boolean aclsEnabled;
  private final String blobColumn;
  private final Pagination pagination;
  private final boolean usingIncrementalUpdates;
  private final TimeZone incrementalUpdateTimezone;
  // variables ending in "Sql" store SQL statements
  private final String allRecordsSql;
  private final String incrementalUpdateSql;
  // variables ending in "Cols" store column definitions
  private final LinkedHashSet<String> allSqlCols;
  private final LinkedHashSet<String> contentSqlCols;
  private final LinkedHashSet<String> uniqueKeyCols;
  private final UrlBuilder urlBuilder;

  /**
   * Store the various relevant column definitions originating from the configuration file for
   * retrieval during traversals.
   *
   * @param builder object containing all the columns strings
   */
  ColumnManager(Builder builder) {
    checkNotNull(builder);
    allRecordsSql = builder.mainSql;
    incrementalUpdateSql = builder.updateSql;

    // check that each column exists in SQL query(s)
    allSqlCols = new LinkedHashSet<>(builder.allSqlCols);
    checkConfiguration(!allSqlCols.contains(""), "All cannot contain empty column: " + allSqlCols);
    LinkedHashSet<String> missing = missingCols(allSqlCols, allRecordsSql);
    checkConfiguration(missing.isEmpty(), "Missing column names in main SQL query: " + missing);

    pagination = builder.pagination;
    checkConfiguration(!pagination.equals(Pagination.INVALID),
        "Pagination must be one of " + Pagination.VALUES);
    if (pagination.equals(Pagination.OFFSET)) {
      checkConfiguration(allRecordsSql.contains("?"),
          "Using pagination by offset requires a place holder in the query ('?').");
    }

    // TODO(normang): For now, inc updates requires a timestamp/datetime column.
    usingIncrementalUpdates = !incrementalUpdateSql.trim().isEmpty();
    if (usingIncrementalUpdates) {
      checkConfiguration(incrementalUpdateSql.contains("?"),
          "Using incremental updates requires a place holder in the query ('?').");
      missing = missingCols(allSqlCols, incrementalUpdateSql);
      checkConfiguration(missing.isEmpty(),
          "Missing column names in incremental update SQL query: " + missing);
    }
    incrementalUpdateTimezone = builder.timezone;

    uniqueKeyCols = makeColumnSet(builder.uniqueKeyCols, allSqlCols);
    log.log(Level.CONFIG, "UniqueKey: {0}", uniqueKeyCols);

    // if the blob column is specified, it must be in the column definitions
    blobColumn = builder.blobCol.trim();
    checkConfiguration(blobColumn.isEmpty() || allSqlCols.contains(blobColumn),
        "Blob column must be defined in all columns.");

    // TODO(normang): For now, Blob column and content columns are mutually exclusive.
    if (!blobColumn.isEmpty()) {
      // TODO(normang): Future: if Blob is defined content columns may be used for metadata content.
      contentSqlCols = new LinkedHashSet<>();
      contentTemplate = null;
      checkConfiguration(builder.contentCols.isEmpty()
              || builder.contentCols.equals(Collections.singletonList("*")),
          "Content columns not allowed with Blob column.");
    } else {
      // "*" is the default meaning use all columns from main SQL query
      checkConfiguration(!builder.contentCols.isEmpty(), "Content columns can't be empty.");
      LinkedHashSet<String> tempContentSqlCols;
      if (builder.contentCols.equals(Collections.singletonList("*"))) {
        tempContentSqlCols = new LinkedHashSet<>(allSqlCols);
        tempContentSqlCols.removeAll(ACL_COLUMNS);
      } else {
        tempContentSqlCols = makeColumnSet(builder.contentCols, allSqlCols);
      }
      contentSqlCols = tempContentSqlCols;
      log.log(Level.CONFIG, "Content columns: {0}", contentSqlCols);

      contentTemplate = createContentTemplate(contentSqlCols);
    }

    // by definition, individual repository-based Acls will be used iff any cols are specified
    List<String> aclColumns = new ArrayList<>(ACL_COLUMNS);
    aclColumns.retainAll(allSqlCols);
    aclsEnabled = !aclColumns.isEmpty();
    log.log(Level.CONFIG, "Repository based Acls: {0}", aclsEnabled ? "yes" : "no");

    RepositoryContext context = builder.context;
    if (!aclsEnabled && !context.getDefaultAclMode().isEnabled()) {
      throw new InvalidConfigurationException(
          "ACLs are not configured. Check db.allColumns and defaultAcl.mode.");
    }

    urlBuilder = UrlBuilder.fromConfiguration();
    Set<String> difference = urlBuilder.getMissingColumns(allSqlCols);
    if (!difference.isEmpty()) {
      throw new InvalidConfigurationException(
          "Invalid view URL column name(s): '" + difference + "'");
    }
  }

  /**
   * Checks for a configuration error.
   *
   * <p>This allows the connector to throw a {@link InvalidConfigurationException} instead of a
   * {@code checkArgument()} method's {@code IllegalArgumentException}. This prevents the SDK start
   * up code from performing retries when a quick exit is appropriate.
   *
   * @param condition the valid condition to test
   * @param errorMessage the thrown exception message when the {@code condition} is not met
   */
  private void checkConfiguration(boolean condition, String errorMessage) {
    if (!condition) {
      throw new InvalidConfigurationException(errorMessage);
    }
  }

  String getAllRecordsSql() {
    return allRecordsSql;
  }

  Pagination getPagination() {
    return pagination;
  }

  String getIncUpdateSql() {
    return incrementalUpdateSql;
  }

  boolean isUsingIncrementalUpdates() {
    return usingIncrementalUpdates;
  }

  TimeZone getIncUpdateTimezone() {
    return incrementalUpdateTimezone;
  }

  LinkedHashSet<String> getAllSqlCols() {
    return new LinkedHashSet<>(allSqlCols);
  }

  LinkedHashSet<String> getContentSqlCols() {
    return new LinkedHashSet<>(contentSqlCols);
  }

  LinkedHashSet<String> getUniqueKey() {
    return new LinkedHashSet<>(uniqueKeyCols);
  }

  @VisibleForTesting
  ContentTemplate getContentTemplate() {
    return contentTemplate;
  }

  String getBlobColumn() {
    return blobColumn;
  }

  String getViewUrl(Map<String, Object> allColumnValues) {
    return urlBuilder.buildUrl(allColumnValues);
  }

  static class Builder {

    private String mainSql;
    private String updateSql;
    private Pagination pagination;
    private TimeZone timezone;
    private List<String> allSqlCols;
    private String blobCol;
    private List<String> contentCols;
    private List<String> uniqueKeyCols;
    private RepositoryContext context;

    Builder() {
    }

    Builder setMainSql(String mainSql) {
      this.mainSql = mainSql;
      return this;
    }

    Builder setUpdateSql(String updateSql) {
      this.updateSql = updateSql;
      return this;
    }

    Builder setPagination(Pagination pagination) {
      this.pagination = pagination;
      return this;
    }

    Builder setTimezone(TimeZone timezone) {
      this.timezone = timezone;
      return this;
    }

    Builder setAllCols(List<String> allSqlCols) {
      this.allSqlCols = allSqlCols;
      return this;
    }

    Builder setBlobCol(String blobCol) {
      this.blobCol = blobCol;
      return this;
    }

    Builder setContentCols(List<String> contentCols) {
      this.contentCols = contentCols;
      return this;
    }

    Builder setUniqueKey(List<String> uniqueKeyCols) {
      this.uniqueKeyCols = uniqueKeyCols;
      return this;
    }

    Builder setRepositoryContext(RepositoryContext context) {
      this.context = context;
      return this;
    }

    ColumnManager build() {
      checkNotNullNotEmpty(mainSql, "All records");
      checkNotNull(updateSql, "Update query can't be null.");
      checkNotNull(timezone, "Update query timezone can't be null.");
      checkNotNullNotEmpty(allSqlCols, "All columns");
      checkNotNullNotEmpty(contentCols, "Content columns");
      checkNotNullNotEmpty(uniqueKeyCols, "Unique key");
      checkNotNull(blobCol, "Blob column can't be null.");
      checkNotNull(context, "RepositoryContext can't be null.");
      return new ColumnManager(this);
    }
  }

  private static void checkNotNullNotEmpty(String value, String msg) {
    checkNotNull(value, msg + " can't be null.");
    checkArgument(!value.trim().isEmpty(), msg + " can't be empty.");
  }

  private static void checkNotNullNotEmpty(List<String> value, String msg) {
    checkNotNull(value, msg + " can't be null.");
    checkArgument(!isEmptyList(value), msg + " can't be empty.");
  }

  /**
   * Check for valid list - containing at least one non-empty value.
   *
   * @param list list to verify
   */
  private static boolean isEmptyList(List<String> list) {
    return list.stream().filter(v -> !Strings.isNullOrEmpty(v)).count() == 0;
  }

  /**
   * Build the {@link ColumnManager} from the configuration parameters.
   *
   * <p>Mandatory parameters are defaulted to {@code null} and will cause an exception if not
   * assigned in the configuration file. Optional parameters are defaulted to an empty string
   * will cause default action and will parse without an exception being thrown.
   *
   * @param context RepositoryContext
   * @return the {@link ColumnManager} instance
   */
  static ColumnManager fromConfiguration(RepositoryContext context) {
    checkState(Configuration.isInitialized(), "configuration not initialized");
    return new Builder()
        // single strings
        .setMainSql(Configuration.getString(DB_ALL_RECORDS_SQL, null).get())
        .setPagination(
            Configuration.getValue(DB_PAGINATION, Pagination.NONE, Pagination::fromString)
                .get())
        .setUpdateSql(Configuration.getString(DB_INC_UPDATE_SQL, "").get())
        .setBlobCol(Configuration.getString(DB_BLOB_COLUMN, "").get())
        // custom value
        .setTimezone(
            Configuration.getValue(DB_INC_UPDATE_TIMEZONE, TimeZone.getDefault(), TIMEZONE_PARSER)
                .get())
        // multi-value strings
        .setAllCols(
            Configuration.getMultiValue(DB_ALL_COLUMNS, null,
                Configuration.STRING_PARSER).get())
        .setContentCols(
            Configuration.getMultiValue(DB_CONTENT_COLUMNS, Collections.singletonList("*"),
                Configuration.STRING_PARSER).get())
        .setUniqueKey(Configuration.getMultiValue(DB_UNIQUE_KEY_COLUMNS, null,
            Configuration.STRING_PARSER).get())
        .setRepositoryContext(context)
        .build();
  }

  /**
   * Build a {@link TimeZone} object from the timezone parameter.
   *
   * <p>The {@link TimeZone#getTimeZone(String)} method doesn't give any error indication when an
   * invalid timezone string is passed. The result of an invalid string is to return the default
   * {@value #DEFAULT_TIMEZONE_GMT} zone. Valid timezones are either an initial "GMT" or any string
   * that does not convert to the default {@value #DEFAULT_TIMEZONE_GMT} timezone.
   */
  private static final Parser<TimeZone> TIMEZONE_PARSER =
      timezone -> {
        TimeZone validTimezone = TimeZone.getTimeZone(timezone);
        checkArgument(timezone.equals(DEFAULT_TIMEZONE_GMT)
                || !validTimezone.getID().equals(DEFAULT_TIMEZONE_GMT),
            "Invalid timezone parameter: " + timezone);
        return validTimezone;
      };

  /**
   * Simple verification that the columns are all contained within the SQL query.
   *
   * @param allCols columns list
   * @param sql the SQL query
   * @return any missing columns (for error display)
   */
  private static LinkedHashSet<String> missingCols(LinkedHashSet<String> allCols, String sql) {
    LinkedHashSet<String> missing = new LinkedHashSet<>();
    for (String col : allCols) {
      if (!sql.contains(col)) {
        missing.add(col);
      }
    }
    return missing;
  }

  /**
   * Verify that columns are valid and not duplicated.
   *
   * @param cols column names to validate
   * @param validCols the known valid column names defined in the configuration file
   * @return the valid column names
   */
  private static LinkedHashSet<String> makeColumnSet(List<String> cols,
      LinkedHashSet<String> validCols) {
    LinkedHashSet<String> tmpContentCols = new LinkedHashSet<>();
    if (cols.isEmpty() || isEmptyList(cols)) {
      return tmpContentCols;
    }
    List<String> errors = new ArrayList<>();
    for (String name : cols) {
      name = name.trim();
      if (!validCols.contains(name)) {
        errors.add("invalid: " + name);
      } else if (tmpContentCols.contains(name)) {
        errors.add("duplicate: " + name);
      } else {
        tmpContentCols.add(name);
      }
    }
    if (!errors.isEmpty()) {
      throw new InvalidConfigurationException(
          "Column name(s) error: '" + errors + "'");
    }
    return tmpContentCols;
  }

  /**
   * Create and verify an HTML template that will be used for content indexing.
   *
   * <p>Verify that the title and every [h/m/l] quality column is contained in the content columns.
   *
   * @param contentColumns all defined content columns
   * @return a content template
   */
  private static ContentTemplate createContentTemplate(LinkedHashSet<String> contentColumns) {
    ContentTemplate template = ContentTemplate.fromConfiguration(DB_CONTENT_TEMPLATE_NAME);

    if (!contentColumns.contains(template.getTitle())) {
      throw new InvalidConfigurationException(
          "Content title is not defined in content columns: " + template.getTitle());
    }
    verifyColumns(contentColumns, template.getHighContent(), "high");
    verifyColumns(contentColumns, template.getMediumContent(), "medium");
    verifyColumns(contentColumns, template.getLowContent(), "low");
    log.log(Level.CONFIG, "Content columns: title: {0}, high: {1}, med: {2}, low: {3}",
        new Object[]{template.getTitle(), template.getHighContent(),
            template.getMediumContent(), template.getLowContent()});

    return template;
  }

  /**
   * Verify that each column of the subset is contained within the allCols.
   *
   * @param allCols all valid columns
   * @param subset columns to check
   * @param colQualityType quality display value describing this set of columns
   * @throws InvalidConfigurationException if the subset contains columns not in all columns
   */
  private static void verifyColumns(LinkedHashSet<String> allCols, Set<String> subset,
      String colQualityType) {
    if (!allCols.containsAll(subset)) {
      throw new InvalidConfigurationException(
          "Quality " + colQualityType + " columns not defined in content columns: "
              + Sets.difference(subset, allCols));
    }
  }

  /**
   * Build the HTML content from the key value set.
   *
   * @param keyValues map of content key/values
   * @return fully formed HTML content
   */
  String applyContentTemplate(Map<String, Object> keyValues) {
    return contentTemplate.apply(keyValues);
  }

  /**
   * Create an Acl for the current record, but only if specific Acls are enabled.
   *
   * @param allColumnValues all column values from the current record
   * @return fully formed specific record Acl or {@code null} if not enabled
   */
  Acl createAclIfEnabled(Map<String, Object> allColumnValues) {
    checkNotNull(allColumnValues, "Values cannot be null.");
    if (!aclsEnabled) {
      return null;
    }
    return Acl.createAcl(
        getOrReturnEmpty(allColumnValues, ACL_READERS_USERS),
        getOrReturnEmpty(allColumnValues, ACL_READERS_GROUPS),
        getOrReturnEmpty(allColumnValues, ACL_DENIED_USERS),
        getOrReturnEmpty(allColumnValues, ACL_DENIED_GROUPS));
  }

  /**
   * Retrieves the map value from the key or empty string if the key is not present.
   *
   * @param allColumnValues key/value map
   * @param key search key
   * @return key value or empty string if key is missing
   */
  private String getOrReturnEmpty(Map<String, Object> allColumnValues, String key) {
    if (!allColumnValues.containsKey(key)) {
      return ("");
    }
    return allColumnValues.get(key).toString();
  }
}
