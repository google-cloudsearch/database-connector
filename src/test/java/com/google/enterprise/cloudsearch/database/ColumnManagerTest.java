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

import static junit.framework.TestCase.assertEquals;
import static junit.framework.TestCase.assertFalse;
import static junit.framework.TestCase.assertTrue;
import static org.hamcrest.CoreMatchers.containsString;
import static org.mockito.Mockito.when;

import com.google.common.collect.ImmutableList;
import com.google.enterprise.cloudsearch.sdk.InvalidConfigurationException;
import com.google.enterprise.cloudsearch.sdk.config.Configuration.ResetConfigRule;
import com.google.enterprise.cloudsearch.sdk.config.Configuration.SetupConfigRule;
import com.google.enterprise.cloudsearch.sdk.indexing.ContentTemplate;
import com.google.enterprise.cloudsearch.sdk.indexing.DefaultAcl.DefaultAclMode;
import com.google.enterprise.cloudsearch.sdk.indexing.UrlBuilder;
import com.google.enterprise.cloudsearch.sdk.indexing.template.RepositoryContext;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;

/** Tests for the ColumnManager class. */
//Ignore unnecessary stubbings.
@RunWith(MockitoJUnitRunner.Silent.class)
public class ColumnManagerTest {

  private static final boolean WITH_TIMESTAMP = true;

  private static final String CONFIG_TITLE_DB_FORMAT =
      "contentTemplate." + ColumnManager.DB_CONTENT_TEMPLATE_NAME + ".title";
  private static final String CONFIG_HIGH_DB_FORMAT =
      "contentTemplate." + ColumnManager.DB_CONTENT_TEMPLATE_NAME + ".quality.high";
  private static final String CONFIG_MEDIUM_DB_FORMAT =
      "contentTemplate." + ColumnManager.DB_CONTENT_TEMPLATE_NAME + ".quality.medium";
  private static final String CONFIG_LOW_DB_FORMAT =
      "contentTemplate." + ColumnManager.DB_CONTENT_TEMPLATE_NAME + ".quality.low";

  @Rule public ExpectedException thrown = ExpectedException.none();
  @Rule public ResetConfigRule resetConfig = new ResetConfigRule();
  @Rule public SetupConfigRule setupConfig = SetupConfigRule.uninitialized();

  @Mock private RepositoryContext repositoryContextMock;

  @Before
  public void setup() {
    when(repositoryContextMock.getDefaultAclMode()).thenReturn(DefaultAclMode.FALLBACK);
  }

  private Properties buildDefaultConfig() {
    return buildDefaultConfig(false);
  }

  /**
   * Build a basic configuration appropriate for most tests - then each test can add/override.
   *
   * @param withTimestamp {@code true} to include incremental SQL with timestamp
   * @return default configuration parameters
   */
  private Properties buildDefaultConfig(boolean withTimestamp) {
    Properties config = new Properties();
    config.put(ColumnManager.DB_ALL_RECORDS_SQL, "select id, name, address from customer");
    config.put(ColumnManager.DB_ALL_COLUMNS, "id, name, address");
    config.put(ColumnManager.DB_CONTENT_COLUMNS, "*");
    config.put(ColumnManager.DB_UNIQUE_KEY_COLUMNS, "name, id");
    config.put(CONFIG_TITLE_DB_FORMAT, "name");
    config.put(UrlBuilder.CONFIG_COLUMNS, "id");
    config.put(UrlBuilder.CONFIG_FORMAT, "{0}");
    if (withTimestamp) {
      config.put(ColumnManager.DB_INC_UPDATE_SQL,
          "select id, name, address from customer where timestamp > ?");
    }
    return config;
  }

  @Test
  public void testColumnManager1() {
    String allRecordsSql = "select id, name, address, phone, url from customer";
    String allCols = "id, name, address, phone, url";
    String contentColumns = "*";
    String ukDecls = "name, id";
    String viewColumns = "url";
    String viewUrl = "/test/{0}";
    String title = " name ";
    Properties config = new Properties();
    config.put(ColumnManager.DB_ALL_RECORDS_SQL, allRecordsSql);
    config.put(ColumnManager.DB_ALL_COLUMNS, allCols);
    config.put(ColumnManager.DB_CONTENT_COLUMNS, contentColumns);
    config.put(ColumnManager.DB_UNIQUE_KEY_COLUMNS, ukDecls);
    config.put(UrlBuilder.CONFIG_COLUMNS, viewColumns);
    config.put(UrlBuilder.CONFIG_FORMAT, viewUrl);
    config.put(CONFIG_TITLE_DB_FORMAT, title);
    setupConfig.initConfig(config);
    ColumnManager colMgr = ColumnManager.fromConfiguration(repositoryContextMock);

    // build targets
    LinkedHashSet<String> targetAllSqlCols =
        new LinkedHashSet<>(Arrays.asList("id", "name", "address", "phone", "url"));
    LinkedHashSet<String> targetContentSqlCols =
        new LinkedHashSet<>(Arrays.asList("id", "name", "address", "phone", "url"));
    LinkedHashSet<String> targetUk =
        new LinkedHashSet<>(Arrays.asList("name", "id"));
    LinkedHashSet<String> targetViewCols = new LinkedHashSet<>(Collections.singletonList("url"));

    //verify
    assertEquals(targetAllSqlCols, colMgr.getAllSqlCols());
    assertEquals(targetContentSqlCols, colMgr.getContentSqlCols());
    assertEquals(targetUk, colMgr.getUniqueKey());
    assertEquals(allRecordsSql, colMgr.getAllRecordsSql());
    assertTrue(colMgr.getIncUpdateSql().isEmpty());
    assertEquals(title.trim(), colMgr.getContentTemplate().getTitle());
    assertTrue(colMgr.getContentTemplate().getHighContent().isEmpty());
    assertTrue(colMgr.getContentTemplate().getMediumContent().isEmpty());
    assertTrue(colMgr.getContentTemplate().getLowContent().isEmpty());
    assertFalse(colMgr.isUsingIncrementalUpdates());
  }

  @Test
  public void testColumnManager2() {
    String allRecordsSql =
      "select id, name, address, phone, timestamp from customer where id > 1000";
    String incUpdateSql =
      "select id, name, address, phone, timestamp from customer where id > 1000 and timestamp > ?";
    String allCols = "id, name , address  , phone,   timestamp";
    String contentColumns = "name, address, phone";
    String ukDecls = "name, id";
    String viewColumns = " id ,  name  ";
    String viewUrl = "{0}";
    String title = " name  ";
    String high = " name , address ";
    String med = " phone ";
    Properties config = new Properties();
    config.put(ColumnManager.DB_ALL_RECORDS_SQL, allRecordsSql);
    config.put(ColumnManager.DB_ALL_COLUMNS, allCols);
    config.put(ColumnManager.DB_CONTENT_COLUMNS, contentColumns);
    config.put(ColumnManager.DB_INC_UPDATE_SQL, incUpdateSql);
    config.put(ColumnManager.DB_UNIQUE_KEY_COLUMNS, ukDecls);
    config.put(UrlBuilder.CONFIG_COLUMNS, viewColumns);
    config.put(UrlBuilder.CONFIG_FORMAT, viewUrl);
    config.put(CONFIG_TITLE_DB_FORMAT, title);
    config.put(CONFIG_HIGH_DB_FORMAT, high);
    config.put(CONFIG_MEDIUM_DB_FORMAT, med);
    setupConfig.initConfig(config);
    ColumnManager colMgr = ColumnManager.fromConfiguration(repositoryContextMock);

    // build targets
    LinkedHashSet<String> targetUk = new LinkedHashSet<>(Arrays.asList("name", "id"));
    LinkedHashSet<String> targetAllSqlCols =
        new LinkedHashSet<>(Arrays.asList("id", "name", "address", "phone", "timestamp"));
    LinkedHashSet<String> targetContentSqlCols =
        new LinkedHashSet<>(Arrays.asList("name", "address", "phone"));
    LinkedHashSet<String> targetViewCols = new LinkedHashSet<>(Arrays.asList("id", "name"));
    String targetViewUrl = "{0}";
    String targetTitle = "name";
    List<String> targetHigh = ImmutableList.of("address");
    List<String> targetMed = ImmutableList.of("phone");

    //verify
    assertEquals(targetUk, colMgr.getUniqueKey());
    assertEquals(targetAllSqlCols, colMgr.getAllSqlCols());
    assertEquals(targetContentSqlCols, colMgr.getContentSqlCols());
    assertEquals(allRecordsSql, colMgr.getAllRecordsSql());
    assertEquals(incUpdateSql, colMgr.getIncUpdateSql());
    assertEquals(targetTitle, colMgr.getContentTemplate().getTitle());
    assertEquals(targetHigh.toString(), colMgr.getContentTemplate().getHighContent().toString());
    assertEquals(targetMed.toString(), colMgr.getContentTemplate().getMediumContent().toString());
    assertTrue(colMgr.getContentTemplate().getLowContent().isEmpty());
  }

  @Test
  public void testAclColumns() {
    Properties config = buildDefaultConfig();
    config.put(ColumnManager.DB_ALL_RECORDS_SQL,
        "select id, name, address, readers_users from customer");
    config.put(ColumnManager.DB_ALL_COLUMNS, "id, name , address, readers_users");
    config.put(ColumnManager.DB_CONTENT_COLUMNS, "name, address");
    setupConfig.initConfig(config);
    ColumnManager.fromConfiguration(repositoryContextMock);
  }

  @Test
  public void testNoAcls_DefaultAclModeNONE() {
    Properties config = buildDefaultConfig();
    setupConfig.initConfig(config);
    when(repositoryContextMock.getDefaultAclMode()).thenReturn(DefaultAclMode.NONE);
    thrown.expect(InvalidConfigurationException.class);
    thrown.expectMessage(containsString("defaultAcl.mode"));
    ColumnManager.fromConfiguration(repositoryContextMock);
  }

  @Test
  public void testAclsEnabled_DefaultAclModeNONE() {
    Properties config = buildDefaultConfig();
    config.put(ColumnManager.DB_ALL_RECORDS_SQL,
        "select id, name, address, readers_users from customer");
    config.put(ColumnManager.DB_ALL_COLUMNS, "id, name , address, readers_users");
    config.put(ColumnManager.DB_CONTENT_COLUMNS, "name, address");
    setupConfig.initConfig(config);
    when(repositoryContextMock.getDefaultAclMode()).thenReturn(DefaultAclMode.NONE);
    ColumnManager.fromConfiguration(repositoryContextMock);
  }

  @Test
  public void testNoAcls_DefaultAclModeFALLBACK() {
    Properties config = buildDefaultConfig();
    setupConfig.initConfig(config);
    ColumnManager.fromConfiguration(repositoryContextMock);
  }

  @Test // repeated unique key
  public void testRepeatedUKey() {
    Properties config = buildDefaultConfig();
    config.put(ColumnManager.DB_UNIQUE_KEY_COLUMNS, "name, id, id");
    setupConfig.initConfig(config);
    thrown.expect(InvalidConfigurationException.class);
    thrown.expectMessage(containsString("duplicate"));
    ColumnManager.fromConfiguration(repositoryContextMock);
  }

  @Test
  public void testMissingCols() {
    Properties config = buildDefaultConfig();
    config.put(ColumnManager.DB_ALL_COLUMNS, "id, name, address, phone");
    setupConfig.initConfig(config);
    thrown.expect(InvalidConfigurationException.class);
    thrown.expectMessage(containsString("phone"));
    ColumnManager.fromConfiguration(repositoryContextMock);
  }

  @Test // unique key not in select query
  public void testInvalidUKey() {
    Properties config = buildDefaultConfig();
    config.put(ColumnManager.DB_ALL_RECORDS_SQL, "select name, address from customer");
    config.put(ColumnManager.DB_ALL_COLUMNS, "name, address");
    config.put(ColumnManager.DB_UNIQUE_KEY_COLUMNS, "name, id");
    setupConfig.initConfig(config);
    thrown.expect(InvalidConfigurationException.class);
    thrown.expectMessage(containsString("id"));
    ColumnManager.fromConfiguration(repositoryContextMock);
  }

  @Test // content column not in select query
  public void testInvalidContent() {
    Properties config = buildDefaultConfig();
    config.put(ColumnManager.DB_CONTENT_COLUMNS, "name, phone");
    setupConfig.initConfig(config);
    thrown.expect(InvalidConfigurationException.class);
    thrown.expectMessage(containsString("phone"));
    ColumnManager.fromConfiguration(repositoryContextMock);
  }

  @Test // med field not in all columns
  public void testInvalidQualityField() {
    Properties config = buildDefaultConfig();
    config.put(CONFIG_MEDIUM_DB_FORMAT, "invalidfield");
    setupConfig.initConfig(config);
    thrown.expect(InvalidConfigurationException.class);
    thrown.expectMessage(containsString("invalidfield"));
    ColumnManager.fromConfiguration(repositoryContextMock);
  }

  @Test
  public void testEmptyAllSql() {
    Properties config = buildDefaultConfig();
    config.put(ColumnManager.DB_ALL_RECORDS_SQL, "   ");
    setupConfig.initConfig(config);
    thrown.expect(InvalidConfigurationException.class);
    thrown.expectMessage(ColumnManager.DB_ALL_RECORDS_SQL);
    ColumnManager.fromConfiguration(repositoryContextMock);
  }

  @Test
  public void testEmptySingleAllCols() {
    Properties config = buildDefaultConfig();
    config.put(ColumnManager.DB_ALL_COLUMNS, "id,  , name, address");
    setupConfig.initConfig(config);
    thrown.expect(InvalidConfigurationException.class);
    thrown.expectMessage(containsString("contain empty column"));
    ColumnManager.fromConfiguration(repositoryContextMock);
  }
  @Test
  public void testEmptyAllCols() {
    Properties config = buildDefaultConfig();
    config.put(ColumnManager.DB_ALL_COLUMNS, "");
    setupConfig.initConfig(config);
    thrown.expect(InvalidConfigurationException.class);
    thrown.expectMessage(ColumnManager.DB_ALL_COLUMNS);
    ColumnManager.fromConfiguration(repositoryContextMock);
  }

  @Test
  public void testDefaultEmptyContent() {
    Properties config = buildDefaultConfig();
    config.put(ColumnManager.DB_CONTENT_COLUMNS, "");
    setupConfig.initConfig(config);
    ColumnManager colMgr = ColumnManager.fromConfiguration(repositoryContextMock);
    LinkedHashSet<String> targetContentSqlCols =
        new LinkedHashSet<>(Arrays.asList("id", "name", "address"));
    assertEquals(targetContentSqlCols, colMgr.getContentSqlCols());
  }

  @Test
  public void testEmptyUKey() {
    Properties config = buildDefaultConfig();
    config.put(ColumnManager.DB_UNIQUE_KEY_COLUMNS, "    ");
    setupConfig.initConfig(config);
    thrown.expect(InvalidConfigurationException.class);
    thrown.expectMessage(ColumnManager.DB_UNIQUE_KEY_COLUMNS);
    ColumnManager.fromConfiguration(repositoryContextMock);
  }

  @Test
  public void testEmptyTitle() {
    Properties config = buildDefaultConfig();
    config.put(CONFIG_TITLE_DB_FORMAT, "   ");
    setupConfig.initConfig(config);
    thrown.expect(InvalidConfigurationException.class);
    thrown.expectMessage(CONFIG_TITLE_DB_FORMAT);
    ColumnManager.fromConfiguration(repositoryContextMock);
  }

  @Test
  public void testInvalidTitle() {
    Properties config = buildDefaultConfig();
    config.put(CONFIG_TITLE_DB_FORMAT, "name, address");
    setupConfig.initConfig(config);
    thrown.expect(InvalidConfigurationException.class);
    thrown.expectMessage(containsString("title is not defined"));
    ColumnManager.fromConfiguration(repositoryContextMock);
  }

  @Test
  public void testDuplicateColumns() {
    Properties config = buildDefaultConfig();
    config.put(ColumnManager.DB_ALL_RECORDS_SQL,
        "select id, name, address, phone from customer");
    config.put(ColumnManager.DB_ALL_COLUMNS, "id, name, address, phone");
    config.put(ColumnManager.DB_CONTENT_COLUMNS, "name, address, phone, address");
    setupConfig.initConfig(config);
    thrown.expect(InvalidConfigurationException.class);
    thrown.expectMessage(containsString("duplicate"));
    ColumnManager.fromConfiguration(repositoryContextMock);
  }

  @Test
  public void testMissingHighContent() {
    Properties config = buildDefaultConfig();
    config.put(CONFIG_HIGH_DB_FORMAT, "name, , address");
    setupConfig.initConfig(config);
    thrown.expect(IllegalArgumentException.class);
    thrown.expectMessage(containsString("content fields cannot be empty"));
    ColumnManager.fromConfiguration(repositoryContextMock);
  }

  @Test
  public void testMissingMedContent() {
    Properties config = buildDefaultConfig();
    config.put(CONFIG_MEDIUM_DB_FORMAT, ", name, address");
    setupConfig.initConfig(config);
    thrown.expect(IllegalArgumentException.class);
    thrown.expectMessage(containsString("content fields cannot be empty"));
    ColumnManager.fromConfiguration(repositoryContextMock);
  }

  @Test
  public void testMissingLowContent() {
    Properties config = buildDefaultConfig();
    config.put(CONFIG_LOW_DB_FORMAT, "name, address, ,");
    setupConfig.initConfig(config);
    thrown.expect(IllegalArgumentException.class);
    thrown.expectMessage(containsString("content fields cannot be empty"));
    ColumnManager.fromConfiguration(repositoryContextMock);
  }

  @Test
  public void testDefaultMissingViewUrl() {
    Properties config = buildDefaultConfig();
    config.put(UrlBuilder.CONFIG_FORMAT, "");
    setupConfig.initConfig(config);
    ColumnManager colMgr = ColumnManager.fromConfiguration(repositoryContextMock);
    Map<String, Object> allColumnValues = new HashMap<String, Object>();
    allColumnValues.put("id", "http://samplesite");
    assertEquals("http://samplesite", colMgr.getViewUrl(allColumnValues));
  }

  @Test
  public void testMissingViewUrlCols() {
    Properties config = buildDefaultConfig();
    config.put(UrlBuilder.CONFIG_COLUMNS, "");
    setupConfig.initConfig(config);
    thrown.expect(IllegalArgumentException.class);
    thrown.expectMessage(containsString("URL columns"));
    ColumnManager.fromConfiguration(repositoryContextMock);
  }

  @Test
  public void testAllRecordsSql_paginationInvalid() {
    Properties config = buildDefaultConfig();
    config.put(ColumnManager.DB_PAGINATION, "foo");
    setupConfig.initConfig(config);
    thrown.expect(InvalidConfigurationException.class);
    thrown.expectMessage("Pagination must be one of [none, offset]");
    ColumnManager.fromConfiguration(repositoryContextMock);
  }

  @Test
  public void testAllRecordsSql_paginationNone() {
    Properties config = buildDefaultConfig();
    config.put(ColumnManager.DB_PAGINATION, "none");
    setupConfig.initConfig(config);
    ColumnManager colMgr = ColumnManager.fromConfiguration(repositoryContextMock);
    assertEquals(Pagination.NONE, colMgr.getPagination());
  }

  @Test
  public void testAllRecordsSql_paginationOffset() {
    Properties config = buildDefaultConfig();
    config.put(ColumnManager.DB_ALL_RECORDS_SQL,
        config.get(ColumnManager.DB_ALL_RECORDS_SQL) + " LIMIT 10 OFFSET ?");
    config.put(ColumnManager.DB_PAGINATION, "offset");
    setupConfig.initConfig(config);
    ColumnManager colMgr = ColumnManager.fromConfiguration(repositoryContextMock);
    assertEquals(Pagination.OFFSET, colMgr.getPagination());
  }

  @Test
  public void testAllRecordsSql_paginationOffset_noPlaceHolder() {
    Properties config = buildDefaultConfig();
    config.put(ColumnManager.DB_PAGINATION, "offset");
    setupConfig.initConfig(config);
    thrown.expect(InvalidConfigurationException.class);
    thrown.expectMessage("requires a place holder");
    ColumnManager.fromConfiguration(repositoryContextMock);
  }

  @Test
  public void testAllRecordsSql_configurationError_paginationNone_withPlaceHolder() {
    Properties config = buildDefaultConfig();
    config.put(ColumnManager.DB_PAGINATION, "none");
    config.put(
        ColumnManager.DB_ALL_RECORDS_SQL,
        config.get(ColumnManager.DB_ALL_RECORDS_SQL) + " LIMIT 10 OFFSET ?");
    setupConfig.initConfig(config);
    thrown.expect(InvalidConfigurationException.class);
    thrown.expectMessage("query should not have a place holder");
    ColumnManager.fromConfiguration(repositoryContextMock);
  }

  @Test
  public void testTimestampWithPlaceHolder() {
    Properties config = buildDefaultConfig(WITH_TIMESTAMP);
    setupConfig.initConfig(config);
    ColumnManager colMgr = ColumnManager.fromConfiguration(repositoryContextMock);
    assertTrue(colMgr.isUsingIncrementalUpdates());
  }

  @Test
  public void testTimestampMissingIncUpdateSql() {
    Properties config = buildDefaultConfig();
    config.put(ColumnManager.DB_ALL_RECORDS_SQL,
        "select id, name, address, timestamp from customer");
    config.put(ColumnManager.DB_ALL_COLUMNS, "id, name, address, timestamp");
    config.put(CONFIG_TITLE_DB_FORMAT, "name");
    config.put(ColumnManager.DB_INC_UPDATE_SQL,
        "select id, name, address, timestamp from customer");
    setupConfig.initConfig(config);
    thrown.expect(InvalidConfigurationException.class);
    thrown.expectMessage(containsString("requires a place holder"));
    ColumnManager.fromConfiguration(repositoryContextMock);
  }

  @Test
  public void testTimezoneValidationGmt() {
    Properties config = buildDefaultConfig(WITH_TIMESTAMP);
    config.put(ColumnManager.DB_INC_UPDATE_TIMEZONE, "GMT");
    setupConfig.initConfig(config);
    ColumnManager colMgr = ColumnManager.fromConfiguration(repositoryContextMock);
    assertEquals("GMT", colMgr.getIncUpdateTimezone().getID());
  }

  @Test
  public void testTimezoneValidationGmt9() {
    Properties config = buildDefaultConfig(WITH_TIMESTAMP);
    config.put(ColumnManager.DB_INC_UPDATE_TIMEZONE, "GMT+9");
    setupConfig.initConfig(config);
    ColumnManager colMgr = ColumnManager.fromConfiguration(repositoryContextMock);
    assertEquals("GMT+09:00", colMgr.getIncUpdateTimezone().getID());
  }

  @Test
  public void testTimezoneValidationLa() {
    Properties config = buildDefaultConfig(WITH_TIMESTAMP);
    config.put(ColumnManager.DB_INC_UPDATE_TIMEZONE, "America/Los_Angeles");
    setupConfig.initConfig(config);
    ColumnManager colMgr = ColumnManager.fromConfiguration(repositoryContextMock);
    assertEquals("America/Los_Angeles", colMgr.getIncUpdateTimezone().getID());
  }

  @Test
  public void testTimezoneValidationLondon() {
    Properties config = buildDefaultConfig(WITH_TIMESTAMP);
    config.put(ColumnManager.DB_INC_UPDATE_TIMEZONE, "Europe/London");
    setupConfig.initConfig(config);
    ColumnManager colMgr = ColumnManager.fromConfiguration(repositoryContextMock);
    assertEquals("Europe/London", colMgr.getIncUpdateTimezone().getID());
  }

  @Test
  public void testTimezoneValidationTokyo() {
    Properties config = buildDefaultConfig(WITH_TIMESTAMP);
    config.put(ColumnManager.DB_INC_UPDATE_TIMEZONE, "Asia/Tokyo");
    setupConfig.initConfig(config);
    ColumnManager colMgr = ColumnManager.fromConfiguration(repositoryContextMock);
    assertEquals("Asia/Tokyo", colMgr.getIncUpdateTimezone().getID());
  }

  @Test
  public void testInvalidTimezoneTypo() {
    Properties config = buildDefaultConfig(WITH_TIMESTAMP);
    config.put(ColumnManager.DB_INC_UPDATE_TIMEZONE, "Asia/Tkyo");
    setupConfig.initConfig(config);
    thrown.expect(IllegalArgumentException.class);
    thrown.expectMessage(containsString("Invalid timezone"));
    ColumnManager.fromConfiguration(repositoryContextMock);
  }

  @Test
  public void testInvalidTimezoneNoPlusMinus() {
    Properties config = buildDefaultConfig(WITH_TIMESTAMP);
    config.put(ColumnManager.DB_INC_UPDATE_TIMEZONE, "GMT 9");
    setupConfig.initConfig(config);
    thrown.expect(IllegalArgumentException.class);
    thrown.expectMessage(containsString("Invalid timezone"));
    ColumnManager.fromConfiguration(repositoryContextMock);
  }

  @Test
  public void testInvalidTimezoneNumberOnly() {
    Properties config = buildDefaultConfig(WITH_TIMESTAMP);
    config.put(ColumnManager.DB_INC_UPDATE_TIMEZONE, "09:00");
    setupConfig.initConfig(config);
    thrown.expect(IllegalArgumentException.class);
    thrown.expectMessage(containsString("Invalid timezone"));
    ColumnManager.fromConfiguration(repositoryContextMock);
  }

  @Test
  public void testBlobColumn() {
    Properties config = buildDefaultConfig();
    config.put(ColumnManager.DB_ALL_RECORDS_SQL,
        "select id, name, address, blob_data from customer");
    config.put(ColumnManager.DB_ALL_COLUMNS, "id, name, address, blob_data");
    config.put(ColumnManager.DB_BLOB_COLUMN, "blob_data");
    config.put(CONFIG_TITLE_DB_FORMAT, "");
    setupConfig.initConfig(config);
    ColumnManager colMgr = ColumnManager.fromConfiguration(repositoryContextMock);
    assertEquals("blob_data", colMgr.getBlobColumn());
  }

  @Test
  public void testBlobColumnWithContentCols() {
    Properties config = buildDefaultConfig();
    config.put(ColumnManager.DB_ALL_RECORDS_SQL,
        "select id, name, address, blob_data from customer");
    config.put(ColumnManager.DB_ALL_COLUMNS, "id, name, address, blob_data");
    config.put(ColumnManager.DB_BLOB_COLUMN, "blob_data");
    config.put(ColumnManager.DB_CONTENT_COLUMNS, "name");
    config.put(CONFIG_TITLE_DB_FORMAT, "");
    setupConfig.initConfig(config);
    thrown.expect(InvalidConfigurationException.class);
    thrown.expectMessage(containsString("Content columns"));
    ColumnManager.fromConfiguration(repositoryContextMock);
  }

  @Test
  public void testInvalidBlobColumn() {
    Properties config = buildDefaultConfig();
    config.put(ColumnManager.DB_ALL_RECORDS_SQL,
        "select id, name, address, blob_data from customer");
    config.put(ColumnManager.DB_ALL_COLUMNS, "id, name, address");
    config.put(ColumnManager.DB_UNIQUE_KEY_COLUMNS, "name, id");
    config.put(UrlBuilder.CONFIG_COLUMNS, "id");
    config.put(ColumnManager.DB_BLOB_COLUMN, "blob_data");
    setupConfig.initConfig(config);
    thrown.expect(InvalidConfigurationException.class);
    thrown.expectMessage(containsString("Blob column"));
    ColumnManager.fromConfiguration(repositoryContextMock);
  }

  @Test
  public void testCreateContentTemplate() {
    Properties config = new Properties();
    config.put(ColumnManager.DB_ALL_RECORDS_SQL, "select id, name from table");
    config.put(ColumnManager.DB_ALL_COLUMNS, "id, name");
    config.put(ColumnManager.DB_CONTENT_COLUMNS, "id, name");
    config.put(ColumnManager.DB_UNIQUE_KEY_COLUMNS, "id");
    config.put(UrlBuilder.CONFIG_COLUMNS, "id");
    config.put(UrlBuilder.CONFIG_FORMAT, "{0}");
    config.put(CONFIG_TITLE_DB_FORMAT, "id");
    setupConfig.initConfig(config);
    ColumnManager colMgr = ColumnManager.fromConfiguration(repositoryContextMock);
    String target = "<!DOCTYPE html>\n<html lang='en'>\n<head>\n"
        + "<meta http-equiv='Content-Type' content='text/html; charset=utf-8'/>\n"
        + "<title>1234</title>\n</head>\n<body>\n"
        + "<div id='id'>\n  <p>id:</p>\n  <h1>1234</h1>\n</div>\n"
        + "<div id='name'>\n  <p>name:</p>\n  <p><small>John Doe</small></p>\n</div>\n"
        + "</body>\n</html>\n";
    ContentTemplate template = colMgr.getContentTemplate();
    Map<String, Object> data = new HashMap<>();
    data.put("id", "1234");
    data.put("name", "John Doe");
    assertEquals(target, template.apply(data));
  }

  @Test
  public void testCreateContentTemplate2() {
    Properties config = new Properties();
    config.put(ColumnManager.DB_ALL_RECORDS_SQL, "select id, name, address, phone from table");
    config.put(ColumnManager.DB_ALL_COLUMNS, "id, name, address, phone");
    config.put(ColumnManager.DB_CONTENT_COLUMNS, "*");
    config.put(ColumnManager.DB_UNIQUE_KEY_COLUMNS, "id");
    config.put(UrlBuilder.CONFIG_COLUMNS, "id");
    config.put(UrlBuilder.CONFIG_FORMAT, "{0}");
    config.put(CONFIG_TITLE_DB_FORMAT, "name");
    config.put(CONFIG_HIGH_DB_FORMAT, "id");
    config.put(CONFIG_MEDIUM_DB_FORMAT, "address");
    setupConfig.initConfig(config);
    ColumnManager colMgr = ColumnManager.fromConfiguration(repositoryContextMock);
    String target = "<!DOCTYPE html>\n<html lang='en'>\n<head>\n"
        + "<meta http-equiv='Content-Type' content='text/html; charset=utf-8'/>\n"
        + "<title>John Doe</title>\n</head>\n<body>\n"
        + "<div id='name'>\n  <p>name:</p>\n  <h1>John Doe</h1>\n</div>\n"
        + "<div id='id'>\n  <p>id:</p>\n  <h1>1234</h1>\n</div>\n"
        + "<div id='address'>\n  <p>address:</p>\n  <p>123 Main St</p>\n</div>\n"
        + "<div id='phone'>\n  <p>phone:</p>\n  <p><small>555-123-4567</small></p>\n</div>\n"
        + "</body>\n</html>\n";
    ContentTemplate template = colMgr.getContentTemplate();
    Map<String, Object> data = new HashMap<>();
    data.put("id", "1234");
    data.put("name", "John Doe");
    data.put("address", "123 Main St");
    data.put("phone", "555-123-4567");
    assertEquals(target, template.apply(data));
  }

  @Test
  public void testViewUrl() {
    Properties config = new Properties();
    String viewUrl = "http://anysite/{0}/category/{1}";
    config.put(ColumnManager.DB_ALL_RECORDS_SQL, "select id, name, address, phone from table");
    config.put(ColumnManager.DB_ALL_COLUMNS, "id, name, address, phone");
    config.put(ColumnManager.DB_CONTENT_COLUMNS, "*");
    config.put(ColumnManager.DB_UNIQUE_KEY_COLUMNS, "id, name");
    config.put(UrlBuilder.CONFIG_COLUMNS, "id, name");
    config.put(UrlBuilder.CONFIG_FORMAT, viewUrl);
    config.put(CONFIG_TITLE_DB_FORMAT, "name");
    setupConfig.initConfig(config);
    Map<String, Object> allColumnValues = new HashMap<String, Object>();
    allColumnValues.put("id", "1#2#3");
    allColumnValues.put("name", "product 1");
    ColumnManager colMgr = ColumnManager.fromConfiguration(repositoryContextMock);
    String golden = "http://anysite/1#2#3/category/product 1";
    assertEquals(golden, colMgr.getViewUrl(allColumnValues));
  }

  @Test
  public void testEscapeViewUrl() {
    Properties config = new Properties();
    String viewUrl = "http://anysite/{0}/category/{1}";
    config.put(ColumnManager.DB_ALL_RECORDS_SQL, "select id, name, address, phone from table");
    config.put(ColumnManager.DB_ALL_COLUMNS, "id, name, address, phone");
    config.put(ColumnManager.DB_CONTENT_COLUMNS, "*");
    config.put(ColumnManager.DB_UNIQUE_KEY_COLUMNS, "id, name");
    config.put(UrlBuilder.CONFIG_COLUMNS, "id, name");
    config.put(UrlBuilder.CONFIG_COLUMNS_TO_ESCAPE, "name");
    config.put(UrlBuilder.CONFIG_FORMAT, viewUrl);
    config.put(CONFIG_TITLE_DB_FORMAT, "name");
    setupConfig.initConfig(config);
    Map<String, Object> allColumnValues = new HashMap<String, Object>();
    allColumnValues.put("id", "1#2#3");
    allColumnValues.put("name", "product 1");
    ColumnManager colMgr = ColumnManager.fromConfiguration(repositoryContextMock);
    String golden = "http://anysite/1#2#3/category/product%201";
    assertEquals(golden, colMgr.getViewUrl(allColumnValues));
  }

  @Test
  public void testEscapeViewUrl_repeatCols() {
    Properties config = new Properties();
    String viewUrl = "http://anysite/{0}/category/{1}/id={0}";
    config.put(ColumnManager.DB_ALL_RECORDS_SQL, "select id, name, address, phone from table");
    config.put(ColumnManager.DB_ALL_COLUMNS, "id, name, address, phone");
    config.put(ColumnManager.DB_CONTENT_COLUMNS, "*");
    config.put(ColumnManager.DB_UNIQUE_KEY_COLUMNS, "name");
    config.put(UrlBuilder.CONFIG_COLUMNS, "id, name");
    config.put(UrlBuilder.CONFIG_COLUMNS_TO_ESCAPE, "name");
    config.put(UrlBuilder.CONFIG_FORMAT, viewUrl);
    config.put(CONFIG_TITLE_DB_FORMAT, "name");
    setupConfig.initConfig(config);
    Map<String, Object> allColumnValues = new HashMap<String, Object>();
    allColumnValues.put("id", "1#2#3");
    allColumnValues.put("name", "product 1");
    ColumnManager colMgr = ColumnManager.fromConfiguration(repositoryContextMock);
    String golden = "http://anysite/1#2#3/category/product%201/id=1#2#3";
    assertEquals(golden, colMgr.getViewUrl(allColumnValues));
  }

  @Test
  public void testEscapeViewUrl_sameValues() {
    Properties config = new Properties();
    String viewUrl = "http://anysite/{0}/category/{1}";
    config.put(ColumnManager.DB_ALL_RECORDS_SQL, "select id, name, address, phone from table");
    config.put(ColumnManager.DB_ALL_COLUMNS, "id, name, address, phone");
    config.put(ColumnManager.DB_CONTENT_COLUMNS, "*");
    config.put(ColumnManager.DB_UNIQUE_KEY_COLUMNS, "id, name");
    config.put(UrlBuilder.CONFIG_COLUMNS, "id, name");
    config.put(UrlBuilder.CONFIG_COLUMNS_TO_ESCAPE, "id, name");
    config.put(UrlBuilder.CONFIG_FORMAT, viewUrl);
    config.put(CONFIG_TITLE_DB_FORMAT, "name");
    setupConfig.initConfig(config);
    Map<String, Object> allColumnValues = new HashMap<String, Object>();
    allColumnValues.put("id", "product 1");
    allColumnValues.put("name", "product 1");
    ColumnManager colMgr = ColumnManager.fromConfiguration(repositoryContextMock);
    String golden = "http://anysite/product%201/category/product%201";
    assertEquals(golden, colMgr.getViewUrl(allColumnValues));
  }

  @Test
  public void testInvalidEscapeViewUrlCols() {
    Properties config = new Properties();
    config.put(ColumnManager.DB_ALL_RECORDS_SQL, "select id, name, address, phone from table");
    config.put(ColumnManager.DB_ALL_COLUMNS, "id, name, address, phone");
    config.put(ColumnManager.DB_UNIQUE_KEY_COLUMNS, "id, name");
    config.put(UrlBuilder.CONFIG_COLUMNS, "id, name");
    config.put(UrlBuilder.CONFIG_COLUMNS_TO_ESCAPE, "name, zipcode");
    config.put(CONFIG_TITLE_DB_FORMAT, "name");
    setupConfig.initConfig(config);
    thrown.expect(InvalidConfigurationException.class);
    thrown.expectMessage("zipcode");
    ColumnManager.fromConfiguration(repositoryContextMock);
  }

  @Test
  public void testInvalidViewUrlCols() {
    Properties config = new Properties();
    config.put(ColumnManager.DB_ALL_RECORDS_SQL, "select id, name, address, phone from table");
    config.put(ColumnManager.DB_ALL_COLUMNS, "id, name, address, phone");
    config.put(ColumnManager.DB_UNIQUE_KEY_COLUMNS, "id, name");
    config.put(UrlBuilder.CONFIG_COLUMNS, "id, name, zipcode");
    config.put(CONFIG_TITLE_DB_FORMAT, "name");
    setupConfig.initConfig(config);
    thrown.expect(InvalidConfigurationException.class);
    thrown.expectMessage("zipcode");
    ColumnManager.fromConfiguration(repositoryContextMock);
  }
}
