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

import static java.nio.charset.StandardCharsets.UTF_8;
import static org.hamcrest.CoreMatchers.containsString;
import static org.hamcrest.CoreMatchers.not;
import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import com.google.api.client.http.ByteArrayContent;
import com.google.api.client.util.Charsets;
import com.google.api.services.cloudsearch.v1.model.Item;
import com.google.api.services.cloudsearch.v1.model.ItemMetadata;
import com.google.api.services.cloudsearch.v1.model.ItemStructuredData;
import com.google.api.services.cloudsearch.v1.model.ObjectDefinition;
import com.google.api.services.cloudsearch.v1.model.Schema;
import com.google.api.services.cloudsearch.v1.model.StructuredDataObject;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Iterables;
import com.google.common.io.CharStreams;
import com.google.enterprise.cloudsearch.sdk.CheckpointCloseableIterable;
import com.google.enterprise.cloudsearch.sdk.CheckpointCloseableIterableImpl;
import com.google.enterprise.cloudsearch.sdk.InvalidConfigurationException;
import com.google.enterprise.cloudsearch.sdk.RepositoryException;
import com.google.enterprise.cloudsearch.sdk.config.Configuration.ResetConfigRule;
import com.google.enterprise.cloudsearch.sdk.config.Configuration.SetupConfigRule;
import com.google.enterprise.cloudsearch.sdk.indexing.Acl;
import com.google.enterprise.cloudsearch.sdk.indexing.DefaultAcl;
import com.google.enterprise.cloudsearch.sdk.indexing.DefaultAcl.DefaultAclMode;
import com.google.enterprise.cloudsearch.sdk.indexing.IndexingItemBuilder;
import com.google.enterprise.cloudsearch.sdk.indexing.IndexingItemBuilder.ItemType;
import com.google.enterprise.cloudsearch.sdk.indexing.IndexingService;
import com.google.enterprise.cloudsearch.sdk.indexing.IndexingService.ContentFormat;
import com.google.enterprise.cloudsearch.sdk.indexing.IndexingService.RequestMode;
import com.google.enterprise.cloudsearch.sdk.indexing.StructuredData;
import com.google.enterprise.cloudsearch.sdk.indexing.StructuredData.ResetStructuredDataRule;
import com.google.enterprise.cloudsearch.sdk.indexing.UrlBuilder;
import com.google.enterprise.cloudsearch.sdk.indexing.template.ApiOperation;
import com.google.enterprise.cloudsearch.sdk.indexing.template.RepositoryContext;
import com.google.enterprise.cloudsearch.sdk.indexing.template.RepositoryDoc;
import java.io.IOException;
import java.io.InputStreamReader;
import java.sql.Blob;
import java.sql.Clob;
import java.sql.Connection;
import java.sql.NClob;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Timestamp;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.Properties;
import java.util.TimeZone;
import java.util.concurrent.atomic.AtomicInteger;
import junitparams.JUnitParamsRunner;
import junitparams.Parameters;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnit;
import org.mockito.junit.MockitoRule;

/** Tests for the DatabaseRepository class. */
@RunWith(JUnitParamsRunner.class)
public class DatabaseRepositoryTest {

  private static final AtomicInteger databaseCount = new AtomicInteger();

  private static String getUrl() {
    return "jdbc:h2:mem:" + databaseCount.getAndIncrement() + ";DATABASE_TO_UPPER=false";
  }

  private static final String CONFIG_TITLE_DB_FORMAT =
      "contentTemplate." + ColumnManager.DB_CONTENT_TEMPLATE_NAME + ".title";
  private static final String CONFIG_HIGH_DB_FORMAT =
      "contentTemplate." + ColumnManager.DB_CONTENT_TEMPLATE_NAME + ".quality.high";
  private static final String CONFIG_LOW_DB_FORMAT =
      "contentTemplate." + ColumnManager.DB_CONTENT_TEMPLATE_NAME + ".quality.low";

  private static final boolean USE_TIMESTAMP_COLUMN = true;

  private static final String LATEST_CHECKPOINT_TIMESTAMP = "2017-01-13 08:55:33.3";
  private static final String TIMEZONE_TEST_LOCALE = "America/New_York";
  private static final String TIMEZONE_TEST_LOCALE_2 = "Europe/Paris";

  private static final byte[] NULL_TRAVERSAL_CHECKPOINT = null;

  @Rule public MockitoRule rule = MockitoJUnit.rule();
  @Rule public ExpectedException thrown = ExpectedException.none();
  @Rule public ResetConfigRule resetConfig = new ResetConfigRule();
  @Rule public SetupConfigRule setupConfig = SetupConfigRule.uninitialized();
  @Rule public ResetStructuredDataRule resetStructuredData = new ResetStructuredDataRule();

  @Mock private DatabaseRepository.Helper helperMock;
  @Mock private DatabaseAccess databaseAccessMock;
  @Mock private RepositoryContext repositoryContextMock;

  @Before
  public void setup() {
    when(repositoryContextMock.getDefaultAclMode()).thenReturn(DefaultAclMode.FALLBACK);
  }

  /**
   * Set mandatory configuration parameters with generic values to pass a freeze/verification,
   * excluding driver class for testing.
   *
   * @param config the configuration object to populate
   */
  private void setAllMandatory(Properties config) {
    config.put(DatabaseConnectionFactory.DB_URL, "something");
    config.put(ColumnManager.DB_ALL_COLUMNS, "id, name, phone");
    config.put(ColumnManager.DB_UNIQUE_KEY_COLUMNS, "id, name");
    config.put(ColumnManager.DB_ALL_RECORDS_SQL,
        "select id, name, phone from testtable order by id");
    config.put(UrlBuilder.CONFIG_COLUMNS, "id");
    config.put(UrlBuilder.CONFIG_FORMAT, "{0}");
    config.put(CONFIG_TITLE_DB_FORMAT, "id");
    config.put(DefaultAcl.DEFAULT_ACL_MODE, DefaultAclMode.FALLBACK.toString());
  }

  @Test
  public void testInit() throws RepositoryException {
    DatabaseRepository dbRepository = new DatabaseRepository();
    Properties config = new Properties();
    setAllMandatory(config);
    setupConfig.initConfig(config);
    dbRepository.init(repositoryContextMock);
  }

  @Test
  public void testInit_DefaultAclModeNONE() throws RepositoryException {
    DatabaseRepository dbRepository = new DatabaseRepository();
    Properties config = new Properties();
    setAllMandatory(config);
    setupConfig.initConfig(config);
    when(repositoryContextMock.getDefaultAclMode()).thenReturn(DefaultAclMode.NONE);
    thrown.expect(InvalidConfigurationException.class);
    thrown.expectMessage(containsString("defaultAcl.mode"));
    dbRepository.init(repositoryContextMock);
  }

  @Test
  public void testInitUninitialized() throws RepositoryException {
    DatabaseRepository dbRepository = new DatabaseRepository();
    thrown.expect(IllegalStateException.class);
    thrown.expectMessage(containsString("not initialized"));
    dbRepository.init(repositoryContextMock);
  }

  @Test
  public void testInitDriver() throws RepositoryException {
    DatabaseRepository dbRepository = new DatabaseRepository();
    Properties config = new Properties();
    setAllMandatory(config);
    config.put(DatabaseConnectionFactory.DB_DRIVERCLASS,
        "com.google.enterprise.cloudsearch.database.DatabaseConnectionFactory");
    setupConfig.initConfig(config);
    dbRepository.init(repositoryContextMock);
    dbRepository.close();
  }

  @Test
  public void testInitFailedDriver() throws RepositoryException {
    DatabaseRepository dbRepository = new DatabaseRepository();
    Properties config = new Properties();
    setAllMandatory(config);
    config.put(DatabaseConnectionFactory.DB_DRIVERCLASS, "Error driver");
    setupConfig.initConfig(config);
    thrown.expect(InvalidConfigurationException.class);
    thrown.expectMessage("Error driver");
    dbRepository.init(repositoryContextMock);
  }

  @Test
  public void testDatabaseConnectionFactory() throws SQLException {
    Properties config = new Properties();
    setAllMandatory(config);
    config.put(DatabaseConnectionFactory.DB_URL, getUrl());
    setupConfig.initConfig(config);
    ConnectionFactory factory = new DatabaseConnectionFactory();
    Connection conn = factory.createConnection();
    assertFalse(conn.isClosed());
    factory.releaseConnection(conn);
    assertTrue(conn.isClosed());
  }

  @Test
  public void testDatabaseConnectionFactoryWithUser() throws SQLException {
    Properties config = new Properties();
    setAllMandatory(config);
    config.put(DatabaseConnectionFactory.DB_URL, getUrl());
    config.put(DatabaseConnectionFactory.DB_USER, "DummyUser");
    config.put(DatabaseConnectionFactory.DB_PASSWORD, "DummyPassword");
    setupConfig.initConfig(config);
    ConnectionFactory factory = new DatabaseConnectionFactory();
    Connection conn = factory.createConnection();
    assertFalse(conn.isClosed());
    factory.releaseConnection(conn);
    assertTrue(conn.isClosed());
  }

  @Test
  public void testDatabaseConnectionFactoryUrlError() {
    Properties config = new Properties();
    setAllMandatory(config);
    config.put(DatabaseConnectionFactory.DB_URL, "");
    setupConfig.initConfig(config);
    thrown.expect(InvalidConfigurationException.class);
    thrown.expectMessage(containsString("not initialized"));
    new DatabaseConnectionFactory();
  }

  @Test
  public void testInvalidUpdateMode() throws RepositoryException {
    Properties config = new Properties();
    config.put(DatabaseConnectionFactory.DB_URL, "someUrl");
    config.put(ColumnManager.DB_UNIQUE_KEY_COLUMNS, "id, name");
    config.put(ColumnManager.DB_ALL_COLUMNS, "id, name");
    config.put(ColumnManager.DB_CONTENT_COLUMNS, "*");
    config.put(ColumnManager.DB_ALL_RECORDS_SQL, "select id, name from testtable");
    config.put(DatabaseRepository.TRAVERSE_UPDATE_MODE, "INVALID_MODE");
    config.put(UrlBuilder.CONFIG_FORMAT, "{0}");
    config.put(UrlBuilder.CONFIG_COLUMNS, "id");
    config.put(CONFIG_TITLE_DB_FORMAT, "id");
    setupConfig.initConfig(config);
    DatabaseRepository dbRepository = new DatabaseRepository();
    thrown.expect(IllegalArgumentException.class);
    thrown.expectMessage(containsString("INVALID_MODE"));
    dbRepository.init(repositoryContextMock);
  }

  /** Used to capture content for (optional) comparisons within RepositoryDocs */
  private static class GoldenContent {
    private int goldenIndex = 0;
    private List<ByteArrayContent> content = new ArrayList<>();
    private List<String> html = new ArrayList<>();

    /** pre-set all the golden content in order */
    void setContent(String htmlContent) {
      html.add(htmlContent);
      content.add(new ByteArrayContent("text/html", htmlContent.getBytes(UTF_8)));
    }

    /** Get each content in order - for repository mock. */
    ByteArrayContent getContent(String htmlContent) {
      assertTrue(goldenIndex < content.size());
      assertEquals(html.get(goldenIndex), htmlContent);
      return content.get(goldenIndex++);
    }

    /** Get specific content for target testing. */
    ByteArrayContent getContent(int index) {
      assertTrue(index < content.size());
      return content.get(index);
    }
  }

  /** Used to store multiple content to compare {@link RepositoryDoc} objects. */
  private void multiMockContent(GoldenContent goldenContent) {
    when(helperMock.getContentFromHtml(anyString())).thenAnswer(
        invocation -> goldenContent.getContent((String) invocation.getArguments()[0]));
  }

  /** Simple replacement of helper mock call to build content from html. */
  private void mockContent() {
    when(helperMock.getContentFromHtml(anyString())).thenAnswer(
        invocation -> new ByteArrayContent("text/html",
            ((String) invocation.getArguments()[0]).getBytes(UTF_8)));
  }

  @Test
  public void testGetAllDocsItemsDefaultAcl() throws Exception {
    Properties config = new Properties();
    config.put(DatabaseConnectionFactory.DB_URL, getUrl());
    config.put(ColumnManager.DB_UNIQUE_KEY_COLUMNS, "id, name");
    config.put(ColumnManager.DB_ALL_COLUMNS, "id, name, phone");
    config.put(ColumnManager.DB_CONTENT_COLUMNS, "phone");
    config.put(ColumnManager.DB_ALL_RECORDS_SQL, "select id, name, phone from testtable");
    config.put(UrlBuilder.CONFIG_FORMAT, "{1}/{0}");
    config.put(UrlBuilder.CONFIG_COLUMNS, "name, id");
    config.put(CONFIG_TITLE_DB_FORMAT, "phone");
    config.put(DefaultAcl.DEFAULT_ACL_MODE, DefaultAclMode.FALLBACK.toString());
    setupConfig.initConfig(config);
    InMemoryDBConnectionFactory factory = new InMemoryDBConnectionFactory();
    when(helperMock.getConnectionFactory()).thenReturn(factory);
    mockContent();
    DatabaseRepository dbRepository = new DatabaseRepository(helperMock);
    dbRepository.init(repositoryContextMock);

    // create targets for verification
    List<String> targetIds = new ArrayList<>();
    targetIds.add("id1/Joe Smith");
    targetIds.add("id2/Mary Jones");
    targetIds.add("id3/Mike Brown");
    targetIds.add("id4/Sue Green");
    targetIds.add("id5/Betty Black");
    Connection conn = factory.createConnection();
    try {
      // build the db
      Statement stmt = conn.createStatement();
      stmt.execute("create table testtable "
          + "(id varchar(32) unique not null, name varchar(128), phone varchar(16))");
      stmt.execute("insert into testtable (id, name, phone) values ('id2', 'Mary Jones', '2134')");
      stmt.execute("insert into testtable (id, name, phone) values ('id1', 'Joe Smith', '1234')");
      stmt.execute("insert into testtable (id, name, phone) values ('id3', 'Mike Brown', '3124')");
      stmt.execute("insert into testtable (id, name, phone) values ('id5', 'Betty Black', '5123')");
      stmt.execute("insert into testtable (id, name, phone) values ('id4', 'Sue Green', '4123')");
      stmt.close();

      // query the db
      try (CheckpointCloseableIterable<ApiOperation> allDocs =
          dbRepository.getAllDocs(NULL_TRAVERSAL_CHECKPOINT)) {
        for (ApiOperation op : allDocs) {
          RepositoryDoc record = (RepositoryDoc) op;
          Item item = record.getItem();
          assertEquals(ContentFormat.HTML, record.getContentFormat());
          assertEquals(RequestMode.UNSPECIFIED, record.getRequestMode());
          assertNull(item.getAcl()); // default: Acl won't be set
          String recordId = item.getName();
          assertNotNull(recordId);
          // cheat: view URL is exactly the ID
          assertEquals(item.getName(), item.getMetadata().getSourceRepositoryUrl());
          assertTrue(targetIds.remove(recordId));
        }
        assertNull(allDocs.getCheckpoint());
        assertFalse(allDocs.hasMore());
        assertTrue(targetIds.size() == 0);
      }
    } finally {
      factory.releaseConnection(conn);
      dbRepository.close();
      factory.shutdown();
    }
  }

  @Test
  public void testGetAllDocsContentDefaultAcl() throws Exception {
    Properties config = new Properties();
    config.put(DatabaseConnectionFactory.DB_URL, getUrl());
    config.put(ColumnManager.DB_UNIQUE_KEY_COLUMNS, "id, name");
    config.put(ColumnManager.DB_ALL_COLUMNS, "id, name, sometimestamp, somedate, sometime");
    config.put(ColumnManager.DB_CONTENT_COLUMNS, "*");
    config.put(ColumnManager.DB_ALL_RECORDS_SQL,
        "select id, name, sometimestamp, somedate, sometime from testtable");
    config.put(DatabaseRepository.TRAVERSE_UPDATE_MODE, RequestMode.ASYNCHRONOUS.name());
    config.put(UrlBuilder.CONFIG_FORMAT, "{0}");
    config.put(UrlBuilder.CONFIG_COLUMNS, "id");
    config.put(CONFIG_TITLE_DB_FORMAT, "id");
    config.put(CONFIG_LOW_DB_FORMAT, "name, sometimestamp, somedate, sometime"); // to force order
    config.put(DefaultAcl.DEFAULT_ACL_MODE, DefaultAclMode.FALLBACK.toString());
    setupConfig.initConfig(config);
    InMemoryDBConnectionFactory factory = new InMemoryDBConnectionFactory();
    when(helperMock.getConnectionFactory()).thenReturn(factory);
    mockContent();
    DatabaseRepository dbRepository = new DatabaseRepository(helperMock);
    dbRepository.init(repositoryContextMock);

    // create target for verification
    String target = "<!DOCTYPE html>\n<html lang='en'>\n<head>\n"
        + "<meta http-equiv='Content-Type' content='text/html; charset=utf-8'/>\n"
        + "<title>id1</title>\n</head>\n<body>\n"
        + "<div id='id'>\n  <p>id:</p>\n  <h1>id1</h1>\n</div>\n"
        + "<div id='name'>\n  <p>name:</p>\n  <p><small>Joe Smith</small></p>\n</div>\n"
        + "<div id='sometimestamp'>\n  <p>sometimestamp:</p>\n"
        + "  <p><small>2017-01-12 14:01:23.4</small></p>\n</div>\n"
        + "<div id='somedate'>\n  <p>somedate:</p>\n  <p><small>2017-01-12</small></p>\n</div>\n"
        + "<div id='sometime'>\n  <p>sometime:</p>\n  <p><small>14:01:23</small></p>\n</div>\n"
        + "</body>\n</html>\n";
    Connection conn = factory.createConnection();
    try {
      // build the db
      Statement stmt = conn.createStatement();
      stmt.execute("create table testtable "
          + "(id varchar(32) unique not null, name varchar(128), sometimestamp timestamp, "
          + "somedate date, sometime time)");
      stmt.execute("insert into testtable (id, name, sometimestamp, somedate, sometime) values "
          + "('id1', 'Joe Smith', '2017-01-12 14:01:23.4', '2017-01-12', '14:01:23')");
      stmt.close();

      // query the db
      try (CheckpointCloseableIterable<ApiOperation> records =
          dbRepository.getAllDocs(NULL_TRAVERSAL_CHECKPOINT)) {
        RepositoryDoc record = (RepositoryDoc) records.iterator().next();
        assertEquals(ContentFormat.HTML, record.getContentFormat());
        assertEquals(RequestMode.ASYNCHRONOUS, record.getRequestMode());
        assertNull(record.getItem().getAcl()); // default: Acl won't be set
        String html = CharStreams.toString(
            new InputStreamReader(record.getContent().getInputStream(), Charsets.UTF_8));
        assertEquals(target, html);

        assertNull(records.getCheckpoint());
        assertFalse(records.hasMore());
      }
    } finally {
      factory.releaseConnection(conn);
      dbRepository.close();
      factory.shutdown();
    }
  }

  @Test
  public void testGetAllDocsDefaultAclNullTimestamp() throws Exception {
    String sdCreate = "sometimestamp";
    Properties config = new Properties();
    config.put(DatabaseConnectionFactory.DB_URL, getUrl());
    config.put(ColumnManager.DB_UNIQUE_KEY_COLUMNS, "id");
    config.put(ColumnManager.DB_ALL_COLUMNS, "id, name, sometimestamp");
    config.put(ColumnManager.DB_CONTENT_COLUMNS, "*");
    config.put(ColumnManager.DB_ALL_RECORDS_SQL,
        "select id, name, sometimestamp from testtable");
    config.put(DatabaseRepository.TRAVERSE_UPDATE_MODE, RequestMode.SYNCHRONOUS.name());
    config.put(UrlBuilder.CONFIG_FORMAT, "{0}");
    config.put(UrlBuilder.CONFIG_COLUMNS, "id");
    config.put(CONFIG_TITLE_DB_FORMAT, "name");
    config.put(CONFIG_HIGH_DB_FORMAT, "id");
    config.put(CONFIG_LOW_DB_FORMAT, "sometimestamp");
    config.put(DefaultAcl.DEFAULT_ACL_MODE, DefaultAclMode.FALLBACK.toString());
    config.put(IndexingItemBuilder.CREATE_TIME_FIELD, sdCreate);
    setupConfig.initConfig(config);
    InMemoryDBConnectionFactory factory = new InMemoryDBConnectionFactory();
    when(helperMock.getConnectionFactory()).thenReturn(factory);
    mockContent();
    DatabaseRepository dbRepository = new DatabaseRepository(helperMock);
    dbRepository.init(repositoryContextMock);

    // create target for verification
    String target = "<!DOCTYPE html>\n<html lang='en'>\n<head>\n"
        + "<meta http-equiv='Content-Type' content='text/html; charset=utf-8'/>\n"
        + "<title>Joe Smith</title>\n</head>\n<body>\n"
        + "<div id='name'>\n  <p>name:</p>\n  <h1>Joe Smith</h1>\n</div>\n"
        + "<div id='id'>\n  <p>id:</p>\n  <h1>id1</h1>\n</div>\n"
        + "<div id='sometimestamp'>\n  <p>sometimestamp:</p>\n"
        + "  <p><small></small></p>\n</div>\n"
        + "</body>\n</html>\n";
    Connection conn = factory.createConnection();
    try {
      // build the db
      Statement stmt = conn.createStatement();
      stmt.execute("create table testtable "
          + "(id varchar(32) unique not null, name varchar(128), sometimestamp timestamp, "
          + "somedate date, sometime time)");
      stmt.execute("insert into testtable (id, name) values "
          + "('id1', 'Joe Smith')");
      stmt.close();

      // query the db
      try (CheckpointCloseableIterable<ApiOperation> records =
          dbRepository.getAllDocs(NULL_TRAVERSAL_CHECKPOINT)) {
        RepositoryDoc record = (RepositoryDoc) records.iterator().next();
        String html = CharStreams.toString(
            new InputStreamReader(record.getContent().getInputStream(), Charsets.UTF_8));
        assertEquals(target, html);
      }
    } finally {
      factory.releaseConnection(conn);
      dbRepository.close();
      factory.shutdown();
    }
  }

  /**
   * Convert a simple display date/time into a fully formatted date/time with UTC offset for
   * comparisons.
   *
   * <p>Keeping this date/time in the local timezone gets around Junit querks that can cause
   * timezone tests to run differently when run singly versus in parallel. This function is required
   * to match date/time format conversion in the SDK for structured data.
   *
   * @param time initial time in simple format ("yyyy-MM-dd HH:mm:ss.S")
   * @return time converted to ("yyyy-MM-dd'T'HH:mm:ss.SSSX:00")
   */
  private String getZonedDateTime(String time) {
    DateTimeFormatter formatter1 = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss.S");
    DateTimeFormatter formatter2 = DateTimeFormatter.ofPattern("yyyy-MM-dd'T'HH:mm:ss.SSSXXX");
    LocalDateTime localDateTime = LocalDateTime.parse(time, formatter1);
    ZonedDateTime zonedDateTime = ZonedDateTime.of(localDateTime, TimeZone.getDefault().toZoneId());
    return zonedDateTime.format(formatter2);
  }

  @Test
  public void testGetAllDocsContentDefaultAclWithStructuredData() throws Exception {
    String sdTitle = "id";
    String sdMod = "modtimestamp";
    String sdCreate = "createdate";
    String sdLanguage = "language";
    String sdObjType = "objtype";
    Properties config = new Properties();
    config.put(DatabaseConnectionFactory.DB_URL, getUrl());
    config.put(ColumnManager.DB_UNIQUE_KEY_COLUMNS, "id");
    config.put(ColumnManager.DB_ALL_COLUMNS,
        "id, name, modtimestamp, createdate, language, objtype");
    config.put(ColumnManager.DB_CONTENT_COLUMNS, "*");
    config.put(ColumnManager.DB_ALL_RECORDS_SQL,
        "select id, name, modtimestamp, createdate, language, objtype from testtable");
    config.put(DatabaseRepository.TRAVERSE_UPDATE_MODE, RequestMode.ASYNCHRONOUS.name());
    config.put(UrlBuilder.CONFIG_FORMAT, "{0}");
    config.put(UrlBuilder.CONFIG_COLUMNS, "id");
    config.put(CONFIG_TITLE_DB_FORMAT, "id");
    config.put(CONFIG_LOW_DB_FORMAT, "name, modtimestamp, createdate, language, objtype");
    config.put(DefaultAcl.DEFAULT_ACL_MODE, DefaultAclMode.FALLBACK.toString());
    config.put(IndexingItemBuilder.TITLE_FIELD, sdTitle);
    config.put(IndexingItemBuilder.UPDATE_TIME_FIELD, sdMod);
    config.put(IndexingItemBuilder.CREATE_TIME_FIELD, sdCreate);
    config.put(IndexingItemBuilder.CONTENT_LANGUAGE_FIELD, sdLanguage);
    config.put(IndexingItemBuilder.OBJECT_TYPE, sdObjType);
    setupConfig.initConfig(config);
    InMemoryDBConnectionFactory factory = new InMemoryDBConnectionFactory();
    when(helperMock.getConnectionFactory()).thenReturn(factory);
    mockContent();
    DatabaseRepository dbRepository = new DatabaseRepository(helperMock);
    dbRepository.init(repositoryContextMock);

    // structured data
    Schema schema = new Schema();
    schema.setObjectDefinitions(Collections.singletonList(
        new ObjectDefinition().setName(sdObjType).setPropertyDefinitions(Collections.emptyList())));
    StructuredData.init(schema);

    // create targets for verification
    String displayCreateDate = "2017-01-15 06:30:00.0";
    String displayModDate = "2017-01-17 14:01:23.0";

    String target = "<!DOCTYPE html>\n<html lang='en'>\n<head>\n"
        + "<meta http-equiv='Content-Type' content='text/html; charset=utf-8'/>\n"
        + "<title>id1</title>\n</head>\n<body>\n"
        + "<div id='id'>\n  <p>id:</p>\n  <h1>id1</h1>\n</div>\n"
        + "<div id='name'>\n  <p>name:</p>\n  <p><small>Joe Smith</small></p>\n</div>\n"
        + "<div id='modtimestamp'>\n  <p>modtimestamp:</p>\n"
        + "  <p><small>" + displayModDate + "</small></p>\n</div>\n"
        + "<div id='createdate'>\n  <p>createdate:</p>\n"
        + "  <p><small>" + displayCreateDate + "</small></p>\n</div>\n"
        + "<div id='language'>\n  <p>language:</p>\n  <p><small>English</small></p>\n</div>\n"
        + "<div id='objtype'>\n  <p>objtype:</p>\n  <p><small>mytype</small></p>\n</div>\n"
        + "</body>\n</html>\n";
    ItemMetadata targetMetadata = new ItemMetadata().setSourceRepositoryUrl("id1")
        .setTitle("id1")
        .setCreateTime(getZonedDateTime(displayCreateDate))
        .setUpdateTime(getZonedDateTime(displayModDate))
        .setContentLanguage("English")
        .setObjectType("objtype");
    Item targetItem = new Item()
        .setName("id1")
        .setMetadata(targetMetadata)
        .setStructuredData(new ItemStructuredData()
            .setObject(new StructuredDataObject()
                .setProperties(Collections.emptyList())))
        .setAcl(null)
        .setItemType(ItemType.CONTENT_ITEM.name());
    Connection conn = factory.createConnection();
    try {
      // build the db
      Statement stmt = conn.createStatement();
      stmt.execute("create table testtable "
          + "(id varchar(32) unique not null, name varchar(128), modtimestamp timestamp, "
          + "createdate timestamp, language varchar(64), objtype varchar(32))");
      stmt.execute(
          "insert into testtable (id, name, modtimestamp, createdate, language, objtype) values "
              + "('id1', 'Joe Smith', "
              + "'" + displayModDate + "', "
              + "'" + displayCreateDate + "', "
              + "'English', 'mytype')");
      stmt.close();

      // query the db
      try (CheckpointCloseableIterable<ApiOperation> records =
          dbRepository.getAllDocs(NULL_TRAVERSAL_CHECKPOINT)) {
        RepositoryDoc record = (RepositoryDoc) records.iterator().next();
        assertEquals(targetItem, record.getItem());
        String html = CharStreams.toString(
            new InputStreamReader(record.getContent().getInputStream(), Charsets.UTF_8));
        assertEquals(target, html);
      }
    } finally {
      factory.releaseConnection(conn);
      dbRepository.close();
      factory.shutdown();
    }
  }

  @Test
  public void testGetAllDocsContentDefaultAclWithPartialMetadata() throws Exception {
    String sdTitle = "id";
    String sdMod = "modtimestamp";
    String sdCreate = "createdate";
    String sdObjType = "objtype";
    Properties config = new Properties();
    config.put(DatabaseConnectionFactory.DB_URL, getUrl());
    config.put(ColumnManager.DB_UNIQUE_KEY_COLUMNS, "id");
    config.put(ColumnManager.DB_ALL_COLUMNS, "id, name, modtimestamp, createdate, objtype");
    config.put(ColumnManager.DB_CONTENT_COLUMNS, "*");
    config.put(
        ColumnManager.DB_ALL_RECORDS_SQL,
        "select id, name, modtimestamp, createdate, language, objtype from testtable");
    config.put(DatabaseRepository.TRAVERSE_UPDATE_MODE, RequestMode.ASYNCHRONOUS.name());
    config.put(UrlBuilder.CONFIG_FORMAT, "{0}");
    config.put(UrlBuilder.CONFIG_COLUMNS, "id");
    config.put(CONFIG_TITLE_DB_FORMAT, "id");
    config.put(CONFIG_LOW_DB_FORMAT, "name, modtimestamp, createdate, objtype");
    config.put(DefaultAcl.DEFAULT_ACL_MODE, DefaultAclMode.FALLBACK.toString());
    config.put(IndexingItemBuilder.TITLE_FIELD, sdTitle);
    config.put(IndexingItemBuilder.UPDATE_TIME_FIELD, sdMod);
    config.put(IndexingItemBuilder.CREATE_TIME_FIELD, sdCreate);
    setupConfig.initConfig(config);
    InMemoryDBConnectionFactory factory = new InMemoryDBConnectionFactory();
    when(helperMock.getConnectionFactory()).thenReturn(factory);
    mockContent();
    DatabaseRepository dbRepository = new DatabaseRepository(helperMock);
    dbRepository.init(repositoryContextMock);

    // structured data
    Schema schema = new Schema();
    schema.setObjectDefinitions(
        Collections.singletonList(
            new ObjectDefinition()
                .setName(sdObjType)
                .setPropertyDefinitions(Collections.emptyList())));
    StructuredData.init(schema);

    // create target for verification
    String displayModDate = "2017-01-17 14:01:23.4";
    String displayCreateDate = "2017-01-15 06:30:00.0";
    String target =
        "<!DOCTYPE html>\n<html lang='en'>\n<head>\n"
            + "<meta http-equiv='Content-Type' content='text/html; charset=utf-8'/>\n"
            + "<title>id1</title>\n</head>\n<body>\n"
            + "<div id='id'>\n  <p>id:</p>\n  <h1>id1</h1>\n</div>\n"
            + "<div id='name'>\n  <p>name:</p>\n  <p><small>Joe Smith</small></p>\n</div>\n"
            + "<div id='modtimestamp'>\n  <p>modtimestamp:</p>\n"
            + "  <p><small>"
            + displayModDate
            + "</small></p>\n</div>\n"
            + "<div id='createdate'>\n  <p>createdate:</p>\n"
            + "  <p><small>"
            + displayCreateDate
            + "</small></p>\n</div>\n"
            + "<div id='objtype'>\n  <p>objtype:</p>\n  <p><small>mytype</small></p>\n</div>\n"
            + "</body>\n</html>\n";
    ItemMetadata targetMetadata =
        new ItemMetadata()
            .setSourceRepositoryUrl("id1")
            .setTitle("id1")
            .setUpdateTime(getZonedDateTime(displayModDate))
            .setCreateTime(getZonedDateTime(displayCreateDate));
    Item targetItem = new Item()
        .setName("id1")
        .setMetadata(targetMetadata)
        .setAcl(null)
        .setItemType(ItemType.CONTENT_ITEM.name());
    Connection conn = factory.createConnection();
    try {
      // build the db
      Statement stmt = conn.createStatement();
      stmt.execute(
          "create table testtable "
              + "(id varchar(32) unique not null, name varchar(128), modtimestamp timestamp, "
              + "createdate timestamp, language varchar(64), objtype varchar(32))");
      stmt.execute(
          "insert into testtable (id, name, modtimestamp, createdate, language, objtype) values "
              + "('id1', 'Joe Smith', "
              + "'" + displayModDate + "', "
              + "'" + displayCreateDate + "', "
              + "'English', 'mytype')");
      stmt.close();

      // query the db
      try (CheckpointCloseableIterable<ApiOperation> records =
          dbRepository.getAllDocs(NULL_TRAVERSAL_CHECKPOINT)) {
        RepositoryDoc record = (RepositoryDoc) records.iterator().next();
        assertEquals(targetItem, record.getItem());
        String html = CharStreams.toString(
            new InputStreamReader(record.getContent().getInputStream(), Charsets.UTF_8));
        assertEquals(target, html);
      }
    } finally {
      factory.releaseConnection(conn);
      dbRepository.close();
      factory.shutdown();
    }
  }

  @Test
  public void testGetAllDocsItemsDatabaseAcls() throws Exception {
    Properties config = new Properties();
    config.put(DatabaseConnectionFactory.DB_URL, getUrl());
    config.put(ColumnManager.DB_UNIQUE_KEY_COLUMNS, "id, name");
    config.put(ColumnManager.DB_ALL_COLUMNS, "id, name, phone, "
        + ColumnManager.ACL_READERS_USERS + ", "
        + ColumnManager.ACL_READERS_GROUPS + ", "
        + ColumnManager.ACL_DENIED_USERS + ", "
        + ColumnManager.ACL_DENIED_GROUPS);
    config.put(ColumnManager.DB_CONTENT_COLUMNS, "phone");
    config.put(ColumnManager.DB_ALL_RECORDS_SQL, "select id, name, phone, "
        + " readers as " + ColumnManager.ACL_READERS_USERS
        + ", reader_groups as " + ColumnManager.ACL_READERS_GROUPS
        + ", denied_readers as " + ColumnManager.ACL_DENIED_USERS
        + ", denied_groups as " + ColumnManager.ACL_DENIED_GROUPS
        + " from testtable");
    config.put(UrlBuilder.CONFIG_FORMAT, "https://acme.com/name={1}/id={0}#{1}");
    config.put(UrlBuilder.CONFIG_COLUMNS, "id, name");
    config.put(UrlBuilder.CONFIG_COLUMNS_TO_ESCAPE, "name");
    config.put(CONFIG_TITLE_DB_FORMAT, "phone");
    config.put(DefaultAcl.DEFAULT_ACL_MODE, DefaultAclMode.NONE.toString());
    config.put(DatabaseRepository.TRAVERSE_UPDATE_MODE, RequestMode.SYNCHRONOUS.name());
    setupConfig.initConfig(config);
    InMemoryDBConnectionFactory factory = new InMemoryDBConnectionFactory();
    when(helperMock.getConnectionFactory()).thenReturn(factory);
    mockContent();
    DatabaseRepository dbRepository = new DatabaseRepository(helperMock);
    dbRepository.init(repositoryContextMock);

    // create targets for verification
    List<String> targetIds = new ArrayList<>();
    targetIds.add("id1/Joe Smith");
    targetIds.add("id2/Mary Jones");
    Item targetItem1 = new Item().setName(targetIds.get(0))
        .setItemType(ItemType.CONTENT_ITEM.name());
    Item targetItem2 = new Item().setName(targetIds.get(1))
        .setItemType(ItemType.CONTENT_ITEM.name());
    String readers1 = "reader1,  reader2 ,reader3";
    String groups1 = "group1";
    String deny1 = " deny1 , deny2";
    String denyGroups1 = "denyGroup1, denyGroup2";
    Acl.createAcl(readers1, groups1, deny1, denyGroups1).applyTo(targetItem1);
    targetItem1.setMetadata(new ItemMetadata().setSourceRepositoryUrl(
        "https://acme.com/name=Joe%20Smith/id=id1#Joe%20Smith"));
    String readers2 = "reader1,  reader2 ";
    String groups2 = "group1, group2";
    String deny2 = " deny1 , deny2, deny3";
    String denyGroups2 = "";
    Acl.createAcl(readers2, groups2, deny2, denyGroups2).applyTo(targetItem2);
    targetItem2.setMetadata(new ItemMetadata().setSourceRepositoryUrl(
        "https://acme.com/name=Mary%20Jones/id=id2#Mary%20Jones"));

    Connection conn = factory.createConnection();
    try {
      // build the db
      Statement stmt = conn.createStatement();
      stmt.execute("create table testtable "
          + "(id varchar(32) unique not null, name varchar(128), phone varchar(16), "
          + "readers varchar(128), reader_groups varchar(128), denied_readers varchar(128),"
          + "denied_groups varchar(128))");
      stmt.execute("insert into testtable "
          + "(id, name, phone, readers, reader_groups, denied_readers, denied_groups) "
          + "values ('id1', 'Joe Smith', '1234', '" + readers1 + "', '" + groups1 + "', '"
          + deny1 + "', '" + denyGroups1 + "')");
      stmt.execute("insert into testtable "
          + "(id, name, phone, readers, reader_groups, denied_readers) "
          + "values ('id2', 'Mary Jones', '2134', '" + readers2 + "', '" + groups2 + "', '"
          + deny2 + "')"); // tests missing "denied groups"
      stmt.close();

      // query the db
      try (CheckpointCloseableIterable<ApiOperation> allDocs =
          dbRepository.getAllDocs(NULL_TRAVERSAL_CHECKPOINT)) {
        for (ApiOperation op : allDocs) {
          RepositoryDoc record = (RepositoryDoc) op;
          Item item = record.getItem();
          String recordId = item.getName();
          assertTrue(targetIds.contains(recordId));
          if (recordId.equals(targetIds.get(0))) {
            assertEquals(targetItem1, item);
          } else if (recordId.equals(targetIds.get(1))) {
            assertEquals(targetItem2, item);
          } else {
            fail("Unknown ID:" + recordId);
          }
        }
      }
    } finally {
      factory.releaseConnection(conn);
      dbRepository.close();
      factory.shutdown();
    }
  }

  /** Tests one row with ACL values and a second row without. */
  @Test
  public void testGetAllDocs_missingAcl() throws Exception {
    Properties config = new Properties();
    config.put(DatabaseConnectionFactory.DB_URL, getUrl());
    config.put(ColumnManager.DB_UNIQUE_KEY_COLUMNS, "id, name");
    config.put(ColumnManager.DB_ALL_COLUMNS, "id, name, phone, "
        + ColumnManager.ACL_READERS_USERS + ", "
        + ColumnManager.ACL_READERS_GROUPS + ", "
        + ColumnManager.ACL_DENIED_USERS + ", "
        + ColumnManager.ACL_DENIED_GROUPS);
    config.put(ColumnManager.DB_CONTENT_COLUMNS, "phone");
    config.put(ColumnManager.DB_ALL_RECORDS_SQL, "select id, name, phone, "
        + " readers as " + ColumnManager.ACL_READERS_USERS
        + ", reader_groups as " + ColumnManager.ACL_READERS_GROUPS
        + ", denied_readers as " + ColumnManager.ACL_DENIED_USERS
        + ", denied_groups as " + ColumnManager.ACL_DENIED_GROUPS
        + " from testtable");
    config.put(UrlBuilder.CONFIG_FORMAT, "https://acme.com/name={1}/id={0}#{1}");
    config.put(UrlBuilder.CONFIG_COLUMNS, "id, name");
    config.put(UrlBuilder.CONFIG_COLUMNS_TO_ESCAPE, "name");
    config.put(CONFIG_TITLE_DB_FORMAT, "phone");
    config.put(DefaultAcl.DEFAULT_ACL_MODE, DefaultAclMode.FALLBACK.toString());
    config.put(DefaultAcl.DEFAULT_ACL_PUBLIC, "true");
    config.put(DatabaseRepository.TRAVERSE_UPDATE_MODE, RequestMode.SYNCHRONOUS.name());
    setupConfig.initConfig(config);

    // Create the default ACL (normally FullTraversalConnector does this).
    IndexingService indexingServiceMock = mock(IndexingService.class);
    DefaultAcl defaultAcl = DefaultAcl.fromConfiguration(indexingServiceMock);
    assertEquals(DefaultAclMode.FALLBACK, defaultAcl.getDefaultAclMode());

    InMemoryDBConnectionFactory factory = new InMemoryDBConnectionFactory();
    when(helperMock.getConnectionFactory()).thenReturn(factory);
    mockContent();
    DatabaseRepository dbRepository = new DatabaseRepository(helperMock);
    dbRepository.init(repositoryContextMock);

    // create targets for verification
    List<String> targetIds = new ArrayList<>();
    targetIds.add("id1/Joe Smith");
    targetIds.add("id2/Mary Jones");
    Item targetItem1 = new Item().setName(targetIds.get(0))
        .setItemType(ItemType.CONTENT_ITEM.name());
    Item targetItem2 = new Item().setName(targetIds.get(1))
        .setItemType(ItemType.CONTENT_ITEM.name());
    String readers1 = "reader1,  reader2 ,reader3";
    String groups1 = "group1";
    String deny1 = " deny1 , deny2";
    String denyGroups1 = "denyGroup1, denyGroup2";
    Acl.createAcl(readers1, groups1, deny1, denyGroups1).applyTo(targetItem1);
    targetItem1.setMetadata(new ItemMetadata().setSourceRepositoryUrl(
        "https://acme.com/name=Joe%20Smith/id=id1#Joe%20Smith"));
    new Acl.Builder()
        .setReaders(ImmutableList.of(Acl.getCustomerPrincipal()))
        .build()
        .applyTo(targetItem2);
    targetItem2.setMetadata(new ItemMetadata().setSourceRepositoryUrl(
        "https://acme.com/name=Mary%20Jones/id=id2#Mary%20Jones"));

    Connection conn = factory.createConnection();
    try {
      // build the db
      Statement stmt = conn.createStatement();
      stmt.execute("create table testtable "
          + "(id varchar(32) unique not null, name varchar(128), phone varchar(16), "
          + "readers varchar(128), reader_groups varchar(128), denied_readers varchar(128),"
          + "denied_groups varchar(128))");
      stmt.execute("insert into testtable "
          + "(id, name, phone, readers, reader_groups, denied_readers, denied_groups) "
          + "values ('id1', 'Joe Smith', '1234', '" + readers1 + "', '" + groups1 + "', '"
          + deny1 + "', '" + denyGroups1 + "')");
      stmt.execute("insert into testtable "
          + "(id, name, phone) "
          + "values ('id2', 'Mary Jones', '2134')");
      stmt.close();

      // query the db
      try (CheckpointCloseableIterable<ApiOperation> allDocs =
          dbRepository.getAllDocs(NULL_TRAVERSAL_CHECKPOINT)) {
        for (ApiOperation op : allDocs) {
          RepositoryDoc record = (RepositoryDoc) op;
          Item item = record.getItem();
          String recordId = item.getName();
          assertTrue(targetIds.contains(recordId));
          if (recordId.equals(targetIds.get(0))) {
            assertFalse(defaultAcl.applyToIfEnabled(item));
            assertEquals(targetItem1, item);
          } else if (recordId.equals(targetIds.get(1))) {
            assertTrue(defaultAcl.applyToIfEnabled(item));
            assertEquals(targetItem2, item);
          } else {
            fail("Unknown ID: " + recordId);
          }
        }
      }
    } finally {
      factory.releaseConnection(conn);
      dbRepository.close();
      factory.shutdown();
    }
  }

  @Test
  public void testBlobColumn() throws Exception {
    Properties config = new Properties();
    config.put(DatabaseConnectionFactory.DB_URL, getUrl());
    config.put(ColumnManager.DB_UNIQUE_KEY_COLUMNS, "id");
    config.put(ColumnManager.DB_ALL_COLUMNS, "id, name, blob_data");
    config.put(ColumnManager.DB_ALL_RECORDS_SQL, "select id, name, blob_data from testtable");
    config.put(ColumnManager.DB_BLOB_COLUMN, "blob_data");
    config.put(ColumnManager.DB_CONTENT_COLUMNS, "*");
    config.put(UrlBuilder.CONFIG_FORMAT, "{0}");
    config.put(UrlBuilder.CONFIG_COLUMNS, "name");
    config.put(DefaultAcl.DEFAULT_ACL_MODE, DefaultAclMode.FALLBACK.toString());
    setupConfig.initConfig(config);
    InMemoryDBConnectionFactory factory = new InMemoryDBConnectionFactory();
    when(helperMock.getConnectionFactory()).thenReturn(factory);
    DatabaseRepository dbRepository = new DatabaseRepository(helperMock);
    dbRepository.init(repositoryContextMock);

    // create target for verification
    String targetContent = "This is some blob content." + '\0' + '\t';
    Connection conn = factory.createConnection();
    try {
      // build the db
      PreparedStatement stmt = conn.prepareStatement("create table testtable "
          + "(id varchar(32) unique not null, name varchar(128), blob_data blob)");
      stmt.execute();
      stmt.close();
      stmt = conn.prepareStatement(
          "insert into testtable (id, name, blob_data) values ('id1', 'Joe Smith', ?)");
      Blob targetBlob = conn.createBlob();
      targetBlob.setBytes(1, targetContent.getBytes(UTF_8));
      stmt.setBlob(1, targetBlob);
      stmt.execute();
      stmt.close();
      targetBlob.free();

      // query the db
      try (CheckpointCloseableIterable<ApiOperation> records =
          dbRepository.getAllDocs(NULL_TRAVERSAL_CHECKPOINT)) {
        RepositoryDoc record = (RepositoryDoc) records.iterator().next();
        String html = CharStreams.toString(
            new InputStreamReader(record.getContent().getInputStream(), Charsets.UTF_8));
        assertEquals(targetContent, html);
      }
    } finally {
      factory.releaseConnection(conn);
      dbRepository.close();
      factory.shutdown();
    }
  }

  @Test
  public void testClobColumn() throws Exception {
    Properties config = new Properties();
    config.put(DatabaseConnectionFactory.DB_URL, getUrl());
    config.put(ColumnManager.DB_UNIQUE_KEY_COLUMNS, "id");
    config.put(ColumnManager.DB_ALL_COLUMNS, "id, name, clob_data");
    config.put(ColumnManager.DB_ALL_RECORDS_SQL, "select id, name, clob_data from testtable");
    config.put(ColumnManager.DB_BLOB_COLUMN, "clob_data");
    config.put(ColumnManager.DB_CONTENT_COLUMNS, "*");
    config.put(UrlBuilder.CONFIG_FORMAT, "{0}");
    config.put(UrlBuilder.CONFIG_COLUMNS, "name");
    config.put(DefaultAcl.DEFAULT_ACL_MODE, DefaultAclMode.FALLBACK.toString());
    setupConfig.initConfig(config);
    InMemoryDBConnectionFactory factory = new InMemoryDBConnectionFactory();
    when(helperMock.getConnectionFactory()).thenReturn(factory);
    DatabaseRepository dbRepository = new DatabaseRepository(helperMock);
    dbRepository.init(repositoryContextMock);

    // create target for verification
    String targetContent = "This is some clob content.";
    Connection conn = factory.createConnection();
    try {
      // build the db
      PreparedStatement stmt = conn.prepareStatement("create table testtable "
          + "(id varchar(32) unique not null, name varchar(128), clob_data clob)");
      stmt.execute();
      stmt.close();
      stmt = conn.prepareStatement(
          "insert into testtable (id, name, clob_data) values ('id1', 'Joe Smith', ?)");
      Clob targetClob = conn.createClob();
      targetClob.setString(1, targetContent);
      stmt.setClob(1, targetClob);
      stmt.execute();
      stmt.close();
      targetClob.free();

      // query the db
      try (CheckpointCloseableIterable<ApiOperation> records =
          dbRepository.getAllDocs(NULL_TRAVERSAL_CHECKPOINT)) {
        RepositoryDoc record = (RepositoryDoc) records.iterator().next();
        String html = CharStreams.toString(
            new InputStreamReader(record.getContent().getInputStream(), Charsets.UTF_8));
        assertEquals(targetContent, html);
      }
    } finally {
      factory.releaseConnection(conn);
      dbRepository.close();
      factory.shutdown();
    }
  }

  @Test
  public void testNClobColumn() throws Exception {
    Properties config = new Properties();
    config.put(DatabaseConnectionFactory.DB_URL, getUrl());
    config.put(ColumnManager.DB_UNIQUE_KEY_COLUMNS, "id");
    config.put(ColumnManager.DB_ALL_COLUMNS, "id, name, nclob_data");
    config.put(ColumnManager.DB_ALL_RECORDS_SQL, "select id, name, nclob_data from testtable");
    config.put(ColumnManager.DB_BLOB_COLUMN, "nclob_data");
    config.put(ColumnManager.DB_CONTENT_COLUMNS, "*");
    config.put(UrlBuilder.CONFIG_FORMAT, "{0}");
    config.put(UrlBuilder.CONFIG_COLUMNS, "name");
    config.put(DefaultAcl.DEFAULT_ACL_MODE, DefaultAclMode.FALLBACK.toString());
    setupConfig.initConfig(config);
    InMemoryDBConnectionFactory factory = new InMemoryDBConnectionFactory();
    when(helperMock.getConnectionFactory()).thenReturn(factory);
    DatabaseRepository dbRepository = new DatabaseRepository(helperMock);
    dbRepository.init(repositoryContextMock);

    // create target for verification
    String targetContent = "This is some nclob content.";
    Connection conn = factory.createConnection();
    try {
      // build the db
      PreparedStatement stmt = conn.prepareStatement("create table testtable "
          + "(id varchar(32) unique not null, name varchar(128), nclob_data nclob)");
      stmt.execute();
      stmt.close();
      stmt = conn.prepareStatement(
          "insert into testtable (id, name, nclob_data) values ('id1', 'Joe Smith', ?)");
      NClob targetNClob = conn.createNClob();
      targetNClob.setString(1, targetContent);
      stmt.setNClob(1, targetNClob);
      stmt.execute();
      stmt.close();
      targetNClob.free();

      // query the db
      try (CheckpointCloseableIterable<ApiOperation> records =
          dbRepository.getAllDocs(NULL_TRAVERSAL_CHECKPOINT)) {
        RepositoryDoc record = (RepositoryDoc) records.iterator().next();
        String html = CharStreams.toString(
            new InputStreamReader(record.getContent().getInputStream(), Charsets.UTF_8));
        assertEquals(targetContent, html);
      }
    } finally {
      factory.releaseConnection(conn);
      dbRepository.close();
      factory.shutdown();
    }
  }

  private void validateResults(String createSql, String insertSql, String expected)
      throws Exception {
    Properties config = new Properties();
    config.put(DatabaseConnectionFactory.DB_URL, getUrl());
    config.put(ColumnManager.DB_UNIQUE_KEY_COLUMNS, "id");
    config.put(ColumnManager.DB_ALL_COLUMNS, "id, name, data");
    config.put(ColumnManager.DB_CONTENT_COLUMNS, "*");
    config.put(ColumnManager.DB_ALL_RECORDS_SQL,
        "select id, name, data from testtable");
    config.put(UrlBuilder.CONFIG_FORMAT, "{0}");
    config.put(UrlBuilder.CONFIG_COLUMNS, "id");
    config.put(CONFIG_TITLE_DB_FORMAT, "id");
    config.put(DefaultAcl.DEFAULT_ACL_MODE, DefaultAclMode.FALLBACK.toString());
    setupConfig.initConfig(config);
    InMemoryDBConnectionFactory factory = new InMemoryDBConnectionFactory();
    when(helperMock.getConnectionFactory()).thenReturn(factory);
    mockContent();
    DatabaseRepository dbRepository = new DatabaseRepository(helperMock);
    dbRepository.init(repositoryContextMock);

    Connection conn = factory.createConnection();
    try {
      // build the db
      Statement stmt = conn.createStatement();
      stmt.execute(createSql);
      stmt.execute(insertSql);
      stmt.close();

      // query the db
      try (CheckpointCloseableIterable<ApiOperation> records =
               dbRepository.getAllDocs(NULL_TRAVERSAL_CHECKPOINT)) {
        RepositoryDoc record = (RepositoryDoc) records.iterator().next();
        String html = CharStreams.toString(
            new InputStreamReader(record.getContent().getInputStream(), Charsets.UTF_8));
        assertEquals(expected, html);
      }
    } finally {
      factory.releaseConnection(conn);
      dbRepository.close();
      factory.shutdown();
    }
  }

  @Test
  public void testArrayColumn() throws Exception {
    String createSql = "create table testtable "
        + "(id varchar(32) unique not null, name varchar(128), data array)";
    String insertSql = "insert into testtable (id, name, data) "
        + "values ('id1', 'Joe Smith', ('joe', 'smith', 'jsmith'))";

    String expected = "<!DOCTYPE html>\n<html lang='en'>\n<head>\n"
        + "<meta http-equiv='Content-Type' content='text/html; charset=utf-8'/>\n"
        + "<title>id1</title>\n</head>\n<body>\n"
        + "<div id='id'>\n  <p>id:</p>\n  <h1>id1</h1>\n</div>\n"
        + "<div id='data'>\n  <p>data:</p>\n  <p><small>[joe, smith, jsmith]</small></p>\n</div>\n"
        + "<div id='name'>\n  <p>name:</p>\n  <p><small>Joe Smith</small></p>\n</div>\n"
        + "</body>\n</html>\n";

    validateResults(createSql, insertSql, expected);
  }

  @Test
  public void testArrayColumn_NullValue() throws Exception {
    String createSql = "create table testtable "
        + "(id varchar(32) unique not null, name varchar(128), data array)";
    String insertSql = "insert into testtable (id, name) "
        + "values ('id1', 'Joe Smith')";

    String expected = "<!DOCTYPE html>\n<html lang='en'>\n<head>\n"
        + "<meta http-equiv='Content-Type' content='text/html; charset=utf-8'/>\n"
        + "<title>id1</title>\n</head>\n<body>\n"
        + "<div id='id'>\n  <p>id:</p>\n  <h1>id1</h1>\n</div>\n"
        + "<div id='name'>\n  <p>name:</p>\n  <p><small>Joe Smith</small></p>\n</div>\n"
        + "</body>\n</html>\n";

    validateResults(createSql, insertSql, expected);
  }

  @Test
  @Parameters({
    "date, {d \'2004-10-06\'}, 2004-10-06",
    "time, {t \'09:15:30\'}, 09:15:30",
    "timestamp, {ts \'2004-10-06 09:15:30.0\'}, 2004-10-06 09:15:30.0"
  })
  public void dateTimeValues_insertedCorrectly(String type, String value, String expected)
      throws Exception {
    String createSql = "create table testtable "
        + "(id varchar(32) unique not null, name varchar(128), data " + type + ")";
    String insertSql = "insert into testtable (id, name, data) "
        + "values ('id1', 'Joe Smith', " + value + ")";

    String result = "<!DOCTYPE html>\n<html lang='en'>\n<head>\n"
        + "<meta http-equiv='Content-Type' content='text/html; charset=utf-8'/>\n"
        + "<title>id1</title>\n</head>\n<body>\n"
        + "<div id='id'>\n  <p>id:</p>\n  <h1>id1</h1>\n</div>\n"
        + "<div id='data'>\n  <p>data:</p>\n  <p><small>" + expected + "</small></p>\n</div>\n"
        + "<div id='name'>\n  <p>name:</p>\n  <p><small>Joe Smith</small></p>\n</div>\n"
        + "</body>\n</html>\n";

    validateResults(createSql, insertSql, result);
  }

  @Test
  public void testLongVarBinaryColumn() throws Exception {
    Properties config = new Properties();
    config.put(DatabaseConnectionFactory.DB_URL, getUrl());
    config.put(ColumnManager.DB_UNIQUE_KEY_COLUMNS, "id");
    config.put(ColumnManager.DB_ALL_COLUMNS, "id, name, longvarbinary_data");
    config.put(ColumnManager.DB_ALL_RECORDS_SQL,
        "select id, name, longvarbinary_data from testtable");
    config.put(ColumnManager.DB_BLOB_COLUMN, "longvarbinary_data");
    config.put(ColumnManager.DB_CONTENT_COLUMNS, "*");
    config.put(UrlBuilder.CONFIG_FORMAT, "{0}");
    config.put(UrlBuilder.CONFIG_COLUMNS, "name");
    config.put(DefaultAcl.DEFAULT_ACL_MODE, DefaultAclMode.FALLBACK.toString());
    setupConfig.initConfig(config);
    InMemoryDBConnectionFactory factory = new InMemoryDBConnectionFactory();
    when(helperMock.getConnectionFactory()).thenReturn(factory);
    DatabaseRepository dbRepository = new DatabaseRepository(helperMock);
    dbRepository.init(repositoryContextMock);

    // create target for verification
    String targetContent = "This is some longvarbinary content.";
    Connection conn = factory.createConnection();
    try {
      // build the db
      PreparedStatement stmt = conn.prepareStatement("create table testtable "
          + "(id varchar(32) unique not null, name varchar(128), "
          + "longvarbinary_data longvarbinary)");
      stmt.execute();
      stmt.close();
      stmt = conn.prepareStatement(
          "insert into testtable (id, name, longvarbinary_data) values ('id1', 'Joe Smith', ?)");
      Blob targetBlob = conn.createBlob();
      targetBlob.setBytes(1, targetContent.getBytes(UTF_8));
      stmt.setBlob(1, targetBlob);
      stmt.execute();
      stmt.close();
      targetBlob.free();

      // query the db
      try (CheckpointCloseableIterable<ApiOperation> records =
          dbRepository.getAllDocs(NULL_TRAVERSAL_CHECKPOINT)) {
        RepositoryDoc record = (RepositoryDoc) records.iterator().next();
        String html = CharStreams.toString(
            new InputStreamReader(record.getContent().getInputStream(), Charsets.UTF_8));
        assertEquals(targetContent, html);
      }
    } finally {
      factory.releaseConnection(conn);
      dbRepository.close();
      factory.shutdown();
    }
  }

  @Test
  public void testVarBinaryColumn() throws Exception {
    Properties config = new Properties();
    config.put(DatabaseConnectionFactory.DB_URL, getUrl());
    config.put(ColumnManager.DB_UNIQUE_KEY_COLUMNS, "id");
    config.put(ColumnManager.DB_ALL_COLUMNS, "id, name, varbinary_data");
    config.put(ColumnManager.DB_ALL_RECORDS_SQL,
        "select id, name, varbinary_data from testtable");
    config.put(ColumnManager.DB_BLOB_COLUMN, "varbinary_data");
    config.put(ColumnManager.DB_CONTENT_COLUMNS, "*");
    config.put(UrlBuilder.CONFIG_FORMAT, "{0}");
    config.put(UrlBuilder.CONFIG_COLUMNS, "name");
    config.put(DefaultAcl.DEFAULT_ACL_MODE, DefaultAclMode.FALLBACK.toString());
    setupConfig.initConfig(config);
    InMemoryDBConnectionFactory factory = new InMemoryDBConnectionFactory();
    when(helperMock.getConnectionFactory()).thenReturn(factory);
    DatabaseRepository dbRepository = new DatabaseRepository(helperMock);
    dbRepository.init(repositoryContextMock);

    // create target for verification
    String targetContent = "This is some varbinary content.";
    Connection conn = factory.createConnection();
    try {
      // build the db
      PreparedStatement stmt = conn.prepareStatement("create table testtable "
          + "(id varchar(32) unique not null, name varchar(128), varbinary_data varbinary)");
      stmt.execute();
      stmt.close();
      stmt = conn.prepareStatement(
          "insert into testtable (id, name, varbinary_data) values ('id1', 'Joe Smith', ?)");
      Blob targetBlob = conn.createBlob();
      targetBlob.setBytes(1, targetContent.getBytes(UTF_8));
      stmt.setBlob(1, targetBlob);
      stmt.execute();
      stmt.close();
      targetBlob.free();

      // query the db
      try (CheckpointCloseableIterable<ApiOperation> records =
          dbRepository.getAllDocs(NULL_TRAVERSAL_CHECKPOINT)) {
        RepositoryDoc record = (RepositoryDoc) records.iterator().next();
        String html = CharStreams.toString(
            new InputStreamReader(record.getContent().getInputStream(), Charsets.UTF_8));
        assertEquals(targetContent, html);
      }
    } finally {
      factory.releaseConnection(conn);
      dbRepository.close();
      factory.shutdown();
    }
  }

  @Test
  public void testVarcharColumn() throws Exception {
    Properties config = new Properties();
    config.put(DatabaseConnectionFactory.DB_URL, getUrl());
    config.put(ColumnManager.DB_UNIQUE_KEY_COLUMNS, "id");
    config.put(ColumnManager.DB_ALL_COLUMNS, "id, name, blob_data");
    config.put(ColumnManager.DB_ALL_RECORDS_SQL, "select id, name, blob_data from testtable");
    config.put(ColumnManager.DB_BLOB_COLUMN, "blob_data");
    config.put(ColumnManager.DB_CONTENT_COLUMNS, "*");
    config.put(UrlBuilder.CONFIG_FORMAT, "{0}");
    config.put(UrlBuilder.CONFIG_COLUMNS, "name");
    config.put(DefaultAcl.DEFAULT_ACL_MODE, DefaultAclMode.FALLBACK.toString());
    setupConfig.initConfig(config);
    InMemoryDBConnectionFactory factory = new InMemoryDBConnectionFactory();
    when(helperMock.getConnectionFactory()).thenReturn(factory);
    DatabaseRepository dbRepository = new DatabaseRepository(helperMock);
    dbRepository.init(repositoryContextMock);

    // create target for verification
    Connection conn = factory.createConnection();
    try {
      // build the db
      String targetContent = "some text";
      PreparedStatement stmt = conn.prepareStatement("create table testtable "
          + "(id varchar(32) unique not null, name varchar(128), blob_data varchar(2048))");
      stmt.execute();
      stmt.close();
      stmt = conn.prepareStatement(
          "insert into testtable (id, name, blob_data) values ('id1', 'Joe Smith', 'some text')");
      stmt.execute();
      stmt.close();

      // query the db
      try (CheckpointCloseableIterable<ApiOperation> records =
          dbRepository.getAllDocs(NULL_TRAVERSAL_CHECKPOINT)) {
        RepositoryDoc record = (RepositoryDoc) records.iterator().next();
        String html = CharStreams.toString(
            new InputStreamReader(record.getContent().getInputStream(), Charsets.UTF_8));
        assertEquals(targetContent, html);
      }
    } finally {
      factory.releaseConnection(conn);
      dbRepository.close();
      factory.shutdown();
    }
  }

  @Test
  public void testInvalidBlobColumn() throws Exception {
    Properties config = new Properties();
    config.put(DatabaseConnectionFactory.DB_URL, getUrl());
    config.put(ColumnManager.DB_UNIQUE_KEY_COLUMNS, "id");
    config.put(ColumnManager.DB_ALL_COLUMNS, "id, name, blob_data");
    config.put(ColumnManager.DB_ALL_RECORDS_SQL, "select id, name, blob_data from testtable");
    config.put(ColumnManager.DB_BLOB_COLUMN, "blob_data");
    config.put(ColumnManager.DB_CONTENT_COLUMNS, "*");
    config.put(UrlBuilder.CONFIG_FORMAT, "{0}");
    config.put(UrlBuilder.CONFIG_COLUMNS, "name");
    config.put(DefaultAcl.DEFAULT_ACL_MODE, DefaultAclMode.FALLBACK.toString());
    setupConfig.initConfig(config);
    InMemoryDBConnectionFactory factory = new InMemoryDBConnectionFactory();
    when(helperMock.getConnectionFactory()).thenReturn(factory);
    DatabaseRepository dbRepository = new DatabaseRepository(helperMock);
    dbRepository.init(repositoryContextMock);

    // create target for verification
    Connection conn = factory.createConnection();
    try {
      // build the db
      PreparedStatement stmt = conn.prepareStatement("create table testtable "
          + "(id varchar(32) unique not null, name varchar(128), blob_data bigint)");
      stmt.execute();
      stmt.close();
      stmt = conn.prepareStatement(
          "insert into testtable (id, name, blob_data) values ('id1', 'Joe Smith', 42)");
      stmt.execute();
      stmt.close();

      // query the db
      try (CheckpointCloseableIterable<ApiOperation> records =
          dbRepository.getAllDocs(NULL_TRAVERSAL_CHECKPOINT)) {
        thrown.expect(InvalidConfigurationException.class);
        thrown.expectMessage("Invalid Blob column type");
        thrown.expectMessage("Long");
        records.iterator().next();
      }
    } finally {
      factory.releaseConnection(conn);
      dbRepository.close();
      factory.shutdown();
    }
  }

  @Test
  public void testNullVarBinaryColumn() throws Exception {
    Properties config = new Properties();
    config.put(DatabaseConnectionFactory.DB_URL, getUrl());
    config.put(ColumnManager.DB_UNIQUE_KEY_COLUMNS, "id");
    config.put(ColumnManager.DB_ALL_COLUMNS, "id, name, varbinary_data");
    config.put(ColumnManager.DB_ALL_RECORDS_SQL,
        "select id, name, varbinary_data from testtable");
    config.put(CONFIG_TITLE_DB_FORMAT, "id");
    config.put(ColumnManager.DB_CONTENT_COLUMNS, "*");
    config.put(UrlBuilder.CONFIG_FORMAT, "{0}");
    config.put(UrlBuilder.CONFIG_COLUMNS, "name");
    config.put(DefaultAcl.DEFAULT_ACL_MODE, DefaultAclMode.FALLBACK.toString());
    setupConfig.initConfig(config);
    InMemoryDBConnectionFactory factory = new InMemoryDBConnectionFactory();
    when(helperMock.getConnectionFactory()).thenReturn(factory);
    mockContent();
    DatabaseRepository dbRepository = new DatabaseRepository(helperMock);
    dbRepository.init(repositoryContextMock);

    // create target for verification
    Connection conn = factory.createConnection();
    try {
      // build the db
      PreparedStatement stmt = conn.prepareStatement("create table testtable "
          + "(id varchar(32) unique not null, name varchar(128), varbinary_data varbinary)");
      stmt.execute();
      stmt.close();
      stmt = conn.prepareStatement(
          "insert into testtable (id, name) values ('id1', 'Joe Smith')");
      stmt.execute();
      stmt.close();

      // query the db
      try (CheckpointCloseableIterable<ApiOperation> records =
          dbRepository.getAllDocs(NULL_TRAVERSAL_CHECKPOINT)) {
        RepositoryDoc record = (RepositoryDoc) records.iterator().next();
        String html = CharStreams.toString(
            new InputStreamReader(record.getContent().getInputStream(), Charsets.UTF_8));
        assertTrue(html.contains("id1"));
        assertTrue(html.contains("Joe Smith"));
      }
    } finally {
      factory.releaseConnection(conn);
      dbRepository.close();
      factory.shutdown();
    }
  }

  @Test
  public void testNullVarBinaryBlobColumn() throws Exception {
    Properties config = new Properties();
    config.put(DatabaseConnectionFactory.DB_URL, getUrl());
    config.put(ColumnManager.DB_UNIQUE_KEY_COLUMNS, "id");
    config.put(ColumnManager.DB_ALL_COLUMNS, "id, name, varbinary_data");
    config.put(ColumnManager.DB_ALL_RECORDS_SQL,
        "select id, name, varbinary_data from testtable");
    config.put(ColumnManager.DB_BLOB_COLUMN, "varbinary_data");
    config.put(ColumnManager.DB_CONTENT_COLUMNS, "*");
    config.put(UrlBuilder.CONFIG_FORMAT, "{0}");
    config.put(UrlBuilder.CONFIG_COLUMNS, "id");
    config.put(DefaultAcl.DEFAULT_ACL_MODE, DefaultAclMode.FALLBACK.toString());
    setupConfig.initConfig(config);
    InMemoryDBConnectionFactory factory = new InMemoryDBConnectionFactory();
    when(helperMock.getConnectionFactory()).thenReturn(factory);
    mockContent();
    DatabaseRepository dbRepository = new DatabaseRepository(helperMock);
    dbRepository.init(repositoryContextMock);

    // create target for verification
    ItemMetadata targetMetadata = new ItemMetadata().setSourceRepositoryUrl("id1");
    Item targetItem = new Item()
        .setName("id1")
        .setMetadata(targetMetadata)
        .setAcl(null)
        .setItemType(ItemType.CONTENT_ITEM.name());
    Connection conn = factory.createConnection();
    try {
      // build the db
      PreparedStatement stmt = conn.prepareStatement("create table testtable "
          + "(id varchar(32) unique not null, name varchar(128), varbinary_data varbinary)");
      stmt.execute();
      stmt.close();
      stmt = conn.prepareStatement(
          "insert into testtable (id, name) values ('id1', 'Joe Smith')");
      stmt.execute();
      stmt.close();

      // query the db
      try (CheckpointCloseableIterable<ApiOperation> records =
          dbRepository.getAllDocs(NULL_TRAVERSAL_CHECKPOINT)) {
        RepositoryDoc record = (RepositoryDoc) records.iterator().next();
        assertEquals(targetItem, record.getItem());
      }
    } finally {
      factory.releaseConnection(conn);
      dbRepository.close();
      factory.shutdown();
    }
  }

  @Test
  public void testGetAllDocsSqlAccessError() throws Exception {
    Properties config = new Properties();
    setAllMandatory(config);
    config.put(DatabaseConnectionFactory.DB_URL, getUrl());
    setupConfig.initConfig(config);
    InMemoryDBConnectionFactory factory = new InMemoryDBConnectionFactory();
    when(helperMock.getConnectionFactory()).thenReturn(factory);
    DatabaseRepository dbRepository = new DatabaseRepository(helperMock);
    dbRepository.init(repositoryContextMock);
    thrown.expect(RepositoryException.class);
    thrown.expectMessage("Error with SQL query");
    try {
      dbRepository.getAllDocs(NULL_TRAVERSAL_CHECKPOINT); // no table
    } finally {
      dbRepository.close();
      factory.shutdown();
    }
  }

  @Test
  public void testGetAllDocs_paginationMissing_checkpointNull() throws Exception {
    Properties config = new Properties();
    setAllMandatory(config);
    config.put(DatabaseConnectionFactory.DB_URL, getUrl());
    setupConfig.initConfig(config);
    InMemoryDBConnectionFactory factory = new InMemoryDBConnectionFactory();
    when(helperMock.getConnectionFactory()).thenReturn(factory);
    DatabaseRepository dbRepository = new DatabaseRepository(helperMock);
    dbRepository.init(repositoryContextMock);

    Connection conn = factory.createConnection();
    try {
      try (Statement stmt = conn.createStatement()) {
        buildTable(stmt);
      }

      List<String> expectedIds = ImmutableList.of(
          "id1/Joe Smith", "id2/Mary Jones", "id3/Mike Brown", "id4/Sue Green", "id5/Betty Black");

      try (CheckpointCloseableIterable<ApiOperation> allDocs = dbRepository.getAllDocs(null)) {
        assertEquals(expectedIds,
            ImmutableList.copyOf(
                Iterables.transform(allDocs, v -> ((RepositoryDoc) v).getItem().getName())));
        assertNull(allDocs.getCheckpoint());
        assertFalse(allDocs.hasMore());
      }
    } finally {
      factory.releaseConnection(conn);
      dbRepository.close();
      factory.shutdown();
    }
  }

  @Test
  public void testGetAllDocs_paginationOffset_checkpointNull() throws Exception {
    Properties config = new Properties();
    setAllMandatory(config);
    config.put(DatabaseConnectionFactory.DB_URL, getUrl());
    config.put(ColumnManager.DB_ALL_RECORDS_SQL,
        config.get(ColumnManager.DB_ALL_RECORDS_SQL) + " limit 4 offset ?");
    config.put(ColumnManager.DB_PAGINATION, "offset");
    setupConfig.initConfig(config);
    InMemoryDBConnectionFactory factory = new InMemoryDBConnectionFactory();
    when(helperMock.getConnectionFactory()).thenReturn(factory);
    DatabaseRepository dbRepository = new DatabaseRepository(helperMock);
    dbRepository.init(repositoryContextMock);

    Connection conn = factory.createConnection();
    try {
      try (Statement stmt = conn.createStatement()) {
        buildTable(stmt);
      }

      List<String> expectedIds = ImmutableList.of(
          "id1/Joe Smith", "id2/Mary Jones", "id3/Mike Brown", "id4/Sue Green");

      try (CheckpointCloseableIterable<ApiOperation> allDocs = dbRepository.getAllDocs(null)) {
        assertEquals(expectedIds,
            ImmutableList.copyOf(
                Iterables.transform(allDocs, v -> ((RepositoryDoc) v).getItem().getName())));
        assertCheckpointEquals(
            new FullCheckpoint().setPagination(Pagination.OFFSET).setOffset(4),
            allDocs.getCheckpoint());
        assertTrue(allDocs.hasMore());
      }
    } finally {
      factory.releaseConnection(conn);
      dbRepository.close();
      factory.shutdown();
    }
  }

  @Test
  public void testGetAllDocs_paginationOffset_checkpointInvalid() throws Exception {
    Properties config = new Properties();
    setAllMandatory(config);
    config.put(DatabaseConnectionFactory.DB_URL, getUrl());
    config.put(ColumnManager.DB_ALL_RECORDS_SQL,
        config.get(ColumnManager.DB_ALL_RECORDS_SQL) + " limit 4 offset ?");
    config.put(ColumnManager.DB_PAGINATION, "offset");
    setupConfig.initConfig(config);
    InMemoryDBConnectionFactory factory = new InMemoryDBConnectionFactory();
    when(helperMock.getConnectionFactory()).thenReturn(factory);
    DatabaseRepository dbRepository = new DatabaseRepository(helperMock);
    dbRepository.init(repositoryContextMock);

    Connection conn = factory.createConnection();
    try {
      try (Statement stmt = conn.createStatement()) {
        buildTable(stmt);
      }

      List<String> expectedIds = ImmutableList.of(
          "id1/Joe Smith", "id2/Mary Jones", "id3/Mike Brown", "id4/Sue Green");

      byte[] checkpoint = new FullCheckpoint().setPagination(Pagination.NONE)
          .toString()
          .replace("none", "foo")
          .getBytes(UTF_8);

      try (CheckpointCloseableIterable<ApiOperation> allDocs = dbRepository.getAllDocs(null)) {
        assertEquals(expectedIds,
            ImmutableList.copyOf(
                Iterables.transform(allDocs, v -> ((RepositoryDoc) v).getItem().getName())));
        assertCheckpointEquals(
            new FullCheckpoint().setPagination(Pagination.OFFSET).setOffset(4),
            allDocs.getCheckpoint());
        assertTrue(allDocs.hasMore());
      }
    } finally {
      factory.releaseConnection(conn);
      dbRepository.close();
      factory.shutdown();
    }
  }

  @Test
  public void testGetAllDocs_paginationNone_checkpointOffset() throws Exception {
    Properties config = new Properties();
    setAllMandatory(config);
    config.put(DatabaseConnectionFactory.DB_URL, getUrl());
    config.put(ColumnManager.DB_PAGINATION, "none");
    setupConfig.initConfig(config);
    InMemoryDBConnectionFactory factory = new InMemoryDBConnectionFactory();
    when(helperMock.getConnectionFactory()).thenReturn(factory);
    DatabaseRepository dbRepository = new DatabaseRepository(helperMock);
    dbRepository.init(repositoryContextMock);

    Connection conn = factory.createConnection();
    try {
      try (Statement stmt = conn.createStatement()) {
        buildTable(stmt);
      }

      List<String> expectedIds = ImmutableList.of(
          "id1/Joe Smith", "id2/Mary Jones", "id3/Mike Brown", "id4/Sue Green", "id5/Betty Black");

      byte[] checkpoint = new FullCheckpoint().setPagination(Pagination.OFFSET)
          .toString()
          .getBytes(UTF_8);

      try (CheckpointCloseableIterable<ApiOperation> allDocs = dbRepository.getAllDocs(null)) {
        assertEquals(expectedIds,
            ImmutableList.copyOf(
                Iterables.transform(allDocs, v -> ((RepositoryDoc) v).getItem().getName())));
        assertNull(allDocs.getCheckpoint());
        assertFalse(allDocs.hasMore());
      }
    } finally {
      factory.releaseConnection(conn);
      dbRepository.close();
      factory.shutdown();
    }
  }

  @Test
  public void testGetAllDocs_paginationOffset_checkpointOffset() throws Exception {
    Properties config = new Properties();
    setAllMandatory(config);
    config.put(DatabaseConnectionFactory.DB_URL, getUrl());
    config.put(ColumnManager.DB_ALL_RECORDS_SQL,
        config.get(ColumnManager.DB_ALL_RECORDS_SQL) + " limit 4 offset ?");
    config.put(ColumnManager.DB_PAGINATION, "offset");
    setupConfig.initConfig(config);
    InMemoryDBConnectionFactory factory = new InMemoryDBConnectionFactory();
    when(helperMock.getConnectionFactory()).thenReturn(factory);
    DatabaseRepository dbRepository = new DatabaseRepository(helperMock);
    dbRepository.init(repositoryContextMock);

    Connection conn = factory.createConnection();
    try {
      try (Statement stmt = conn.createStatement()) {
        buildTable(stmt);
      }

      byte[] checkpoint = new FullCheckpoint().setPagination(Pagination.OFFSET).setOffset(9)
          .toString()
          .getBytes(UTF_8);
      try (CheckpointCloseableIterable<ApiOperation> allDocs =
          dbRepository.getAllDocs(checkpoint)) {
        assertEquals(ImmutableList.of(), ImmutableList.copyOf(allDocs));
        assertNull(allDocs.getCheckpoint());
        assertFalse(allDocs.hasMore());
      }
    } finally {
      factory.releaseConnection(conn);
      dbRepository.close();
      factory.shutdown();
    }
  }

  @Test
  public void testGetAllDocs_paginationOffset_fullTraversals() throws Exception {
    Properties config = new Properties();
    setAllMandatory(config);
    config.put(DatabaseConnectionFactory.DB_URL, getUrl());
    config.put(ColumnManager.DB_ALL_RECORDS_SQL,
        config.get(ColumnManager.DB_ALL_RECORDS_SQL) + " limit 1 offset ?");
    config.put(ColumnManager.DB_PAGINATION, "offset");
    setupConfig.initConfig(config);
    InMemoryDBConnectionFactory factory = new InMemoryDBConnectionFactory();
    when(helperMock.getConnectionFactory()).thenReturn(factory);
    DatabaseRepository dbRepository = new DatabaseRepository(helperMock);
    dbRepository.init(repositoryContextMock);

    Connection conn = factory.createConnection();
    try {
      try (Statement stmt = conn.createStatement()) {
        buildTable(stmt);
      }

      List<String> expectedIds = ImmutableList.of(
          "id1/Joe Smith", "id2/Mary Jones", "id3/Mike Brown", "id4/Sue Green", "id5/Betty Black");

      // Run two full traversals, to verify that the checkpoint is reset to null each time.
      byte[] checkpoint = null;
      for (int i = 0; i < 2; i++) {
        List<String> actualIds = new ArrayList<>();
        boolean hasMore = false;
        do {
          try (CheckpointCloseableIterable<ApiOperation> allDocs =
              dbRepository.getAllDocs(checkpoint)) {
            Iterator<ApiOperation> it = allDocs.iterator();
            if (actualIds.size() < expectedIds.size()) {
              assertTrue("Only found " + actualIds.toString(), it.hasNext());
              actualIds.add(((RepositoryDoc) it.next()).getItem().getName());
              assertFalse(it.hasNext());
              hasMore = allDocs.hasMore();
              assertTrue(hasMore);
            } else {
              assertFalse(it.hasNext());
              hasMore = allDocs.hasMore();
              assertFalse(hasMore);
            }
            checkpoint = allDocs.getCheckpoint();
          }
        } while (hasMore);

        assertEquals(expectedIds, actualIds);
        assertNull(checkpoint);
      }
    } finally {
      factory.releaseConnection(conn);
      dbRepository.close();
      factory.shutdown();
    }
  }

  @Test
  public void getAllDocs_spaceInTableName_succeeds() throws Exception {
    Properties config = new Properties();
    config.put(DatabaseConnectionFactory.DB_URL, getUrl());
    config.put(ColumnManager.DB_UNIQUE_KEY_COLUMNS, "id");
    config.put(ColumnManager.DB_ALL_COLUMNS, "id");
    config.put(ColumnManager.DB_ALL_RECORDS_SQL, "select id from \"test table\"");
    config.put(UrlBuilder.CONFIG_COLUMNS, "id");
    config.put(CONFIG_TITLE_DB_FORMAT, "id");
    config.put(DefaultAcl.DEFAULT_ACL_MODE, DefaultAclMode.FALLBACK.toString());
    setupConfig.initConfig(config);
    InMemoryDBConnectionFactory factory = new InMemoryDBConnectionFactory();
    when(helperMock.getConnectionFactory()).thenReturn(factory);
    mockContent();
    DatabaseRepository dbRepository = new DatabaseRepository(helperMock);
    dbRepository.init(repositoryContextMock);

    Connection conn = factory.createConnection();
    try {
      // build the db
      try (Statement stmt = conn.createStatement()) {
        stmt.execute("create table \"test table\" (id varchar(32) unique not null)");
        stmt.execute("insert into \"test table\" (id) values ('id1')");
      }
      // query the db
      try (CheckpointCloseableIterable<ApiOperation> allDocs =
          dbRepository.getAllDocs(NULL_TRAVERSAL_CHECKPOINT)) {
        for (ApiOperation op : allDocs) {
          RepositoryDoc record = (RepositoryDoc) op;
          Item item = record.getItem();
          assertEquals("id1", item.getName());
        }
      }
    } finally {
      factory.releaseConnection(conn);
      dbRepository.close();
      factory.shutdown();
    }
  }

  @Test
  public void getAllDocs_quotesInTableName_succeeds() throws Exception {
    Properties config = new Properties();
    config.put(DatabaseConnectionFactory.DB_URL, getUrl());
    config.put(ColumnManager.DB_UNIQUE_KEY_COLUMNS, "id");
    config.put(ColumnManager.DB_ALL_COLUMNS, "id");
    config.put(ColumnManager.DB_ALL_RECORDS_SQL, "select id from \"test \"\"table\"\"\"");
    config.put(UrlBuilder.CONFIG_COLUMNS, "id");
    config.put(CONFIG_TITLE_DB_FORMAT, "id");
    config.put(DefaultAcl.DEFAULT_ACL_MODE, DefaultAclMode.FALLBACK.toString());
    setupConfig.initConfig(config);
    InMemoryDBConnectionFactory factory = new InMemoryDBConnectionFactory();
    when(helperMock.getConnectionFactory()).thenReturn(factory);
    mockContent();
    DatabaseRepository dbRepository = new DatabaseRepository(helperMock);
    dbRepository.init(repositoryContextMock);

    Connection conn = factory.createConnection();
    try {
      // build the db
      try (Statement stmt = conn.createStatement()) {
        String createString =
            "create table \"test \"\"table\"\"\" (id varchar(32) unique not null)";
        String insertString = "insert into \"test \"\"table\"\"\" (id) values ('id1')";
        stmt.execute(createString);
        stmt.execute(insertString);
      }
      // query the db
      try (CheckpointCloseableIterable<ApiOperation> allDocs =
          dbRepository.getAllDocs(NULL_TRAVERSAL_CHECKPOINT)) {
        for (ApiOperation op : allDocs) {
          RepositoryDoc record = (RepositoryDoc) op;
          Item item = record.getItem();
          assertEquals("id1", item.getName());
        }
      }
    } finally {
      factory.releaseConnection(conn);
      dbRepository.close();
      factory.shutdown();
    }
  }

  @Test
  public void getAllDocs_euroInTableName_succeeds() throws Exception {
    Properties config = new Properties();
    config.put(DatabaseConnectionFactory.DB_URL, getUrl());
    config.put(ColumnManager.DB_UNIQUE_KEY_COLUMNS, "id");
    config.put(ColumnManager.DB_ALL_COLUMNS, "id");
    config.put(ColumnManager.DB_ALL_RECORDS_SQL, "select id from testtabl\u20ac");
    config.put(UrlBuilder.CONFIG_COLUMNS, "id");
    config.put(CONFIG_TITLE_DB_FORMAT, "id");
    config.put(DefaultAcl.DEFAULT_ACL_MODE, DefaultAclMode.FALLBACK.toString());
    setupConfig.initConfig(config);
    InMemoryDBConnectionFactory factory = new InMemoryDBConnectionFactory();
    when(helperMock.getConnectionFactory()).thenReturn(factory);
    mockContent();
    DatabaseRepository dbRepository = new DatabaseRepository(helperMock);
    dbRepository.init(repositoryContextMock);

    Connection conn = factory.createConnection();
    try {
      // build the db
      try (Statement stmt = conn.createStatement()) {
        String createString = "create table testtabl\u20ac (id varchar(32) unique not null)";
        String insertString = "insert into testtabl\u20ac (id) values ('id1')";
        stmt.execute(createString);
        stmt.execute(insertString);
      }
      // query the db
      try (CheckpointCloseableIterable<ApiOperation> allDocs =
          dbRepository.getAllDocs(NULL_TRAVERSAL_CHECKPOINT)) {
        for (ApiOperation op : allDocs) {
          RepositoryDoc record = (RepositoryDoc) op;
          Item item = record.getItem();
          assertEquals("id1", item.getName());
        }
      }
    } finally {
      factory.releaseConnection(conn);
      dbRepository.close();
      factory.shutdown();
    }
  }

  private String getContentString(RepositoryDoc doc) throws IOException {
    return CharStreams.toString(
        new InputStreamReader(doc.getContent().getInputStream(), Charsets.UTF_8));
  }

  private String getExpectedContentString(String columnName, String columnValue) {
    String output =
        "<!DOCTYPE html>\n"
        + "<html lang='en'>\n"
        + "<head>\n"
        + "<meta http-equiv='Content-Type' content='text/html; charset=utf-8'/>\n"
        + "<title>" + columnValue + "</title>\n"
        + "</head>\n"
        + "<body>\n"
        + "<div id='" + columnName + "'>\n"
        + "  <p>" + columnName + ":</p>\n"
        + "  <h1>" + columnValue + "</h1>\n"
        + "</div>\n"
        + "</body>\n"
        + "</html>\n";
    return output;
  }

  @Test
  public void getAllDocs_spaceInColumnName_succeeds() throws Exception {
    Properties config = new Properties();
    config.put(DatabaseConnectionFactory.DB_URL, getUrl());
    config.put(ColumnManager.DB_UNIQUE_KEY_COLUMNS, "id with space");
    config.put(ColumnManager.DB_ALL_COLUMNS, "id with space");
    config.put(ColumnManager.DB_ALL_RECORDS_SQL, "select \"id with space\" from testtable");
    config.put(UrlBuilder.CONFIG_COLUMNS, "id with space");
    config.put(CONFIG_TITLE_DB_FORMAT, "id with space");
    config.put(DefaultAcl.DEFAULT_ACL_MODE, DefaultAclMode.FALLBACK.toString());
    setupConfig.initConfig(config);
    InMemoryDBConnectionFactory factory = new InMemoryDBConnectionFactory();
    when(helperMock.getConnectionFactory()).thenReturn(factory);
    mockContent();
    DatabaseRepository dbRepository = new DatabaseRepository(helperMock);
    dbRepository.init(repositoryContextMock);

    Connection conn = factory.createConnection();
    try {
      // build the db
      try (Statement stmt = conn.createStatement()) {
        stmt.execute("create table testtable (\"id with space\" varchar(32) unique not null)");
        stmt.execute("insert into testtable (\"id with space\") values ('id1')");
      }
      // query the db
      try (CheckpointCloseableIterable<ApiOperation> allDocs =
          dbRepository.getAllDocs(NULL_TRAVERSAL_CHECKPOINT)) {
        for (ApiOperation op : allDocs) {
          RepositoryDoc record = (RepositoryDoc) op;
          Item item = record.getItem();
          assertEquals("id1", item.getName());
          // The column name is used as an id in the generated html, though id values
          // aren't technically allowed to contain spaces.
          assertEquals(getExpectedContentString("id with space", "id1"), getContentString(record));
        }
      }
    } finally {
      factory.releaseConnection(conn);
      dbRepository.close();
      factory.shutdown();
    }
  }

  @Test
  public void getAllDocs_spaceInColumnNameWithAlias_succeeds() throws Exception {
    Properties config = new Properties();
    config.put(DatabaseConnectionFactory.DB_URL, getUrl());
    config.put(ColumnManager.DB_UNIQUE_KEY_COLUMNS, "idalias");
    config.put(ColumnManager.DB_ALL_COLUMNS, "idalias");
    config.put(ColumnManager.DB_ALL_RECORDS_SQL,
        "select \"id with space\" as idalias from testtable");
    config.put(UrlBuilder.CONFIG_COLUMNS, "idalias");
    config.put(CONFIG_TITLE_DB_FORMAT, "idalias");
    config.put(DefaultAcl.DEFAULT_ACL_MODE, DefaultAclMode.FALLBACK.toString());
    setupConfig.initConfig(config);
    InMemoryDBConnectionFactory factory = new InMemoryDBConnectionFactory();
    when(helperMock.getConnectionFactory()).thenReturn(factory);
    mockContent();
    DatabaseRepository dbRepository = new DatabaseRepository(helperMock);
    dbRepository.init(repositoryContextMock);

    Connection conn = factory.createConnection();
    try {
      // build the db
      try (Statement stmt = conn.createStatement()) {
        stmt.execute("create table testtable (\"id with space\" varchar(32) unique not null)");
        stmt.execute("insert into testtable (\"id with space\") values ('id1')");
      }
      // query the db
      try (CheckpointCloseableIterable<ApiOperation> allDocs =
          dbRepository.getAllDocs(NULL_TRAVERSAL_CHECKPOINT)) {
        for (ApiOperation op : allDocs) {
          RepositoryDoc record = (RepositoryDoc) op;
          Item item = record.getItem();
          assertEquals("id1", item.getName());
          assertEquals(getExpectedContentString("idalias", "id1"), getContentString(record));
        }
      }
    } finally {
      factory.releaseConnection(conn);
      dbRepository.close();
      factory.shutdown();
    }
  }

  @Test
  public void getAllDocs_spaceInAlias_succeeds() throws Exception {
    Properties config = new Properties();
    config.put(DatabaseConnectionFactory.DB_URL, getUrl());
    config.put(ColumnManager.DB_UNIQUE_KEY_COLUMNS, "id alias");
    config.put(ColumnManager.DB_ALL_COLUMNS, "id alias");
    config.put(ColumnManager.DB_ALL_RECORDS_SQL,
        "select id  as \"id alias\" from testtable");
    config.put(UrlBuilder.CONFIG_COLUMNS, "id alias");
    config.put(CONFIG_TITLE_DB_FORMAT, "id alias");
    config.put(DefaultAcl.DEFAULT_ACL_MODE, DefaultAclMode.FALLBACK.toString());
    setupConfig.initConfig(config);
    InMemoryDBConnectionFactory factory = new InMemoryDBConnectionFactory();
    when(helperMock.getConnectionFactory()).thenReturn(factory);
    mockContent();
    DatabaseRepository dbRepository = new DatabaseRepository(helperMock);
    dbRepository.init(repositoryContextMock);

    Connection conn = factory.createConnection();
    try {
      // build the db
      try (Statement stmt = conn.createStatement()) {
        stmt.execute("create table testtable (id varchar(32) unique not null)");
        stmt.execute("insert into testtable (id) values ('id1')");
      }
      // query the db
      try (CheckpointCloseableIterable<ApiOperation> allDocs =
          dbRepository.getAllDocs(NULL_TRAVERSAL_CHECKPOINT)) {
        for (ApiOperation op : allDocs) {
          RepositoryDoc record = (RepositoryDoc) op;
          Item item = record.getItem();
          assertEquals("id1", item.getName());
          assertEquals(getExpectedContentString("id alias", "id1"), getContentString(record));
        }
      }
    } finally {
      factory.releaseConnection(conn);
      dbRepository.close();
      factory.shutdown();
    }
  }

  @Test
  public void getAllDocs_quotesInColumnName_failsValidation() throws Exception {
    Properties config = new Properties();
    config.put(DatabaseConnectionFactory.DB_URL, getUrl());
    config.put(ColumnManager.DB_UNIQUE_KEY_COLUMNS, "id with \"quotes\"");
    config.put(ColumnManager.DB_ALL_COLUMNS, "id with \"quotes\"");
    config.put(ColumnManager.DB_ALL_RECORDS_SQL,
        "select \"id with \"\"quotes\"\"\" from testtable");
    config.put(UrlBuilder.CONFIG_COLUMNS, "id with \"quotes\"");
    config.put(CONFIG_TITLE_DB_FORMAT, "id with \"quotes\"");
    config.put(DefaultAcl.DEFAULT_ACL_MODE, DefaultAclMode.FALLBACK.toString());
    setupConfig.initConfig(config);
    InMemoryDBConnectionFactory factory = new InMemoryDBConnectionFactory();
    when(helperMock.getConnectionFactory()).thenReturn(factory);
    mockContent();
    DatabaseRepository dbRepository = new DatabaseRepository(helperMock);
    // Fails because the validation checks for 'id with "quotes"' in the select statement
    // but the statement contains 'id with ""quotes""'.
    thrown.expect(InvalidConfigurationException.class);
    thrown.expectMessage("Missing column names in main SQL query: [id with \"quotes\"]");
    dbRepository.init(repositoryContextMock);

    // TODO(gemerson): Leaving this here for now in case we decide to change the validation
    Connection conn = factory.createConnection();
    try {
      // build the db
      try (Statement stmt = conn.createStatement()) {
        stmt.execute(
            "create table testtable (\"id with \"\"quotes\"\"\" varchar(32) unique not null)");
        stmt.execute("insert into testtable (\"id with \"\"quotes\"\"\") values ('id1')");
      }
      // query the db
      try (CheckpointCloseableIterable<ApiOperation> allDocs =
          dbRepository.getAllDocs(NULL_TRAVERSAL_CHECKPOINT)) {
        for (ApiOperation op : allDocs) {
          RepositoryDoc record = (RepositoryDoc) op;
          Item item = record.getItem();
          assertEquals("id1", item.getName());
          assertEquals(getExpectedContentString("id with &quot;quotes&quot;", "id1"),
              getContentString(record));
        }
      }
    } finally {
      factory.releaseConnection(conn);
      dbRepository.close();
      factory.shutdown();
    }
  }

  @Test
  public void getAllDocs_euroInColumnName_succeeds() throws Exception {
    Properties config = new Properties();
    config.put(DatabaseConnectionFactory.DB_URL, getUrl());
    config.put(ColumnManager.DB_UNIQUE_KEY_COLUMNS, "idWith\u20acuro");
    config.put(ColumnManager.DB_ALL_COLUMNS, "idWith\u20acuro");
    config.put(ColumnManager.DB_ALL_RECORDS_SQL, "select \"idWith\u20acuro\" from testtable");
    config.put(UrlBuilder.CONFIG_COLUMNS, "idWith\u20acuro");
    config.put(CONFIG_TITLE_DB_FORMAT, "idWith\u20acuro");
    config.put(DefaultAcl.DEFAULT_ACL_MODE, DefaultAclMode.FALLBACK.toString());
    setupConfig.initConfig(config);
    InMemoryDBConnectionFactory factory = new InMemoryDBConnectionFactory();
    when(helperMock.getConnectionFactory()).thenReturn(factory);
    mockContent();
    DatabaseRepository dbRepository = new DatabaseRepository(helperMock);
    dbRepository.init(repositoryContextMock);

    Connection conn = factory.createConnection();
    try {
      // build the db
      try (Statement stmt = conn.createStatement()) {
        stmt.execute("create table testtable (\"idWith\u20acuro\" varchar(32) unique not null)");
        stmt.execute("insert into testtable (\"idWith\u20acuro\") values ('id1')");
      }
      // query the db
      try (CheckpointCloseableIterable<ApiOperation> allDocs =
          dbRepository.getAllDocs(NULL_TRAVERSAL_CHECKPOINT)) {
        for (ApiOperation op : allDocs) {
          RepositoryDoc record = (RepositoryDoc) op;
          Item item = record.getItem();
          assertEquals("id1", item.getName());
          assertEquals(getExpectedContentString("idWith\u20acuro", "id1"),
              getContentString(record));
        }
      }
    } finally {
      factory.releaseConnection(conn);
      dbRepository.close();
      factory.shutdown();
    }
  }

  @Test
  @Parameters({
    "data-with-dash",
    "data with spaces",
    "data with \u20acuro",
    "data with MixedCaseChars"
  })
  public void getAllDocs_charsInMetadataTitle_succeeds(String dataForTitle) throws Exception {
    Properties config = new Properties();
    config.put(DatabaseConnectionFactory.DB_URL, getUrl());
    config.put(ColumnManager.DB_UNIQUE_KEY_COLUMNS, "id");
    config.put(ColumnManager.DB_ALL_COLUMNS, "id, data");
    config.put(ColumnManager.DB_ALL_RECORDS_SQL, "select id, data from testtable");
    config.put(IndexingItemBuilder.TITLE_FIELD, "data");
    config.put(UrlBuilder.CONFIG_COLUMNS, "id");
    config.put(CONFIG_TITLE_DB_FORMAT, "id");
    config.put(DefaultAcl.DEFAULT_ACL_MODE, DefaultAclMode.FALLBACK.toString());
    setupConfig.initConfig(config);
    InMemoryDBConnectionFactory factory = new InMemoryDBConnectionFactory();
    when(helperMock.getConnectionFactory()).thenReturn(factory);
    mockContent();
    DatabaseRepository dbRepository = new DatabaseRepository(helperMock);
    dbRepository.init(repositoryContextMock);

    Connection conn = factory.createConnection();
    try {
      // build the db
      try (Statement stmt = conn.createStatement()) {
        stmt.execute("create table testtable (id varchar(32) unique not null, data varchar(128))");
        stmt.execute("insert into testtable (id, data) values ('id1', '" + dataForTitle + "')");
      }
      // query the db
      try (CheckpointCloseableIterable<ApiOperation> allDocs =
          dbRepository.getAllDocs(NULL_TRAVERSAL_CHECKPOINT)) {
        for (ApiOperation op : allDocs) {
          RepositoryDoc record = (RepositoryDoc) op;
          Item item = record.getItem();
          assertEquals(dataForTitle, item.getMetadata().getTitle());
        }
      }
    } finally {
      factory.releaseConnection(conn);
      dbRepository.close();
      factory.shutdown();
    }
  }

  private Object uniqueKeyColumnParams() {
    return new Object[]{
      new Object[] { "id", "id1"},
      new Object[] { "id,textdata", "id1/textcontent"},
      new Object[] { "id,numericdata", "id1/11"},
      new Object[] { "textdata,numericdata", "textcontent/11"},
      new Object[] { "numericdata, id, textdata", "11/id1/textcontent"}
    };
  }

  @Test
  @Parameters(method = "uniqueKeyColumnParams")
  public void getAllDocs_uniqueKeyColumn_succeeds(String keyColumns, String key) throws Exception {
    Properties config = new Properties();
    config.put(DatabaseConnectionFactory.DB_URL, getUrl());
    config.put(ColumnManager.DB_UNIQUE_KEY_COLUMNS, keyColumns);
    config.put(ColumnManager.DB_ALL_COLUMNS, "id, textdata, numericdata");
    config.put(ColumnManager.DB_ALL_RECORDS_SQL, "select id, textdata, numericdata from testtable");
    config.put(IndexingItemBuilder.TITLE_FIELD, "textdata");
    config.put(UrlBuilder.CONFIG_COLUMNS, "id");
    config.put(CONFIG_TITLE_DB_FORMAT, "id");
    config.put(DefaultAcl.DEFAULT_ACL_MODE, DefaultAclMode.FALLBACK.toString());
    setupConfig.initConfig(config);
    InMemoryDBConnectionFactory factory = new InMemoryDBConnectionFactory();
    when(helperMock.getConnectionFactory()).thenReturn(factory);
    mockContent();
    DatabaseRepository dbRepository = new DatabaseRepository(helperMock);
    dbRepository.init(repositoryContextMock);

    Connection conn = factory.createConnection();
    try {
      // build the db
      try (Statement stmt = conn.createStatement()) {
        stmt.execute("create table testtable ("
            + "id varchar(32) unique not null, "
            + "textdata varchar(128), "
            + "numericdata int)");
        stmt.execute(
            "insert into testtable (id, textdata, numericdata) values ('id1', 'textcontent', 11)");
      }
      // query the db
      try (CheckpointCloseableIterable<ApiOperation> allDocs =
          dbRepository.getAllDocs(NULL_TRAVERSAL_CHECKPOINT)) {
        for (ApiOperation op : allDocs) {
          RepositoryDoc record = (RepositoryDoc) op;
          Item item = record.getItem();
          assertEquals(key, item.getName());
        }
      }
    } finally {
      factory.releaseConnection(conn);
      dbRepository.close();
      factory.shutdown();
    }
  }

  @Test
  @Parameters({
    "int, 123",
    "double, 12.3", // float, double data type
    "bigint, 9223372036854775802", // long data type
    "boolean, false",
    "char(3), abc",
    "varchar(16), stringid", // string data type
    "date, 2015-12-15",
    "time, 23:59:59",
    "timestamp, 2015-12-15 23:15:15.0"
  })
  // http://www.h2database.com/html/datatypes.html
  public void uniqueKeyColumn_DataTypes_succeed(String columnType, String value)
      throws Exception {
    Properties config = new Properties();
    config.put(DatabaseConnectionFactory.DB_URL, getUrl());
    config.put(ColumnManager.DB_UNIQUE_KEY_COLUMNS, "id");
    config.put(ColumnManager.DB_ALL_COLUMNS, "id, data");
    config.put(ColumnManager.DB_ALL_RECORDS_SQL, "select id, data from testtable");
    config.put(IndexingItemBuilder.TITLE_FIELD, "data");
    config.put(UrlBuilder.CONFIG_COLUMNS, "id");
    config.put(CONFIG_TITLE_DB_FORMAT, "id");
    config.put(DefaultAcl.DEFAULT_ACL_MODE, DefaultAclMode.FALLBACK.toString());
    setupConfig.initConfig(config);
    InMemoryDBConnectionFactory factory = new InMemoryDBConnectionFactory();
    when(helperMock.getConnectionFactory()).thenReturn(factory);
    mockContent();
    DatabaseRepository dbRepository = new DatabaseRepository(helperMock);
    dbRepository.init(repositoryContextMock);

    Connection conn = factory.createConnection();
    try {
      try (Statement stmt = conn.createStatement()) {
        String createSQL = String.format("create table testtable (id %s unique not null, "
            + "data varchar(128))", columnType);
        String insertSQL = String.format("insert into testtable (id, data) "
            + "values('%s', 'testdata')", value);
        stmt.execute(createSQL);
        stmt.execute(insertSQL);
      }

      try (CheckpointCloseableIterable<ApiOperation> allDocs =
          dbRepository.getAllDocs(NULL_TRAVERSAL_CHECKPOINT)) {
        for (ApiOperation op : allDocs) {
          RepositoryDoc record = (RepositoryDoc) op;
          Item item = record.getItem();
          assertEquals(value, item.getName());
        }
      }
    } finally {
      factory.releaseConnection(conn);
      dbRepository.close();
      factory.shutdown();
    }
  }

  @Test
  public void getAllDocs_nullValueInUrlColumn_throwsIllegalArgumentException() throws Exception {
    Properties config = new Properties();
    config.put(DatabaseConnectionFactory.DB_URL, getUrl());
    config.put(ColumnManager.DB_UNIQUE_KEY_COLUMNS, "id");
    config.put(ColumnManager.DB_ALL_COLUMNS, "id, data");
    config.put(ColumnManager.DB_ALL_RECORDS_SQL, "select id, data from testtable");
    config.put(IndexingItemBuilder.TITLE_FIELD, "data");
    config.put(UrlBuilder.CONFIG_COLUMNS, "data");
    config.put(CONFIG_TITLE_DB_FORMAT, "id");
    config.put(DefaultAcl.DEFAULT_ACL_MODE, DefaultAclMode.FALLBACK.toString());
    setupConfig.initConfig(config);
    InMemoryDBConnectionFactory factory = new InMemoryDBConnectionFactory();
    when(helperMock.getConnectionFactory()).thenReturn(factory);
    mockContent();
    DatabaseRepository dbRepository = new DatabaseRepository(helperMock);
    dbRepository.init(repositoryContextMock);

    Connection conn = factory.createConnection();
    try {
      // build the db
      try (Statement stmt = conn.createStatement()) {
        stmt.execute("create table testtable (id varchar(32) unique not null, data varchar(128))");
        stmt.execute("insert into testtable (id, data) values ('id1', 'text')");
        stmt.execute("insert into testtable (id) values ('id2')");
        stmt.execute("insert into testtable (id, data) values ('id3', 'text')");
      }

      // The exception is thrown when the iterator calls next and the repository class
      // tries to build the url for the row with a null value.
      try (CheckpointCloseableIterable<ApiOperation> allDocs =
          dbRepository.getAllDocs(NULL_TRAVERSAL_CHECKPOINT)) {
        thrown.expect(IllegalArgumentException.class);
        for (ApiOperation op : allDocs) {
        }
      }
    } finally {
      factory.releaseConnection(conn);
      dbRepository.close();
      factory.shutdown();
    }
  }

  @Test
  public void testGetChangesNoInitialCheckpoint() throws Exception {
    setupConfiguration(getUrl(), "");
    InMemoryDBConnectionFactory factory = new InMemoryDBConnectionFactory();
    when(helperMock.getConnectionFactory()).thenReturn(factory);
    Timestamp startTime = Timestamp.valueOf("2017-01-12 14:50:00.0");
    when(helperMock.getCurrentTime()).thenReturn(startTime.getTime());
    DatabaseRepository dbRepository = new DatabaseRepository(helperMock);
    dbRepository.init(repositoryContextMock);

    // create target for verification
    CheckpointCloseableIterable<ApiOperation> targetIncrementalChanges =
        getTargetIncrementalChanges(LATEST_CHECKPOINT_TIMESTAMP);
    Connection conn = factory.createConnection();
    try {
      // build the db
      try (Statement stmt = conn.createStatement()) {
        buildTable(stmt);
      }

      // query the db
      try (CheckpointCloseableIterable<ApiOperation> incrementalChanges =
          dbRepository.getChanges(null)) {
        assertNotNull(incrementalChanges);

        // verify items
        assertIterableEquals(targetIncrementalChanges, incrementalChanges);
      }
    } finally {
      factory.releaseConnection(conn);
      dbRepository.close();
      factory.shutdown();
    }
  }

  @Test
  public void testGetChanges() throws Exception {
    setupConfiguration(getUrl(), "");
    InMemoryDBConnectionFactory factory = new InMemoryDBConnectionFactory();
    when(helperMock.getConnectionFactory()).thenReturn(factory);
    Timestamp startTime = Timestamp.valueOf("2017-01-12 14:50:00.0");
    when(helperMock.getCurrentTime()).thenReturn(startTime.getTime());
    IncrementalCheckpoint checkpointStart = new IncrementalCheckpoint(startTime.getTime());
    DatabaseRepository dbRepository = new DatabaseRepository(helperMock);
    dbRepository.init(repositoryContextMock);

    // create target for verification
    CheckpointCloseableIterable<ApiOperation> targetIncrementalChanges =
        getTargetIncrementalChanges(LATEST_CHECKPOINT_TIMESTAMP);
    Connection conn = factory.createConnection();
    try {
      // build the db
      try (Statement stmt = conn.createStatement()) {
        buildTable(stmt);
      }

      // query the db
      try (CheckpointCloseableIterable<ApiOperation> incrementalChanges =
          dbRepository.getChanges(checkpointStart.get())) {
        assertNotNull(incrementalChanges);

        // verify items
        assertIterableEquals(targetIncrementalChanges, incrementalChanges);
      }
    } finally {
      factory.releaseConnection(conn);
      dbRepository.close();
      factory.shutdown();
    }
  }

  @Test
  public void testGetChangesWithTimezoneNoTimestampColumn() throws Exception {
    setupConfiguration(getUrl(), TIMEZONE_TEST_LOCALE, !USE_TIMESTAMP_COLUMN); // (e.g. +3 hrs)
    InMemoryDBConnectionFactory factory = new InMemoryDBConnectionFactory();
    when(helperMock.getConnectionFactory()).thenReturn(factory);
    // set the time for the start in remote zone (e.g. NY), but converted to local time
    ZonedDateTime localStartTime = ZonedDateTime
        .of(2017, 1, 12, 14, 50, 0, 0, ZoneId.of(TIMEZONE_TEST_LOCALE));
    Timestamp startTime = Timestamp.from(localStartTime.toInstant());
    // the traversal start time is only used as a return value, so timezones are irrelevant
    final String traversalStartTime = "2017-01-14 10:00:00.0";
    Timestamp traversalTime = Timestamp.valueOf(traversalStartTime);
    when(helperMock.getCurrentTime()).thenReturn(startTime.getTime(), traversalTime.getTime());
    DatabaseRepository dbRepository = new DatabaseRepository(helperMock);
    dbRepository.init(repositoryContextMock);

    // create target for verification
    CheckpointCloseableIterable<ApiOperation> targetIncrementalChanges =
        getTargetIncrementalChanges(traversalStartTime);
    Connection conn = factory.createConnection();
    try {
      // build the db
      try (Statement stmt = conn.createStatement()) {
        buildTable(stmt);
      }

      // query the db
      try (CheckpointCloseableIterable<ApiOperation> incrementalChanges =
          dbRepository.getChanges(null)) {
        assertNotNull(incrementalChanges);

        // verify items
        assertIterableEquals(targetIncrementalChanges, incrementalChanges);
      }
    } finally {
      factory.releaseConnection(conn);
      dbRepository.close();
      factory.shutdown();
    }
  }

  @Test
  public void testGetChangesWithTimezoneCheckpointNoTimestampColumn()
      throws Exception {
    setupConfiguration(getUrl(), TIMEZONE_TEST_LOCALE, !USE_TIMESTAMP_COLUMN); // e.g. +3 hrs
    InMemoryDBConnectionFactory factory = new InMemoryDBConnectionFactory();
    when(helperMock.getConnectionFactory()).thenReturn(factory);
    // this start time will not actually be used since there will be a checkpoint
    Timestamp startTime = Timestamp.valueOf("2017-01-12 14:50:00.0");
    final String traversalStartTime = "2017-01-14 10:00:00.0";
    Timestamp traversalTime = Timestamp.valueOf(traversalStartTime);
    when(helperMock.getCurrentTime()).thenReturn(startTime.getTime(), traversalTime.getTime());
    DatabaseRepository dbRepository = new DatabaseRepository(helperMock);
    dbRepository.init(repositoryContextMock);
    // set the time for the start in remote zone (e.g. NY), but converted to local time
    ZonedDateTime localStartTime = ZonedDateTime
        .of(2017, 1, 12, 14, 50, 0, 0, ZoneId.of(TIMEZONE_TEST_LOCALE));
    Timestamp localTime = Timestamp.from(localStartTime.toInstant());
    IncrementalCheckpoint checkpointLocal = new IncrementalCheckpoint(localTime.getTime());

    // create target for verification
    CheckpointCloseableIterable<ApiOperation> targetIncrementalChanges =
        getTargetIncrementalChanges(traversalStartTime);
    Connection conn = factory.createConnection();
    try {
      // build the db
      try (Statement stmt = conn.createStatement()) {
        buildTable(stmt);
      }

      // query the db
      try (CheckpointCloseableIterable<ApiOperation> incrementalChanges =
          dbRepository.getChanges(checkpointLocal.get())) {
        assertNotNull(incrementalChanges);

        // verify items
        assertIterableEquals(targetIncrementalChanges, incrementalChanges);
      }
    } finally {
      factory.releaseConnection(conn);
      dbRepository.close();
      factory.shutdown();
    }
  }

  @Test
  public void testGetChangesWithTimezone1() throws Exception {
    // use local timezone (wherever this may be run)
    changesWithTimezone(getUrl());
  }

  @Test
  public void testGetChangesWithTimezone2() throws Exception {
    // store local default timezone to restore this thread after change
    ZoneId defaultZone = TimeZone.getDefault().toZoneId();
    // use first test timezone
    TimeZone.setDefault(TimeZone.getTimeZone(ZoneId.of(TIMEZONE_TEST_LOCALE)));
    try {
      changesWithTimezone(getUrl());
    } finally {
      // restore default
      TimeZone.setDefault(TimeZone.getTimeZone(defaultZone));
    }
  }

  @Test
  public void testGetChangesWithTimezone3() throws Exception {
    // store local default timezone to restore this thread after change
    ZoneId defaultZone = TimeZone.getDefault().toZoneId();
    // use a different test timezone
    TimeZone.setDefault(TimeZone.getTimeZone(ZoneId.of(TIMEZONE_TEST_LOCALE_2)));
    try {
      changesWithTimezone(getUrl());
    } finally {
      // restore default
      TimeZone.setDefault(TimeZone.getTimeZone(defaultZone));
    }
  }

  private void changesWithTimezone(String url) throws Exception {
    // Note: since these tests can be run in parallel, it's best to use different db names
    setupConfiguration(url, TIMEZONE_TEST_LOCALE); // e.g. +3 hrs
    InMemoryDBConnectionFactory factory = new InMemoryDBConnectionFactory();
    when(helperMock.getConnectionFactory()).thenReturn(factory);
    // this start time will not actually be used since there will be a checkpoint
    Timestamp startTime = Timestamp.valueOf("2017-01-12 14:50:00.0");
    when(helperMock.getCurrentTime()).thenReturn(startTime.getTime());
    DatabaseRepository dbRepository = new DatabaseRepository(helperMock);
    dbRepository.init(repositoryContextMock);
    // set the time for the start in remote zone (e.g. NY), but converted to local time
    ZonedDateTime localStartTime = ZonedDateTime
        .of(2017, 1, 12, 14, 50, 0, 0, ZoneId.of(TIMEZONE_TEST_LOCALE));
    Timestamp localTime = Timestamp.from(localStartTime.toInstant());
    IncrementalCheckpoint checkpointLocal = new IncrementalCheckpoint(localTime.getTime());

    // start with the database latest timestamp, force it to be in the remote db timezone (e.g. NY),
    // then convert to local time for verification
    Timestamp latestCheckpoint = Timestamp.valueOf(LATEST_CHECKPOINT_TIMESTAMP);
    LocalDateTime localDateTime = LocalDateTime
        .ofInstant(latestCheckpoint.toInstant(), ZoneId.systemDefault());
    ZonedDateTime dbLocalZonedDateTime = ZonedDateTime
        .of(localDateTime, ZoneId.of(TIMEZONE_TEST_LOCALE));
    Timestamp latestLocalCheckpoint = Timestamp.from(dbLocalZonedDateTime.toInstant());
    // create target for verification
    CheckpointCloseableIterable<ApiOperation> targetIncrementalChanges =
        getTargetIncrementalChanges(latestLocalCheckpoint.toString()); // e.g. NY time - 3hrs
    Connection conn = factory.createConnection();
    try {
      // build the db
      try (Statement stmt = conn.createStatement()) {
        buildTable(stmt);
      }

      // query the db
      try (CheckpointCloseableIterable<ApiOperation> incrementalChanges =
          dbRepository.getChanges(checkpointLocal.get())) {
        assertNotNull(incrementalChanges);

        // verify items
        assertIterableEquals(targetIncrementalChanges, incrementalChanges);
      }
    } finally {
      factory.releaseConnection(conn);
      dbRepository.close();
      factory.shutdown();
    }
  }

  @Test
  public void testEmptyGetChanges() throws Exception {
    setupConfiguration(getUrl(), "");
    InMemoryDBConnectionFactory factory = new InMemoryDBConnectionFactory();
    when(helperMock.getConnectionFactory()).thenReturn(factory);
    Timestamp startTime = Timestamp.valueOf("2017-01-12 14:50:00.0");
    Timestamp sinceTime = Timestamp.valueOf("2017-01-13 14:50:00.0");
    when(helperMock.getCurrentTime()).thenReturn(startTime.getTime());
    DatabaseRepository dbRepository = new DatabaseRepository(helperMock);
    IncrementalCheckpoint checkpoint1 = new IncrementalCheckpoint(startTime.getTime());
    dbRepository.init(repositoryContextMock);

    // create target for verification
    CheckpointCloseableIterable<ApiOperation> targetIncrementalChanges =
        getTargetIncrementalChanges(LATEST_CHECKPOINT_TIMESTAMP);
    Connection conn = factory.createConnection();
    try {
      // build the db
      try (Statement stmt = conn.createStatement()) {
        buildTable(stmt);
      }

      // new query with updated checkpoint - two records should be returned
      byte[] checkpoint2;
      try (CheckpointCloseableIterable<ApiOperation> incrementalChanges =
          dbRepository.getChanges(checkpoint1.get())) {
        assertNotNull(incrementalChanges);

        // verify items
        assertIterableEquals(targetIncrementalChanges, incrementalChanges);
        checkpoint2 = incrementalChanges.getCheckpoint();
      }

      // query the db - no records should be returned
      try (CheckpointCloseableIterable<ApiOperation> incrementalChanges =
          dbRepository.getChanges(checkpoint2)) {
        assertNotNull(incrementalChanges);
        assertIterableEquals(
                new CheckpointCloseableIterableImpl.Builder<ApiOperation>(Collections.emptyList())
                .setCheckpoint(checkpoint2).build(),
                incrementalChanges);
      }
    } finally {
      factory.releaseConnection(conn);
      dbRepository.close();
      factory.shutdown();
    }
  }

  /**
   * common configuration builder(s)
   */
  private void setupConfiguration(String url, String timezone) {
    setupConfiguration(url, timezone, USE_TIMESTAMP_COLUMN); // default to use timestamp col
  }

  private void setupConfiguration(String url, String timezone, boolean timestampCol) {
    Properties config = new Properties();
    config.put(DatabaseConnectionFactory.DB_URL, url);
    config.put(ColumnManager.DB_UNIQUE_KEY_COLUMNS, "id");
    config.put(ColumnManager.DB_ALL_COLUMNS, "id, name, phone, lastmod_timestamp");
    config.put(ColumnManager.DB_CONTENT_COLUMNS, "name, phone");
    config.put(ColumnManager.DB_ALL_RECORDS_SQL,
        "select id, name, phone, lastmod_timestamp from testtable");
    // order allows for simpler verification
    config.put(ColumnManager.DB_INC_UPDATE_SQL,
        "select id, name, phone, lastmod_timestamp"
            + (timestampCol ? " as " + ColumnManager.TIMESTAMP_COLUMN : "")
            + " from testtable "
            + "where lastmod_timestamp > ? order by lastmod_timestamp desc");
    config.put(UrlBuilder.CONFIG_COLUMNS, "id");
    config.put(CONFIG_TITLE_DB_FORMAT, "name");
    config.put(DefaultAcl.DEFAULT_ACL_MODE, DefaultAclMode.FALLBACK.toString());
    if (!timezone.isEmpty()) {
      config.put(ColumnManager.DB_INC_UPDATE_TIMEZONE, timezone);
    }
    setupConfig.initConfig(config);
  }

  /** common target builder */
  private CheckpointCloseableIterable<ApiOperation> getTargetIncrementalChanges(
      String checkpointTime) {
    GoldenContent goldenContent = new GoldenContent();
    String targetContentId3 = "<!DOCTYPE html>\n<html lang='en'>\n<head>\n"
        + "<meta http-equiv='Content-Type' content='text/html; charset=utf-8'/>\n"
        + "<title>Mike Brown</title>\n</head>\n<body>\n"
        + "<div id='name'>\n  <p>name:</p>\n  <h1>Mike Brown</h1>\n</div>\n"
        + "<div id='phone'>\n  <p>phone:</p>\n  <p><small>3124</small></p>\n</div>\n"
        + "</body>\n</html>\n";
    String targetContentId5 = "<!DOCTYPE html>\n<html lang='en'>\n<head>\n"
        + "<meta http-equiv='Content-Type' content='text/html; charset=utf-8'/>\n"
        + "<title>Betty Black</title>\n</head>\n<body>\n"
        + "<div id='name'>\n  <p>name:</p>\n  <h1>Betty Black</h1>\n</div>\n"
        + "<div id='phone'>\n  <p>phone:</p>\n  <p><small>5123</small></p>\n</div>\n"
        + "</body>\n</html>\n";
    goldenContent.setContent(targetContentId3);
    goldenContent.setContent(targetContentId5);
    multiMockContent(goldenContent);
    Item itemId3 = new Item()
        .setName("id3")
        .setAcl(null)
        .setItemType(ItemType.CONTENT_ITEM.name());
    itemId3.setMetadata(new ItemMetadata().setSourceRepositoryUrl("id3"));
    RepositoryDoc docId3 = new RepositoryDoc.Builder().setItem(itemId3)
        .setContent(goldenContent.getContent(0), ContentFormat.HTML).build();
    Item itemId5 = new Item()
        .setName("id5")
        .setAcl(null)
        .setItemType(ItemType.CONTENT_ITEM.name());
    itemId5.setMetadata(new ItemMetadata().setSourceRepositoryUrl("id5"));
    RepositoryDoc docId5 = new RepositoryDoc.Builder().setItem(itemId5)
        .setContent(goldenContent.getContent(1), ContentFormat.HTML).build();
    List<ApiOperation> targetDocs = Arrays.asList(docId3, docId5);
    IncrementalCheckpoint checkpoint =
        new IncrementalCheckpoint(Timestamp.valueOf(checkpointTime).getTime());
    return new CheckpointCloseableIterableImpl.Builder<ApiOperation>(targetDocs)
        .setCheckpoint(checkpoint.get()).build();
  }

  /** common table builder */
  private void buildTable(Statement stmt) throws SQLException {
    stmt.execute("create table testtable "
        + "(id varchar(32) unique not null, name varchar(128), phone varchar(16), "
        + "lastmod_timestamp timestamp)");
    stmt.execute("insert into testtable (id, name, phone, lastmod_timestamp) "
        + "values ('id2', 'Mary Jones', '2134', '2017-01-11 14:55:22.2')");
    stmt.execute("insert into testtable (id, name, phone, lastmod_timestamp) "
        + "values ('id1', 'Joe Smith', '1234', '2017-01-12 13:59:11.1')");
    stmt.execute("insert into testtable (id, name, phone, lastmod_timestamp) "
        + "values ('id3', 'Mike Brown', '3124', '" + LATEST_CHECKPOINT_TIMESTAMP + "')");
    stmt.execute("insert into testtable (id, name, phone, lastmod_timestamp) "
        + "values ('id5', 'Betty Black', '5123', '2017-01-12 14:55:55.5')");
    stmt.execute("insert into testtable (id, name, phone, lastmod_timestamp) "
        + "values ('id4', 'Sue Green', '4123', '2017-01-12 14:44:44.4')");
  }

  @Test
  public void testGetChangesSqlError() throws Exception {
    setupConfiguration(getUrl(), "");
    InMemoryDBConnectionFactory factory = new InMemoryDBConnectionFactory();
    when(helperMock.getConnectionFactory()).thenReturn(factory);
    DatabaseRepository dbRepository = new DatabaseRepository(helperMock);
    dbRepository.init(repositoryContextMock);
    thrown.expect(RepositoryException.class);
    thrown.expectMessage("Error with SQL query");
    try {
      dbRepository.getChanges(null); // no table
    } finally {
      dbRepository.close();
      factory.shutdown();
    }
  }

  @Test
  public void testGetChangesIncUpdateUndefined() throws Exception {
    Properties config = new Properties();
    setAllMandatory(config);
    config.put(DatabaseConnectionFactory.DB_URL, getUrl());
    config.put(ColumnManager.DB_INC_UPDATE_SQL, "");
    setupConfig.initConfig(config);
    InMemoryDBConnectionFactory factory = new InMemoryDBConnectionFactory();
    when(helperMock.getConnectionFactory()).thenReturn(factory);
    DatabaseRepository dbRepository = new DatabaseRepository(helperMock);
    dbRepository.init(repositoryContextMock);
    try {
      assertNull(dbRepository.getChanges(null)); // no table
    } finally {
      dbRepository.close();
      factory.shutdown();
    }
  }

  @Test
  public void next_accessNextReturnsFalse_throwsException() throws SQLException, IOException {
    Properties config = new Properties();
    setAllMandatory(config);
    config.put(DatabaseConnectionFactory.DB_URL, getUrl());
    setupConfig.initConfig(config);
    InMemoryDBConnectionFactory factory = new InMemoryDBConnectionFactory();
    when(helperMock.getConnectionFactory()).thenReturn(factory);
    DatabaseRepository dbRepository = new DatabaseRepository(helperMock);
    dbRepository.init(repositoryContextMock);
    when(databaseAccessMock.next()).thenReturn(false);
    try (CheckpointCloseableIterable<ApiOperation> res =
        dbRepository.getRepositoryDocIterable(databaseAccessMock, new FullCheckpoint())) {
      thrown.expect(NoSuchElementException.class);
      res.iterator().next();
    } finally {
      dbRepository.close();
      factory.shutdown();
    }
  }

  private ImmutableList<Class<? extends Exception>>
        parametersFor_next_accessNextThrowsException_throwsException() {
    return ImmutableList.of(SQLException.class, IOException.class);
  }

  @Test
  @Parameters(method = "parametersFor_next_accessNextThrowsException_throwsException")
  public void next_accessNextThrowsException_throwsException(Class<? extends Exception> exception)
      throws SQLException, IOException {
    Properties config = new Properties();
    setAllMandatory(config);
    config.put(DatabaseConnectionFactory.DB_URL, getUrl());
    setupConfig.initConfig(config);
    InMemoryDBConnectionFactory factory = new InMemoryDBConnectionFactory();
    when(helperMock.getConnectionFactory()).thenReturn(factory);
    DatabaseRepository dbRepository = new DatabaseRepository(helperMock);
    dbRepository.init(repositoryContextMock);
    when(databaseAccessMock.next()).thenThrow(exception);
    try (CheckpointCloseableIterable<ApiOperation> res =
        dbRepository.getRepositoryDocIterable(databaseAccessMock, new FullCheckpoint())) {
      thrown.expect(NoSuchElementException.class);
      res.iterator().next();
    } finally {
      dbRepository.close();
      factory.shutdown();
    }
  }

  @Test
  public void testCreateResultSetRecordException() throws SQLException, IOException {
    Properties config = new Properties();
    setAllMandatory(config);
    config.put(DatabaseConnectionFactory.DB_URL, getUrl());
    setupConfig.initConfig(config);
    InMemoryDBConnectionFactory factory = new InMemoryDBConnectionFactory();
    when(helperMock.getConnectionFactory()).thenReturn(factory);
    DatabaseRepository dbRepository = new DatabaseRepository(helperMock);
    dbRepository.init(repositoryContextMock);
    when(databaseAccessMock.next()).thenReturn(true);
    when(databaseAccessMock.getAllColumnValues()).thenThrow(IllegalStateException.class);
    try (CheckpointCloseableIterable<ApiOperation> res =
        dbRepository.getRepositoryDocIterable(databaseAccessMock, new FullCheckpoint())) {
      thrown.expect(IllegalStateException.class);
      res.iterator().next();
    } finally {
      dbRepository.close();
      factory.shutdown();
    }
  }

  @Test
  public void testPagination() {
    assertEquals("offset", Pagination.OFFSET.toString());
    assertEquals(Pagination.NONE, Pagination.fromString("none"));
    assertEquals(Pagination.OFFSET, Pagination.fromString("offset"));
    assertEquals(Pagination.INVALID, Pagination.fromString("foo"));
  }

  @Test
  public void testFullCheckpoint() throws RepositoryException {
    FullCheckpoint fromNothing = new FullCheckpoint();
    FullCheckpoint fromSetters = new FullCheckpoint().setPagination(Pagination.OFFSET).setOffset(9);
    FullCheckpoint fromParse = FullCheckpoint.parse(fromSetters.get());

    assertEquals(fromSetters, fromParse);
    assertThat(fromNothing.toString(), fromNothing, not(fromSetters));
  }

  @Test
  public void testFullCheckpoint_update() {
    FullCheckpoint none = new FullCheckpoint();
    FullCheckpoint offset = new FullCheckpoint().setPagination(Pagination.OFFSET);

    none.updateCheckpoint(Collections.emptyMap());
    assertEquals(0, none.getOffset());
    offset.updateCheckpoint(Collections.emptyMap());
    assertEquals(1, offset.getOffset());
  }

  @Test
  public void testIncrementalCheckpoint() throws RepositoryException {
    IncrementalCheckpoint fromNothing = new IncrementalCheckpoint();
    IncrementalCheckpoint fromTimestamp =
        new IncrementalCheckpoint(Timestamp.valueOf("2018-06-18 14:50:00.0").getTime());
    IncrementalCheckpoint fromParse = IncrementalCheckpoint.parse(fromTimestamp.get());

    assertEquals(fromTimestamp, fromParse);
    assertThat(fromNothing.toString(), fromNothing, not(fromTimestamp));
  }

  @Test
  public void testIncrementalCheckpoint_update() {
    IncrementalCheckpoint checkpoint = new IncrementalCheckpoint(-1);
    assertEquals(-1, checkpoint.getLastUpdateTime());
    checkpoint.updateCheckpoint(Collections.emptyMap());
    assertEquals(0, checkpoint.getLastUpdateTime());
  }

  // TODO(normang): Update all not implemented method tests when needed
  @Test
  public void testGetIds() throws IOException {
    DatabaseRepository dbRepository = new DatabaseRepository(helperMock);
    thrown.expect(UnsupportedOperationException.class);
    thrown.expectMessage(containsString("not supported"));
    dbRepository.getIds(NULL_TRAVERSAL_CHECKPOINT);
  }

  @Test
  public void testGetDoc() throws IOException {
    DatabaseRepository dbRepository = new DatabaseRepository(helperMock);
    thrown.expect(UnsupportedOperationException.class);
    thrown.expectMessage(containsString("not supported"));
    dbRepository.getDoc(new Item());
  }

  @Test
  public void testExists() throws IOException {
    DatabaseRepository dbRepository = new DatabaseRepository(helperMock);
    thrown.expect(UnsupportedOperationException.class);
    thrown.expectMessage(containsString("not supported"));
    dbRepository.exists(new Item());
  }

  private void assertCheckpointEquals(Checkpoint expected, byte[] actual)
      throws RepositoryException {
    assertEquals(expected, Checkpoint.parse(actual, expected.getClass()));
  }

  // TODO(jlacey): This is an improvement over CompareCheckpointCloseableIterableRule.
  // Move this to the SDK, either as-is, or as a Hamcrest Matcher or a Truth Subject.
  // (We intend to migrate to Truth, but see b/123863881.)
  private <T> void assertIterableEquals(CheckpointCloseableIterable<T> expected,
      CheckpointCloseableIterable<T> actual) {
    Iterator actualIterator = actual.iterator();
    for (T element : expected) {
      assertTrue("Missing expected elements starting at: " + element, actualIterator.hasNext());
      assertEquals(element, actualIterator.next());
    }
    if (actualIterator.hasNext()) {
      fail("Unexpected elements starting at: " + actualIterator.next());
    }
    assertArrayEquals("getCheckpoint", expected.getCheckpoint(), actual.getCheckpoint());
    assertEquals("hasMore", expected.hasMore(), actual.hasMore());
  }

  @Test
  public void invalidColumnName_shouldThrowException()
      throws Exception {
    String createSql = "create table testtable "
        + "(id varchar(32) unique not null, name varchar(128))";
    String insertSql = "insert into testtable (id, name) "
        + "values ('id1', 'Joe Smith')";

    Properties config = new Properties();
    config.put(DatabaseConnectionFactory.DB_URL, getUrl());
    config.put(ColumnManager.DB_UNIQUE_KEY_COLUMNS, "id");
    config.put(ColumnManager.DB_ALL_COLUMNS, "id, invalidcolumn");
    config.put(ColumnManager.DB_CONTENT_COLUMNS, "*");
    config.put(ColumnManager.DB_ALL_RECORDS_SQL,
        "select id, invalidcolumn from testtable");
    config.put(UrlBuilder.CONFIG_FORMAT, "{0}");
    config.put(UrlBuilder.CONFIG_COLUMNS, "id");
    config.put(CONFIG_TITLE_DB_FORMAT, "id");
    config.put(DefaultAcl.DEFAULT_ACL_MODE, DefaultAclMode.FALLBACK.toString());
    setupConfig.initConfig(config);
    InMemoryDBConnectionFactory factory = new InMemoryDBConnectionFactory();
    when(helperMock.getConnectionFactory()).thenReturn(factory);
    DatabaseRepository dbRepository = new DatabaseRepository(helperMock);
    dbRepository.init(repositoryContextMock);

    Connection conn = factory.createConnection();
    try {
      Statement stmt = conn.createStatement();
      stmt.execute(createSql);
      stmt.execute(insertSql);
      stmt.close();

      try {
        dbRepository.getAllDocs(NULL_TRAVERSAL_CHECKPOINT);
      } catch (RepositoryException e) {
        Throwable t = e.getCause();
        assertTrue(t instanceof SQLException);
        assertTrue(t.getMessage().contains("Column \"invalidcolumn\" not found"));
      }
    } finally {
      factory.releaseConnection(conn);
      dbRepository.close();
      factory.shutdown();
    }
  }

  @Parameters({
    "id, name, data",
    "ID, NAME, DATA",
    "Id, Name, Data"
  })
  public void caseSensitiveColumnName_shouldSucceed(String id, String name, String data)
      throws Exception {
    String createSql = "create table testtable ("
        + "\"" + id + "\" varchar(32) unique not null, "
        + "\"" + name + "\" varchar(128), "
        + "\"" + data + "\" varchar(128))";
    String insertSql = "insert into testtable (\"" + id + "\", \"" + name + "\", \"" + data + "\")"
        + "values ('id1', 'Joe Smith', 'Column name case Test')";

    String expected = "<!DOCTYPE html>\n<html lang='en'>\n<head>\n"
        + "<meta http-equiv='Content-Type' content='text/html; charset=utf-8'/>\n"
        + "<title>id1</title>\n</head>\n<body>\n"
        + "<div id='" + id + "'>\n  <p>"
        + id + ":</p>\n  <h1>id1</h1>\n</div>\n"
        + "<div id='" + data + "'>\n  <p>"
        + data + ":</p>\n  <p><small>Column name case Test</small></p>\n</div>\n"
        + "<div id='" + name + "'>\n  <p>"
        + name + ":</p>\n  <p><small>Joe Smith</small></p>\n</div>\n"
        + "</body>\n</html>\n";

    Properties config = new Properties();
    String dbUrl = "jdbc:h2:mem:" + databaseCount.getAndIncrement();
    config.put(DatabaseConnectionFactory.DB_URL, dbUrl);
    config.put(ColumnManager.DB_UNIQUE_KEY_COLUMNS, id);
    config.put(ColumnManager.DB_ALL_COLUMNS, id + ", " + name +  ", " + data);
    config.put(ColumnManager.DB_CONTENT_COLUMNS, "*");
    config.put(ColumnManager.DB_ALL_RECORDS_SQL,
        "select \"" + id + "\", \"" + name +  "\", \"" + data + "\" from testtable");
    config.put(UrlBuilder.CONFIG_FORMAT, "{0}");
    config.put(UrlBuilder.CONFIG_COLUMNS, id);
    config.put(CONFIG_TITLE_DB_FORMAT, id);
    config.put(DefaultAcl.DEFAULT_ACL_MODE, DefaultAclMode.FALLBACK.toString());
    setupConfig.initConfig(config);
    InMemoryDBConnectionFactory factory = new InMemoryDBConnectionFactory();
    when(helperMock.getConnectionFactory()).thenReturn(factory);
    mockContent();
    DatabaseRepository dbRepository = new DatabaseRepository(helperMock);
    dbRepository.init(repositoryContextMock);

    Connection conn = factory.createConnection();
    try {
      // build the db
      Statement stmt = conn.createStatement();
      stmt.execute(createSql);
      stmt.execute(insertSql);
      stmt.close();

      // query the db
      try (CheckpointCloseableIterable<ApiOperation> records =
               dbRepository.getAllDocs(NULL_TRAVERSAL_CHECKPOINT)) {
        RepositoryDoc record = (RepositoryDoc) records.iterator().next();
        String html = CharStreams.toString(
            new InputStreamReader(record.getContent().getInputStream(), Charsets.UTF_8));
        assertEquals(expected, html);
      }
    } finally {
      factory.releaseConnection(conn);
      dbRepository.close();
      factory.shutdown();
    }
  }

  @Test
  // Test scenario here should be classified as invalid configuration. This is a placeholder test
  // that validates current implementation which allows using blob columns in unique key.
  public void blobColumn_uniqueKey_isAllowed() throws Exception {
    Properties config = new Properties();
    config.put(DatabaseConnectionFactory.DB_URL, getUrl());
    config.put(ColumnManager.DB_UNIQUE_KEY_COLUMNS, "blob_data");
    config.put(ColumnManager.DB_ALL_COLUMNS, "id, name, blob_data");
    config.put(ColumnManager.DB_ALL_RECORDS_SQL, "select id, name, blob_data from testtable");
    config.put(ColumnManager.DB_BLOB_COLUMN, "blob_data");
    config.put(ColumnManager.DB_CONTENT_COLUMNS, "*");
    config.put(UrlBuilder.CONFIG_FORMAT, "{0}");
    config.put(UrlBuilder.CONFIG_COLUMNS, "name");
    config.put(DefaultAcl.DEFAULT_ACL_MODE, DefaultAclMode.FALLBACK.toString());
    setupConfig.initConfig(config);
    InMemoryDBConnectionFactory factory = new InMemoryDBConnectionFactory();
    when(helperMock.getConnectionFactory()).thenReturn(factory);
    DatabaseRepository dbRepository = new DatabaseRepository(helperMock);
    dbRepository.init(repositoryContextMock);

    // create target for verification
    String targetContent = "This is some blob content." + '\0' + '\t';
    Connection conn = factory.createConnection();
    try {
      // build the db
      PreparedStatement stmt =
          conn.prepareStatement(
              "create table testtable "
                  + "(id varchar(32) unique not null, name varchar(128), blob_data blob)");
      stmt.execute();
      stmt.close();
      stmt =
          conn.prepareStatement(
              "insert into testtable (id, name, blob_data) values ('id1', 'Joe Smith', ?)");
      Blob targetBlob = conn.createBlob();
      targetBlob.setBytes(1, targetContent.getBytes(UTF_8));
      stmt.setBlob(1, targetBlob);
      stmt.execute();
      stmt.close();
      targetBlob.free();

      // query the db
      try (CheckpointCloseableIterable<ApiOperation> records =
          dbRepository.getAllDocs(NULL_TRAVERSAL_CHECKPOINT)) {
        RepositoryDoc record = (RepositoryDoc) records.iterator().next();
        String html =
            CharStreams.toString(
                new InputStreamReader(record.getContent().getInputStream(), Charsets.UTF_8));
        assertEquals(targetContent, html);
        String itemName = record.getItem().getName();
        assertTrue(
            String.format(
                "Item name should start with [B@ as default implementation "
                    + "for byte[].toString(). Instead name is [%s]",
                itemName),
            itemName.startsWith("[B@"));
      }
    } finally {
      factory.releaseConnection(conn);
      dbRepository.close();
      factory.shutdown();
    }
  }

  @Test
  // Test scenario here should be classified as invalid configuration. This is a placeholder test
  // that validates current implementation which allows binary columns in URL.
  public void varbinary_urlField_isAllowed() throws Exception {
    Properties config = new Properties();
    config.put(DatabaseConnectionFactory.DB_URL, getUrl());
    config.put(ColumnManager.DB_UNIQUE_KEY_COLUMNS, "id");
    config.put(ColumnManager.DB_ALL_COLUMNS, "id, name, binary_data");
    config.put(ColumnManager.DB_ALL_RECORDS_SQL, "select id, name, binary_data from testtable");
    config.put(ColumnManager.DB_BLOB_COLUMN, "binary_data");
    config.put(ColumnManager.DB_CONTENT_COLUMNS, "*");
    config.put(UrlBuilder.CONFIG_FORMAT, "http://whybinary.com/{0}");
    config.put(UrlBuilder.CONFIG_COLUMNS, "binary_data");
    config.put(DefaultAcl.DEFAULT_ACL_MODE, DefaultAclMode.FALLBACK.toString());
    setupConfig.initConfig(config);
    InMemoryDBConnectionFactory factory = new InMemoryDBConnectionFactory();
    when(helperMock.getConnectionFactory()).thenReturn(factory);
    DatabaseRepository dbRepository = new DatabaseRepository(helperMock);
    dbRepository.init(repositoryContextMock);

    // create target for verification
    String targetContent = "This is some binary content.";
    Connection conn = factory.createConnection();
    try {
      // build the db
      PreparedStatement stmt =
          conn.prepareStatement(
              "create table testtable "
                  + "(id varchar(32) unique not null, name varchar(128), binary_data varbinary)");
      stmt.execute();
      stmt.close();
      stmt =
          conn.prepareStatement(
              "insert into testtable (id, name, binary_data) values "
                  + "('id1', 'Joe Smith', STRINGTOUTF8('This is some binary content.'))");
      stmt.execute();
      stmt.close();
      // query the db
      try (CheckpointCloseableIterable<ApiOperation> records =
          dbRepository.getAllDocs(NULL_TRAVERSAL_CHECKPOINT)) {
        RepositoryDoc record = (RepositoryDoc) records.iterator().next();
        String html =
            CharStreams.toString(
                new InputStreamReader(record.getContent().getInputStream(), Charsets.UTF_8));
        assertEquals(targetContent, html);
        String sourceRepositoryUrl = record.getItem().getMetadata().getSourceRepositoryUrl();
        assertTrue(
            String.format(
                "Item URL should start with http://whybinary.com/[B@ as default implementation "
                    + "for byte[].toString(). Instead URL is [%s]",
                sourceRepositoryUrl),
            sourceRepositoryUrl.startsWith("http://whybinary.com/[B@"));
      }
    } finally {
      factory.releaseConnection(conn);
      dbRepository.close();
      factory.shutdown();
    }
  }

  @Test
  @Parameters({
    "int, 11", // int, integer, mediumint, int4, signed
    "bit, false", // boolean, bit, bool
    "tinyint, 127",
    "smallint, 32767", // smallint, int2, year
    "bigint, 9223372036854775807", // bigint, int8
    "identity, 0",
    "decimal, 43.21", // decimal, number, dec, numeric
    "float, 11.23", // double, float, float8
    "real, 12.34" // real, float(precisionInt), float4
  })
  // http://www.h2database.com/html/datatypes.html
  public void numericTypes_h2_succeeds(String typeName, String value) throws Exception {
    Properties config = new Properties();
    config.put(DatabaseConnectionFactory.DB_URL, getUrl());
    config.put(ColumnManager.DB_UNIQUE_KEY_COLUMNS, "id");
    config.put(ColumnManager.DB_ALL_COLUMNS, "id, numeric_type, type_name");
    config.put(ColumnManager.DB_ALL_RECORDS_SQL,
        "select id, numeric_type, type_name from testtable");
    config.put(ColumnManager.DB_CONTENT_COLUMNS, "*");
    config.put(UrlBuilder.CONFIG_FORMAT, "http://example.com/{0}");
    config.put(UrlBuilder.CONFIG_COLUMNS, "id");
    config.put(CONFIG_TITLE_DB_FORMAT, "id");
    config.put(DefaultAcl.DEFAULT_ACL_MODE, DefaultAclMode.FALLBACK.toString());
    setupConfig.initConfig(config);
    InMemoryDBConnectionFactory factory = new InMemoryDBConnectionFactory();
    when(helperMock.getConnectionFactory()).thenReturn(factory);
    mockContent();
    DatabaseRepository dbRepository = new DatabaseRepository(helperMock);
    dbRepository.init(repositoryContextMock);

    // create target for verification
    String targetContent = "<!DOCTYPE html>\n"
        + "<html lang='en'>\n"
        + "<head>\n"
        + "<meta http-equiv='Content-Type' content='text/html; charset=utf-8'/>\n"
        + "<title>id1</title>\n"
        + "</head>\n"
        + "<body>\n"
        + "<div id='id'>\n"
        + "  <p>id:</p>\n"
        + "  <h1>id1</h1>\n"
        + "</div>\n"
        + "<div id='type_name'>\n"
        + "  <p>type_name:</p>\n"
        + "  <p><small>" + typeName + "</small></p>\n"
        + "</div>\n"
        + "<div id='numeric_type'>\n"
        + "  <p>numeric_type:</p>\n"
        + "  <p><small>" + value + "</small></p>\n"
        + "</div>\n"
        + "</body>\n"
        + "</html>\n";
    Connection conn = factory.createConnection();
    try {
      // build the db
      String createStmt = "create table testtable ("
          + "id varchar(32) unique not null, "
          + "numeric_type " + typeName + ", "
          + "type_name varchar(32))";
      String insertStmt =
          "insert into testtable (id, numeric_type, type_name) values ('id1', ?, ?)";
      try (PreparedStatement stmt = conn.prepareStatement(createStmt)) {
        stmt.execute();
      }
      try (PreparedStatement stmt = conn.prepareStatement(insertStmt)) {
        stmt.setString(1, value);
        stmt.setString(2, typeName);
        stmt.execute();
      }
      // query the db
      try (CheckpointCloseableIterable<ApiOperation> records =
          dbRepository.getAllDocs(NULL_TRAVERSAL_CHECKPOINT)) {
        RepositoryDoc record = (RepositoryDoc) records.iterator().next();
        String html =
            CharStreams.toString(
                new InputStreamReader(record.getContent().getInputStream(), Charsets.UTF_8));
        assertEquals(targetContent, html);
      }
    } finally {
      factory.releaseConnection(conn);
      dbRepository.close();
      factory.shutdown();
    }
  }

  @Test
  @Parameters({
    "binary", "varbinary" // binary, varbinary, binary varying, longvarbinary, raw, bytea
  })
  public void binaryTypes_h2_succeeds(String typeName) throws Exception {
    Properties config = new Properties();
    config.put(DatabaseConnectionFactory.DB_URL, getUrl());
    config.put(ColumnManager.DB_UNIQUE_KEY_COLUMNS, "id");
    config.put(ColumnManager.DB_ALL_COLUMNS, "id, binary_type, type_name");
    config.put(ColumnManager.DB_ALL_RECORDS_SQL,
        "select id, binary_type, type_name from testtable");
    config.put(ColumnManager.DB_CONTENT_COLUMNS, "*");
    config.put(UrlBuilder.CONFIG_FORMAT, "http://example.com/{0}");
    config.put(UrlBuilder.CONFIG_COLUMNS, "id");
    config.put(CONFIG_TITLE_DB_FORMAT, "id");
    config.put(DefaultAcl.DEFAULT_ACL_MODE, DefaultAclMode.FALLBACK.toString());
    setupConfig.initConfig(config);
    InMemoryDBConnectionFactory factory = new InMemoryDBConnectionFactory();
    when(helperMock.getConnectionFactory()).thenReturn(factory);
    mockContent();
    DatabaseRepository dbRepository = new DatabaseRepository(helperMock);
    dbRepository.init(repositoryContextMock);

    Connection conn = factory.createConnection();
    try {
      // build the db
      String createStmt = "create table testtable ("
          + "id varchar(32) unique not null, "
          + "binary_type " + typeName + ","
          + "type_name varchar(32))";
      String insertStmt =
          "insert into testtable (id, binary_type, type_name) values ('id1', ?, ?)";
      try (PreparedStatement stmt = conn.prepareStatement(createStmt)) {
        stmt.execute();
      }
      try (PreparedStatement stmt = conn.prepareStatement(insertStmt)) {
        stmt.setBytes(1, "binary data".getBytes(Charsets.UTF_8));
        stmt.setString(2, typeName);
        stmt.execute();
      }
      // query the db
      try (CheckpointCloseableIterable<ApiOperation> records =
          dbRepository.getAllDocs(NULL_TRAVERSAL_CHECKPOINT)) {
        RepositoryDoc record = (RepositoryDoc) records.iterator().next();
        String html =
            CharStreams.toString(
                new InputStreamReader(record.getContent().getInputStream(), Charsets.UTF_8));
        assertThat(html, containsString("<small>[B@"));
      }
    } finally {
      factory.releaseConnection(conn);
      dbRepository.close();
      factory.shutdown();
    }
  }
}
