/*
 * Copyright 2019 Google LLC
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.google.enterprise.cloudsearch.database;

import static com.google.enterprise.cloudsearch.sdk.TestProperties.SERVICE_KEY_PROPERTY_NAME;
import static com.google.enterprise.cloudsearch.sdk.TestProperties.qualifyTestProperty;
import static java.util.Arrays.asList;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import com.google.api.services.cloudsearch.v1.model.Item;
import com.google.common.base.Splitter;
import com.google.common.base.Strings;
import com.google.common.collect.ImmutableList;
import com.google.enterprise.cloudsearch.sdk.Util;
import com.google.enterprise.cloudsearch.sdk.config.Configuration.ResetConfigRule;
import com.google.enterprise.cloudsearch.sdk.indexing.CloudSearchService;
import com.google.enterprise.cloudsearch.sdk.indexing.DefaultAcl.DefaultAclMode;
import com.google.enterprise.cloudsearch.sdk.indexing.IndexingApplication;
import com.google.enterprise.cloudsearch.sdk.indexing.IndexingItemBuilder.ItemType;
import com.google.enterprise.cloudsearch.sdk.indexing.MockItem;
import com.google.enterprise.cloudsearch.sdk.indexing.StructuredDataHelper;
import com.google.enterprise.cloudsearch.sdk.indexing.TestUtils;
import com.google.enterprise.cloudsearch.sdk.indexing.template.FullTraversalConnector;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.Properties;
import java.util.logging.Level;
import java.util.logging.Logger;
import org.junit.BeforeClass;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.junit.rules.TestName;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

/**
 * Tests to check the integration between the database connector and CloudSearch Indexing API.
 */
@RunWith(JUnit4.class)
public class DatabaseConnectorIT {
  private static final Logger logger = Logger.getLogger(DatabaseConnectorIT.class.getName());
  private static final String DATA_SOURCE_ID_PROPERTY_NAME = qualifyTestProperty("sourceId");
  private static final String ROOT_URL_PROPERTY_NAME = qualifyTestProperty("rootUrl");
  private static final String SQL_SERVER_PARAMS_PROPERTY_NAME =
      qualifyTestProperty("dbSqlParameters");
  private static String dbSqlParameters;
  private static String keyFilePath;
  private static String indexingSourceId;
  private static Optional<String> rootUrl;
  private static CloudSearchService v1Client;
  private static TestUtils testUtils;

  private static class DatabaseConnectionParams {
    private final String dbUrl;
    private final String user;
    private final String password;

    private DatabaseConnectionParams(String dbUrl, String user, String password) {
      this.dbUrl = dbUrl;
      this.user = user;
      this.password = password;
    }
  }

  private static enum Database {
    H2, SQLSERVER
  };

  @Rule public ResetConfigRule resetConfig = new ResetConfigRule();
  @Rule public TemporaryFolder configFolder = new TemporaryFolder();
  @Rule public TestName name = new TestName();

  @BeforeClass
  public static void initialize() throws Exception {
    validateInputParams();
    v1Client = new CloudSearchService(keyFilePath, indexingSourceId, rootUrl);
    StructuredDataHelper.verifyMockContentDatasourceSchema(v1Client.getSchema());
    testUtils = new TestUtils(v1Client);
  }

  private DatabaseConnectionParams getDatabaseConnectionParams(Database databaseType)
      throws IOException {
    if (databaseType == Database.SQLSERVER) {
      List<String> params =
          Splitter.on(",").trimResults().omitEmptyStrings().splitToList(dbSqlParameters);
      if (params.size() != 3) {
        logger.log(Level.SEVERE, getUsageString());
        throw new IllegalArgumentException(
            "Invalid connection params for SQL Server -" + dbSqlParameters);
      }
      return new DatabaseConnectionParams(params.get(0), params.get(1), params.get(2));
    } else {
      String dbUrl =
          "jdbc:h2:" + new File(configFolder.newFolder(), "integration-test").getAbsolutePath()
              + ";DATABASE_TO_UPPER=false";
      return new DatabaseConnectionParams(dbUrl, "sa", "");
    }
  }

  private static String getUsageString() {
    return "Missing input parameters. Rerun the test as \"mvn verify"
            + " -DargLine=-Dapi.test.serviceAccountPrivateKeyFile=./path/to/key.json"
            + " -Dapi.test.sourceId=dataSourceId\""
            + " -Dapi.test.dbSqlParameters="
            + "jdbc:sqlserver:\\<ipAddress>;databaseName=<dbName>;,<sqlUser>,<sqlPwd>\"";
  }

  private void executeDatabaseStatement(DatabaseConnectionParams dbConnection,
      List<String> queryStatement) throws SQLException, IOException {
    // h2 will automatically create database if not available.
    try (
        Connection connection =
        DriverManager.getConnection(dbConnection.dbUrl, dbConnection.user, dbConnection.password);
        Statement statement = connection.createStatement()) {
      for (String query : queryStatement) {
        statement.execute(query);
      }
    }
  }

  private Properties createRequiredProperties(DatabaseConnectionParams dbConnection)
      throws IOException {
    Properties config = new Properties();
    rootUrl.ifPresent(r -> config.setProperty("api.rootUrl", r));
    config.setProperty("api.sourceId", indexingSourceId);
    config.setProperty("api.serviceAccountPrivateKeyFile", keyFilePath);
    config.setProperty("connector.runOnce", "true");
    config.setProperty("connector.checkpointDirectory",
        configFolder.newFolder().getAbsolutePath());
    config.setProperty("db.url", dbConnection.dbUrl);
    config.setProperty("db.user", dbConnection.user);
    config.setProperty("db.password", dbConnection.password);
    config.setProperty("db.viewUrlColumns", "id");
    config.setProperty("db.uniqueKeyColumns", "id");
    config.setProperty("url.columns", "id, name");
    config.setProperty("itemMetadata.title.field", "id");
    config.setProperty("itemMetadata.contentLanguage.defaultValue", "en-US");
    config.setProperty("contentTemplate.db.title", "id");
    config.setProperty("defaultAcl.mode", DefaultAclMode.FALLBACK.toString());
    config.setProperty("defaultAcl.public", "true");
    config.setProperty("traverse.queueTag", "mockDatabaseConnectorQueue-" + Util.getRandomId());
    return config;
  }

  private static void validateInputParams() throws IOException {
    String dataSourceId;
    Path serviceKeyPath;
    logger.log(Level.FINE, "Validating input parameters...");
    try {
      dataSourceId = System.getProperty(DATA_SOURCE_ID_PROPERTY_NAME);
      serviceKeyPath = Paths.get(System.getProperty(SERVICE_KEY_PROPERTY_NAME));
      assertTrue(serviceKeyPath.toFile().exists());
      assertFalse(Strings.isNullOrEmpty(dataSourceId));
      rootUrl = Optional.ofNullable(System.getProperty(ROOT_URL_PROPERTY_NAME));
      dbSqlParameters = System.getProperty(SQL_SERVER_PARAMS_PROPERTY_NAME);
    } catch (AssertionError error) {
      logger.log(Level.SEVERE, getUsageString());
      throw error;
    }
    indexingSourceId = dataSourceId;
    keyFilePath = serviceKeyPath.toAbsolutePath().toString();
  }

  private String[] setupDataAndConfiguration(DatabaseConnectionParams dbConnection,
      Properties additionalConfig, List<String> queryStatement) throws SQLException, IOException {
    executeDatabaseStatement(dbConnection, queryStatement);
    Properties config = createRequiredProperties(dbConnection);
    config.putAll(additionalConfig);
    logger.log(Level.INFO, "Config file properties: {0}", config);
    File file = configFolder.newFile();
    try (FileOutputStream output = new FileOutputStream(file)) {
      config.store(output, "properties file");
      output.flush();
    }
    return new String[] {"-Dconfig=" + file.getAbsolutePath()};
  }

  @Test
  public void testHappyFlow() throws IOException, SQLException, InterruptedException {
    String randomId = Util.getRandomId();
    String tableName = name.getMethodName() + randomId;
    Properties config = new Properties();
    config.setProperty("db.allRecordsSql", "Select id, name, phone from " + tableName);
    config.setProperty("db.allColumns", "id, name, phone");
    String row1 = "x1" + randomId;
    String row2 = "x2" + randomId;
    String row3 = "x3" + randomId;
    List<String> query = new ArrayList<>();
    List<String> mockItems = new ArrayList<>();
    query.add("create table " + tableName
        + "(id varchar(32) unique not null, name varchar(128), phone varchar(16))");
    query.add("insert into " + tableName + " (id, name, phone)"
        + " values ('" + row1 + "', 'Jones May', '2134')");
    query.add("insert into " + tableName + " (id, name, phone)"
        + " values ('" + row2 + "', 'Joe Smith', '9848')");
    query.add("insert into " + tableName + " (id, name, phone)"
        + " values ('" + row3 + "', 'Mike Brown', '3476')");
    try {
      DatabaseConnectionParams dbConnection = getDatabaseConnectionParams(Database.H2);
      String[] args = setupDataAndConfiguration(dbConnection, config, query);
      MockItem itemId1 = new MockItem.Builder(getItemId(row1))
          .setTitle(row1)
          .setSourceRepositoryUrl(row1)
          .setContentLanguage("en-US")
          .setItemType(ItemType.CONTENT_ITEM.toString())
          .build();
      MockItem itemId2 = new MockItem.Builder(getItemId(row2))
          .setTitle(row2)
          .setSourceRepositoryUrl(row2)
          .setContentLanguage("en-US")
          .setItemType(ItemType.CONTENT_ITEM.toString())
          .build();
      MockItem itemId3 = new MockItem.Builder(getItemId(row3))
          .setTitle(row3)
          .setSourceRepositoryUrl(row3)
          .setContentLanguage("en-US")
          .setItemType(ItemType.CONTENT_ITEM.toString())
          .build();
      mockItems.addAll(asList(getItemId(row1), getItemId(row2), getItemId(row3)));
      runAwaitConnector(args);
      testUtils.waitUntilEqual(getItemId(row1), itemId1.getItem());
      testUtils.waitUntilEqual(getItemId(row2), itemId2.getItem());
      testUtils.waitUntilEqual(getItemId(row3), itemId3.getItem());
    } finally {
      v1Client.deleteItemsIfExist(mockItems);
    }
  }

  @Test
  public void testStructuredData() throws IOException, SQLException, InterruptedException {
    String randomId = Util.getRandomId();
    String tableName = name.getMethodName() + randomId;
    Properties config = new Properties();
    config.setProperty("db.allRecordsSql", "Select id, textField as text, integerField as integer,"
        + " booleanField as boolean, doubleField as double, dateField as date,"
        + " timestampField as timestamp, enumField as enum from " + tableName);
    config.setProperty("db.allColumns", "id, textField, integerField, booleanField, doubleField,"
        + " dateField, timestampField, enumField");
    config.setProperty("itemMetadata.objectType", "myMockDataObject");
    config.setProperty("url.columns", "id");
    config.setProperty("url.format", "http://example.com/employee/{0}");
    String row1 = "s1" + randomId;
    String row2 = "s2" + randomId;
    String row3 = "s3" + randomId;

    List<String> query = new ArrayList<>();
    List<String> mockItems = new ArrayList<>();
    query.add("create table " + tableName + "(id varchar(32) unique not null,"
        + " textField varchar(128), integerField integer(50), booleanField boolean,"
        + " doubleField double, dateField date, timestampField timestamp, enumField integer)");
    query.add("insert into " + tableName
        + " (id, textField, integerField, booleanField, doubleField, dateField, timestampField,"
        + " enumField) values ('" + row1 + "', 'Jones May', '2134678', 'true', '2000', "
            + "'2007-11-20', '1907-10-10T14:21:23.400Z', '2')");
    query.add("insert into " + tableName
        + " (id, textField, integerField, booleanField, doubleField, dateField, timestampField,"
        + " enumField) values ('" + row2 + "', 'Joe Smith', '-9846', 'false', '12000.00',"
            + " '1987-02-28', '2017-10-10T14:01:23.400Z', '1')");
    query.add("insert into " + tableName
        + " (id, textField, integerField, booleanField, doubleField, dateField, timestampField,"
        + " enumField) values ('" + row3 + "', 'Mike Smith', '9358014', 'true', '-9000.00',"
            + " '1940-11-11', '1817-10-10T14:21:23.040Z', '2')");
    try {
      DatabaseConnectionParams dbConnection = getDatabaseConnectionParams(Database.H2);
      String[] args = setupDataAndConfiguration(dbConnection, config, query);

      MockItem itemId1 = new MockItem.Builder(getItemId(row1))
          .setTitle(row1)
          .setSourceRepositoryUrl("http://example.com/employee/" + row1)
          .setContentLanguage("en-US")
          .setItemType(ItemType.CONTENT_ITEM.toString())
          .addValue("text", "Jones May")
          .addValue("integer", "2134678")
          .addValue("boolean", "true")
          .addValue("date", "2007-11-20")
          .addValue("double", "2000.00")
          .setObjectType("myMockDataObject")
          .addValue("timestamp", "1907-10-10T14:21:23.400Z")
          .addValue("enum", 2)
          .build();
      MockItem itemId2 = new MockItem.Builder(getItemId(row2))
          .setTitle(row2)
          .setSourceRepositoryUrl("http://example.com/employee/" + row2)
          .setContentLanguage("en-US")
          .setItemType(ItemType.CONTENT_ITEM.toString())
          .addValue("text", "Joe Smith")
          .addValue("integer", "-9846")
          .addValue("boolean", "false")
          .addValue("date", "1987-02-28")
          .addValue("double", "12000.00")
          .setObjectType("myMockDataObject")
          .addValue("timestamp", "2017-10-10T14:01:23.400Z")
          .addValue("enum", 1)
          .build();
      MockItem itemId3 = new MockItem.Builder(getItemId(row3))
          .setTitle(row3)
          .setSourceRepositoryUrl("http://example.com/employee/" + row3)
          .setContentLanguage("en-US")
          .setItemType(ItemType.CONTENT_ITEM.toString())
          .addValue("text", "Mike Smith")
          .addValue("integer", "9358014")
          .addValue("boolean", "true")
          .addValue("date", "1940-11-11")
          .addValue("double", "-9000.00")
          .setObjectType("myMockDataObject")
          .addValue("timestamp", "1817-10-10T14:21:23.040Z")
          .addValue("enum", 2)
          .build();
      mockItems.addAll(asList(getItemId(row1), getItemId(row2), getItemId(row3)));
      runAwaitConnector(args);
      verifyStructuredData(getItemId(row1), "myMockDataObject", itemId1.getItem());
      verifyStructuredData(getItemId(row2), "myMockDataObject", itemId2.getItem());
      verifyStructuredData(getItemId(row3), "myMockDataObject", itemId3.getItem());
    } finally {
      v1Client.deleteItemsIfExist(mockItems);
    }
  }

  @Test
  public void testItemMetadataDefaultValues()
      throws IOException, SQLException, InterruptedException {
    String randomId = Util.getRandomId();
    String tableName = name.getMethodName() + randomId;
    Properties config = new Properties();
    config.setProperty("db.allRecordsSql", "Select id, title, name, phone, created, modified,"
        + " language from " + tableName);
    config.setProperty("db.allColumns", "id, title, name, phone, created, modified, language");
    config.setProperty("itemMetadata.title.field", "title");
    // itemMetadata.field.defaultValue will take effect in case of null values.
    config.setProperty("itemMetadata.title.defaultValue", "DEFAULT_TITLE");
    config.setProperty("url.format", "https://DEFAULT.URL/{0}");
    config.setProperty("itemMetadata.sourceRepositoryUrl.defaultValue",
        "https://example.org/" + "name");
    config.setProperty("itemMetadata.createTime.field", "created");
    config.setProperty("itemMetadata.createTime.defaultValue", "2007-10-10T14:21:23.400Z");
    config.setProperty("itemMetadata.updateTime.field", "modified");
    config.setProperty("itemMetadata.updateTime.defaultValue", "1919-10-10T14:21:23.400Z");
    config.setProperty("itemMetadata.contentLanguage.field", "language");
    config.setProperty("itemMetadata.contentLanguage.defaultValue", "fr-CA");
    String row1 = "row1" + randomId;
    String row2 = "row2" + randomId;
    List<String> query = new ArrayList<>();
    List<String> mockItems = new ArrayList<>();
    query.add("create table " + tableName + "(id varchar(32) unique not null, title varchar(100), "
        + " name varchar(128), phone integer, created timestamp, modified timestamp,"
        + " language varchar(10))");
    query.add("insert into " + tableName + " (id, title, name, phone, created, modified, language)"
        + " values ('" + row1 + "', 'TitleRow1', 'Jones May', '2134', '1907-10-10T14:21:23.400Z',"
            + " '2018-10-10T14:21:23.400Z', 'de-ch')");
    query.add("insert into " + tableName + " (id, name, phone)"
        + " values ('" + row2 + "', 'Mike Smith', '9848')");
    try {
      DatabaseConnectionParams dbConnection = getDatabaseConnectionParams(Database.H2);
      String[] args = setupDataAndConfiguration(dbConnection, config, query);
      MockItem itemId1 = new MockItem.Builder(getItemId(row1))
          .setTitle("TitleRow1")
          .setSourceRepositoryUrl("https://DEFAULT.URL/" + row1)
          .setContentLanguage("de-ch")
          .setItemType(ItemType.CONTENT_ITEM.toString())
          .setCreateTime("1907-10-10T14:21:23.400Z")
          .setUpdateTime("2018-10-10T14:21:23.400Z")
          .build();
      MockItem itemId2 = new MockItem.Builder(getItemId(row2))
          .setTitle("DEFAULT_TITLE")
          .setSourceRepositoryUrl("https://DEFAULT.URL/" + row2)
          .setContentLanguage("fr-CA")
          .setCreateTime("2007-10-10T14:21:23.400Z")
          .setUpdateTime("1919-10-10T14:21:23.400Z")
          .build();
      mockItems.addAll(asList(getItemId(row1), getItemId(row2)));
      runAwaitConnector(args);
      testUtils.waitUntilEqual(getItemId(row1), itemId1.getItem());
      testUtils.waitUntilEqual(getItemId(row2), itemId2.getItem());
    } finally {
      v1Client.deleteItemsIfExist(mockItems);
    }
  }

  @Test
  public void testArrayDataTypes() throws IOException, SQLException, InterruptedException {
    String randomId = Util.getRandomId();
    String tableName = name.getMethodName() + randomId;
    Properties config = new Properties();
    config.setProperty("db.allRecordsSql", "Select id, arrayTextField as text,"
        + " arrayIntegerField as integer from " + tableName);
    config.setProperty("db.allColumns", "id, arrayTextField, arrayIntegerField");
    config.setProperty("itemMetadata.objectType", "myMockDataObject");
    config.setProperty("url.columns", "id");
    config.setProperty("url.format", "http://example.com/employee/{0}");
    String row1 = "row1" + randomId;
    String row2 = "row2" + randomId;

    List<String> query = new ArrayList<>();
    List<String> mockItems = new ArrayList<>();
    query.add("create table " + tableName + "(id varchar(32) unique not null,"
        + " arrayTextField array, arrayIntegerField array)");
    query.add("insert into " + tableName
        + " (id, arrayTextField, arrayIntegerField) values"
        + " ('" + row1 + "', ('joe', 'Smith', 'black'), ('1092', '8765'))");
    query.add("insert into " + tableName
        + " (id, arrayTextField, arrayIntegerField) values"
        + " ('" + row2 + "', ('Jim', 'Black'), ('1092873', '-128765'))");
    try {
      DatabaseConnectionParams dbConnection = getDatabaseConnectionParams(Database.H2);
      String[] args = setupDataAndConfiguration(dbConnection, config, query);
      MockItem itemId1 = new MockItem.Builder(getItemId(row1))
          .setTitle(row1)
          .setSourceRepositoryUrl("http://example.com/employee/" + row1)
          .setContentLanguage("en-US")
          .setItemType(ItemType.CONTENT_ITEM.toString())
          .addValue("text", "joe")
          .addValue("text", "Smith")
          .addValue("text", "black")
          .addValue("integer", "1092")
          .addValue("integer", "8765")
          .setObjectType("myMockDataObject")
          .build();
     MockItem itemId2 = new MockItem.Builder(getItemId(row2))
          .setTitle(row2)
          .setSourceRepositoryUrl("http://example.com/employee/" + row2)
          .setContentLanguage("en-US")
          .setItemType(ItemType.CONTENT_ITEM.toString())
          .addValue("text", "Jim")
          .addValue("text", "Black")
          .addValue("integer", "1092873")
          .addValue("integer", "-128765")
          .setObjectType("myMockDataObject")
          .build();
      mockItems.addAll(asList(getItemId(row1), getItemId(row2)));
      runAwaitConnector(args);
      verifyStructuredData(getItemId(row1), "myMockDataObject", itemId1.getItem());
      verifyStructuredData(getItemId(row2), "myMockDataObject", itemId2.getItem());
    } finally {
      v1Client.deleteItemsIfExist(mockItems);
    }
  }

  @Test
  public void testSQLServer() throws IOException, InterruptedException, SQLException {
    String randomId = Util.getRandomId();
    String tableName = name.getMethodName() + randomId;
    Properties config = new Properties();
    config.setProperty("db.allRecordsSql", "Select id, textField as text, intField as integer,"
        + " booleanField as boolean, floatField as double, dateField as date,"
        + " enumField as enum from " + tableName);
    config.setProperty("db.allColumns", "id, textField, intField, booleanField, floatField,"
        + " dateField, enumField");
    config.setProperty("url.columns", "id");
    config.setProperty("url.format", "http://example.com/employee/{0}");
    config.setProperty("itemMetadata.objectType", "myMockDataObject");

    String row1 = "row1" + randomId;
    String row2 = "row2" + randomId;
    List<String> query = new ArrayList<>();
    List<String> mockItems = new ArrayList<>();
    query.add("create table " + tableName + " (id varchar(32) unique not null,"
        + " textField varchar(128), intField int, booleanField bit,"
        + " floatField float, dateField date, enumField int)");
    query.add("insert into " + tableName
        + " (id, textField, intField, booleanField, floatField, dateField,"
        + " enumField) values ('" + row1 + "', 'Jones May', '2134678', 'true', '2000', "
            + "'2007-11-20', '2')");
    query.add("insert into " + tableName
        + " (id, textField, intField, booleanField, floatField, dateField,"
        + " enumField) values ('" + row2 + "', 'Joe Smith', '-9846', 'false', '12000.00',"
            + " '1987-02-28', '1')");
    DatabaseConnectionParams dbConnection = getDatabaseConnectionParams(Database.H2);
    String[] args = setupDataAndConfiguration(dbConnection, config, query);
    MockItem itemId1 = new MockItem.Builder(getItemId(row1))
        .setTitle(row1)
        .setSourceRepositoryUrl("http://example.com/employee/" + row1)
        .setContentLanguage("en-US")
        .setItemType(ItemType.CONTENT_ITEM.toString())
        .addValue("text", "Jones May")
        .addValue("integer", "2134678")
        .addValue("boolean", "true")
        .addValue("date", "2007-11-20")
        .addValue("double", "2000.00")
        .setObjectType("myMockDataObject")
        .addValue("enum", 2)
        .build();
    MockItem itemId2 = new MockItem.Builder(getItemId(row2))
        .setTitle(row2)
        .setSourceRepositoryUrl("http://example.com/employee/" + row2)
        .setContentLanguage("en-US")
        .setItemType(ItemType.CONTENT_ITEM.toString())
        .addValue("text", "Joe Smith")
        .addValue("integer", "-9846")
        .addValue("boolean", "false")
        .addValue("date", "1987-02-28")
        .addValue("double", "12000.00")
        .setObjectType("myMockDataObject")
        .addValue("enum", 1)
        .build();
    mockItems.addAll(asList(getItemId(row1), getItemId(row2)));
    try {
      runAwaitConnector(args);
      verifyStructuredData(getItemId(row1), "myMockDataObject", itemId1.getItem());
      verifyStructuredData(getItemId(row2), "myMockDataObject", itemId2.getItem());
    } finally {
      executeDatabaseStatement(dbConnection, ImmutableList.of("drop table " + tableName));
      v1Client.deleteItemsIfExist(mockItems);
    }
  }

  private void verifyStructuredData(String itemId, String schemaObjectType, Item expectedItem)
      throws IOException {
    testUtils.waitUntilEqual(itemId, expectedItem);
    StructuredDataHelper.assertStructuredData(v1Client.getItem(itemId),
        expectedItem, schemaObjectType);
  }

  private void runAwaitConnector(String[] args) throws InterruptedException {
    IndexingApplication dbConnector = runConnector(args);
    dbConnector.awaitTerminated();
  }

  private static IndexingApplication runConnector(String[] args) throws InterruptedException {
    IndexingApplication dbConnector = new IndexingApplication.Builder(
        new FullTraversalConnector(new DatabaseRepository()), args)
        .build();
    dbConnector.start();
    return dbConnector;
  }

  private String getItemId(String name) {
    return Util.getItemId(indexingSourceId, name);
  }
}