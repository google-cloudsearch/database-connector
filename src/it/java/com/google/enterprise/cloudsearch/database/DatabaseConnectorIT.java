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

import static com.google.common.truth.Truth.assertThat;
import static com.google.enterprise.cloudsearch.sdk.TestProperties.SERVICE_KEY_PROPERTY_NAME;
import static com.google.enterprise.cloudsearch.sdk.TestProperties.qualifyTestProperty;

import com.google.api.services.cloudsearch.v1.model.Item;
import com.google.enterprise.cloudsearch.sdk.Util;
import com.google.enterprise.cloudsearch.sdk.config.Configuration.ResetConfigRule;
import com.google.enterprise.cloudsearch.sdk.indexing.CloudSearchService;
import com.google.enterprise.cloudsearch.sdk.indexing.DefaultAcl.DefaultAclMode;
import com.google.enterprise.cloudsearch.sdk.indexing.IndexingApplication;
import com.google.enterprise.cloudsearch.sdk.indexing.IndexingItemBuilder.ItemType;
import com.google.enterprise.cloudsearch.sdk.indexing.MockItem;
import com.google.enterprise.cloudsearch.sdk.indexing.StructuredDataHelper;
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
  private static final String DB_USER = "sa";
  private static final String DB_PASSWORD = "";
  private static String keyFilePath;
  private static String indexingSourceId;
  private static Optional<String> rootUrl;
  private static CloudSearchService v1Client;

  @Rule public ResetConfigRule resetConfig = new ResetConfigRule();
  @Rule public TemporaryFolder configFolder = new TemporaryFolder();
  @Rule public TestName name = new TestName();

  @BeforeClass
  public static void initialize() throws Exception {
    validateInputParams();
    v1Client = new CloudSearchService(keyFilePath, indexingSourceId, rootUrl);
    StructuredDataHelper.verifyMockContentDatasourceSchema(v1Client.getSchema());
  }

  private String getDBUrl() throws IOException {
   return "jdbc:h2:" + new File(configFolder.newFolder(), "integration-test").getAbsolutePath()
       + ";DATABASE_TO_UPPER=false";
  }

  private void createDatabase(String dbUrl, List<String> queryStatement)
      throws SQLException, IOException {
    // h2 will automatically create database if not available.
    try (Connection connection = DriverManager.getConnection(dbUrl, DB_USER, DB_PASSWORD);
        Statement statement = connection.createStatement()) {
      for (String query : queryStatement) {
        statement.execute(query);
      }
    }
  }

  private Properties createRequiredProperties(String dbUrl) throws IOException {
    Properties config = new Properties();
    rootUrl.ifPresent(r -> config.setProperty("api.rootUrl", r));
    config.put("api.sourceId", indexingSourceId);
    config.put("api.serviceAccountPrivateKeyFile", keyFilePath);
    config.put("connector.runOnce", "true");
    config.put("connector.checkpointDirectory",
        configFolder.newFolder().getAbsolutePath());
    config.put("db.url", dbUrl);
    config.put("db.user", DB_USER);
    config.put("db.password", DB_PASSWORD);
    config.put("db.viewUrlColumns", "id");
    config.put("db.uniqueKeyColumns", "id");
    config.put("url.columns", "id, name");
    config.put("itemMetadata.title.field", "id");
    config.put("itemMetadata.contentLanguage.defaultValue", "en-US");
    config.put("contentTemplate.db.title", "id");
    config.put("defaultAcl.mode", DefaultAclMode.FALLBACK.toString());
    config.put("defaultAcl.public", "true");
    config.put("traverse.queueTag", "mockDatabaseConnectorQueue-" + Util.getRandomId());
    return config;
  }

  private static void validateInputParams() throws IOException {
    String dataSourceId;
    Path serviceKeyPath;
    logger.log(Level.FINE, "Validating input parameters...");
    try {
      dataSourceId = System.getProperty(DATA_SOURCE_ID_PROPERTY_NAME);
      serviceKeyPath = Paths.get(System.getProperty(SERVICE_KEY_PROPERTY_NAME));
      assertThat(serviceKeyPath.toFile().exists()).isTrue();
      assertThat(dataSourceId).isNotNull();
      assertThat(dataSourceId).isNotEmpty();
      rootUrl = Optional.ofNullable(System.getProperty(ROOT_URL_PROPERTY_NAME));
    } catch (AssertionError error) {
      logger.log(Level.SEVERE,
          "Missing input parameters. Rerun the test as \"mvn verify"
              + " -DargLine=-Dapi.test.serviceAccountPrivateKeyFile=./path/to/key.json"
              + " -Dapi.test.sourceId=dataSourceId\"");
      throw error;
    }
    indexingSourceId = dataSourceId;
    keyFilePath = serviceKeyPath.toAbsolutePath().toString();
  }

  private String[] setupDataAndConfiguration(Properties additionalConfig,
      List<String> queryStatement) throws SQLException, IOException {
    String dbUrl = getDBUrl();
    createDatabase(dbUrl, queryStatement);
    Properties config = createRequiredProperties(dbUrl);
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
    config.put("db.allRecordsSql", "Select id, name, phone from " + tableName);
    config.put("db.allColumns", "id, name, phone");
    String row1 = "x1" + randomId;
    String row2 = "x2" + randomId;
    String row3 = "x3" + randomId;
    List<String> query = new ArrayList<>();
    query.add("create table " + tableName
        + "(id varchar(32) unique not null, name varchar(128), phone varchar(16))");
    query.add("insert into " + tableName + " (id, name, phone)"
        + " values ('" + row1 + "', 'Jones May', '2134')");
    query.add("insert into " + tableName + " (id, name, phone)"
        + " values ('" + row2 + "', 'Joe Smith', '9848')");
    query.add("insert into " + tableName + " (id, name, phone)"
        + " values ('" + row3 + "', 'Mike Brown', '3476')");
    String[] args = setupDataAndConfiguration(config, query);
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
    runAwaitConnector(args);
    getAndAssertItem(v1Client.getItem(getItemId(row1)), itemId1.getItem());
    getAndAssertItem(v1Client.getItem(getItemId(row2)), itemId2.getItem());
    getAndAssertItem(v1Client.getItem(getItemId(row3)), itemId3.getItem());
  }

  @Test
  public void testStructuredData() throws IOException, SQLException, InterruptedException {
    String randomId = Util.getRandomId();
    String tableName = name.getMethodName() + randomId;
    Properties config = new Properties();
    config.put("db.allRecordsSql", "Select id, textField as text, integerField as integer,"
        + " booleanField as boolean, doubleField as double, dateField as date from " + tableName);
    config.put("db.allColumns", "id, textField, integerField, booleanField, doubleField,"
        + " dateField");
    config.put("itemMetadata.objectType", "myMockDataObject");
    config.put("url.columns", "id");
    config.put("url.format", "http://mycompany.com/employee/{0}");
    String row1 = "s1" + randomId;
    String row2 = "s2" + randomId;
    String row3 = "s3" + randomId;

    List<String> query = new ArrayList<>();
    query.add("create table " + tableName + "(id varchar(32) unique not null,"
        + " textField varchar(128), integerField integer(50), booleanField boolean,"
        + " doubleField double, dateField date)");
    query.add("insert into " + tableName
        + " (id, textField, integerField, booleanField, doubleField, dateField)"
        + " values ('" + row1 + "', 'Jones May', '2134678', 'true', '2000', '2007-11-20')");
    query.add("insert into " + tableName
        + " (id, textField, integerField, booleanField, doubleField, dateField)"
        + " values ('" + row2 + "', 'Joe Smith', '-9846', 'false', '12000.00', '1987-02-28')");
    query.add("insert into " + tableName
        + " (id, textField, integerField, booleanField, doubleField, dateField)"
        + " values ('" + row3 + "', 'Mike Smith', '9358014', 'true', '-9000.00', '1940-11-11')");
    String[] args = setupDataAndConfiguration(config, query);

    MockItem itemId1 = new MockItem.Builder(getItemId(row1))
        .setTitle(row1)
        .setSourceRepositoryUrl("http://mycompany.com/employee/" + row1)
        .setContentLanguage("en-US")
        .setItemType(ItemType.CONTENT_ITEM.toString())
        .addValue("text", "Jones May")
        .addValue("integer", "2134678")
        .addValue("boolean", "true")
        .addValue("date", "2007-11-20")
        .addValue("double", "2000.00")
        .setObjectType("myMockDataObject")
        .build();
    MockItem itemId2 = new MockItem.Builder(getItemId(row2))
        .setTitle(row2)
        .setSourceRepositoryUrl("http://mycompany.com/employee/" + row2)
        .setContentLanguage("en-US")
        .setItemType(ItemType.CONTENT_ITEM.toString())
        .addValue("text", "Joe Smith")
        .addValue("integer", "-9846")
        .addValue("boolean", "false")
        .addValue("date", "1987-02-28")
        .addValue("double", "12000.00")
        .setObjectType("myMockDataObject")
        .build();
    MockItem itemId3 = new MockItem.Builder(getItemId(row3))
        .setTitle(row3)
        .setSourceRepositoryUrl("http://mycompany.com/employee/" + row3)
        .setContentLanguage("en-US")
        .setItemType(ItemType.CONTENT_ITEM.toString())
        .addValue("text", "Mike Smith")
        .addValue("integer", "9358014")
        .addValue("boolean", "true")
        .addValue("date", "1940-11-11")
        .addValue("double", "-9000.00")
        .setObjectType("myMockDataObject")
        .build();
    runAwaitConnector(args);
    verifyStructuredData(getItemId(row1), "myMockDataObject", itemId1.getItem());
    verifyStructuredData(getItemId(row2), "myMockDataObject", itemId2.getItem());
    verifyStructuredData(getItemId(row3), "myMockDataObject", itemId3.getItem());
  }

  private void verifyStructuredData(String itemId, String schemaObjectType,
      Item expectedItem) throws IOException {
    Item actualItem = v1Client.getItem(itemId);
    logger.log(Level.INFO, "Verifying actualItem: {0}", actualItem);
    StructuredDataHelper.assertStructuredData(actualItem, expectedItem, schemaObjectType);
    getAndAssertItem(actualItem, expectedItem);
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

  private void getAndAssertItem(Item actualItem, Item expectedItem) throws IOException {
    logger.log(Level.INFO, "Verifying actualItem: {0}", actualItem);
    try {
      assertThat(actualItem.getStatus().getCode()).isEqualTo("ACCEPTED");
      assertThat(actualItem.getItemType()).isEqualTo(expectedItem.getItemType());
      assertThat(actualItem.getMetadata()).isEqualTo(expectedItem.getMetadata());
      assertThat(actualItem.getName()).isEqualTo(expectedItem.getName());
    } finally {
      v1Client.deleteItem(actualItem.getName(), actualItem.getVersion());
    }
  }

  private String getItemId(String name) {
    return Util.getItemId(indexingSourceId, name);
  }
}