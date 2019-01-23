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
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.io.BaseEncoding;
import com.google.enterprise.cloudsearch.sdk.config.Configuration.ResetConfigRule;
import com.google.enterprise.cloudsearch.sdk.config.Configuration.SetupConfigRule;
import java.io.IOException;
import java.sql.Clob;
import java.sql.Connection;
import java.sql.NClob;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Timestamp;
import java.util.Arrays;
import java.util.Map;
import java.util.Properties;
import java.util.Random;
import java.util.TimeZone;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

/** Tests for the DatabaseAccess class. */
public class DatabaseAccessTest {

  private static final AtomicInteger databaseCount = new AtomicInteger();

  private static String getUrl() {
    return "jdbc:h2:mem:" + databaseCount.getAndIncrement() + ";DATABASE_TO_UPPER=false";
  }

  @Rule public ExpectedException thrown = ExpectedException.none();
  @Rule public ResetConfigRule resetConfig = new ResetConfigRule();
  @Rule public SetupConfigRule setupConfig = SetupConfigRule.uninitialized();

  private InMemoryDBConnectionFactory factory;
  private Connection conn;

  @Before
  public void setUp() throws SQLException {
    Properties config = new Properties();
    config.put(DatabaseConnectionFactory.DB_URL, getUrl());
    setupConfig.initConfig(config);
    factory = new InMemoryDBConnectionFactory();
    conn = factory.createConnection();
  }

  @After
  public void tearDown() throws SQLException {
    factory.releaseConnection(conn);
    factory.shutdown();
  }

  @Test
  public void testAll() throws Exception {
    executeUpdate(conn,
        "create table testtable (id varchar(2048) unique not null, name varchar(2048))",
        "insert into testtable (id, name) values ('id1', 'Joe Smith')");

    try (DatabaseAccess databaseAccess = new DatabaseAccess.Builder()
        .setConnectionFactory(factory)
        .setSql("select * from testtable")
        .setCheckpoint(new FullCheckpoint())
        .build()) {
      assertTrue(databaseAccess.next());
      assertEquals(
          ImmutableMap.of("id", "id1", "name", "Joe Smith"),
          databaseAccess.getAllColumnValues());
    }
  }

  @Test
  public void testAll_badSqlQuery() throws Exception {
    executeUpdate(conn,
        "create table testtable (id varchar(2048) unique not null, name varchar(2048))",
        "insert into testtable (id, name) values ('id1', 'Joe Smith')");

    thrown.expect(SQLException.class);
    new DatabaseAccess.Builder()
        .setConnectionFactory(factory)
        .setSql("select * from")
        .setCheckpoint(new FullCheckpoint())
        .build();
  }

  @Test
  public void testAll_nullValue() throws Exception {
    executeUpdate(conn,
        "create table testtable (id varchar(2048) unique not null, name varchar(2048))",
        "insert into testtable (id, name) values ('id1', null)");

    try (DatabaseAccess databaseAccess = new DatabaseAccess.Builder()
        .setConnectionFactory(factory)
        .setSql("select * from testtable")
        .setCheckpoint(new FullCheckpoint())
        .build()) {
      assertTrue(databaseAccess.next());
      assertEquals(
          ImmutableMap.of("id", "id1"),
          databaseAccess.getAllColumnValues());
    }
  }

  @Test
  public void testAll_noResults() throws Exception {
    executeUpdate(conn,
        "create table testtable (id varchar(2048) unique not null, name varchar(2048))");

    try (DatabaseAccess databaseAccess = new DatabaseAccess.Builder()
        .setConnectionFactory(factory)
        .setSql("select * from testtable")
        .setCheckpoint(new FullCheckpoint())
        .build()) {
      assertFalse(databaseAccess.next());
    }
  }

  @Test
  public void testAll_offset() throws Exception {
    executeUpdate(conn,
        "create table testtable "
            + "(id varchar(2048) unique not null, name varchar(2048), modifiedtime timestamp)",
        "insert into testtable (id, name, modifiedtime) "
            + "values ('id1', 'Joe Smith', '2017-01-12 14:01:28.1')",
        "insert into testtable (id, name, modifiedtime) "
            + "values ('id2', 'Bill Jones', '2017-01-12 14:55:23.2')", // only match to return
        "insert into testtable (id, name, modifiedtime) "
            + "values ('id3', 'Mary Brown', '2017-01-12 14:32:03.6')",
        "insert into testtable (id, name, modifiedtime) "
            + "values ('id4', 'Jill Black', '2017-01-12 14:45:13.9')");

    try (DatabaseAccess databaseAccess = new DatabaseAccess.Builder()
        .setConnectionFactory(factory)
        .setSql("select id, name from testtable order by modifiedtime limit 4 offset ?")
        .setCheckpoint(new FullCheckpoint().setPagination(Pagination.OFFSET).setOffset(3))
        .build()) {
      assertTrue(databaseAccess.next());
      assertEquals(
          ImmutableMap.of("id", "id2", "name", "Bill Jones"),
          databaseAccess.getAllColumnValues());
      assertFalse(databaseAccess.next()); // and no other records
    }
  }

  @Test
  public void testChanges_typeTimestamp() throws Exception {
    executeUpdate(conn,
        "create table testtable "
            + "(id varchar(2048) unique not null, name varchar(2048), modifiedtime timestamp)",
        "insert into testtable (id, name, modifiedtime) "
            + "values ('id1', 'Joe Smith', '2017-01-12 14:01:28.1')",
        "insert into testtable (id, name, modifiedtime) "
            + "values ('id2', 'Bill Jones', '2017-01-12 14:55:23.2')", // only match to return
        "insert into testtable (id, name, modifiedtime) "
            + "values ('id3', 'Mary Brown', '2017-01-12 14:32:03.6')",
        "insert into testtable (id, name, modifiedtime) "
            + "values ('id4', 'Jill Black', '2017-01-12 14:45:13.9')");

    Timestamp sinceTime = Timestamp.valueOf("2017-01-12 14:46:02.7");
    try (DatabaseAccess databaseAccess = new DatabaseAccess.Builder()
        .setConnectionFactory(factory)
        .setSql("select id, name, modifiedtime as "
            + ColumnManager.TIMESTAMP_COLUMN
            + " from testtable where modifiedtime > ?")
        .setCheckpoint(new IncrementalCheckpoint(sinceTime.getTime()))
        .build()) {
      assertTrue(databaseAccess.next());
      assertEquals(
          ImmutableMap.of("id", "id2", "name", "Bill Jones",
              ColumnManager.TIMESTAMP_COLUMN, Timestamp.valueOf("2017-01-12 14:55:23.2")),
          databaseAccess.getAllColumnValues());
      assertFalse(databaseAccess.next()); // and no other records
    }
  }

  @Test
  public void testAll_typeTimestamp_wrongType() throws Exception {
    executeUpdate(conn,
        "create table testtable (id varchar(2048) unique not null, name varchar(2048))",
        "insert into testtable (id, name) values ('id2', 'Bill Jones')");

    try (DatabaseAccess databaseAccess = new DatabaseAccess.Builder()
        .setConnectionFactory(factory)
        .setSql("select id, name as "
            + ColumnManager.TIMESTAMP_COLUMN
            + " from testtable")
        .setCheckpoint(new FullCheckpoint())
        .build()) {
      thrown.expect(IOException.class);
      thrown.expectMessage(containsString("must be a TIMESTAMP"));
      databaseAccess.next();
    }
  }

  @Test
  public void testChanges_typeTimestamp_timezone() throws Exception {
    // Create a TimeZone instance relative to the current system time zone.
    TimeZone defaultTimeZone = TimeZone.getDefault();
    int offset = defaultTimeZone.getRawOffset(); // milliseconds to add to UTC for zone
    int offsetEarlier = offset + 2 * 60 * 60 * 1000; // two hours earlier
    TimeZone earlierTimeZone = (TimeZone) defaultTimeZone.clone();
    earlierTimeZone.setRawOffset(offsetEarlier);

    executeUpdate(conn,
        "create table testtable "
        + "(id varchar(2048) unique not null, name varchar(2048), modifiedtime timestamp)",
        "insert into testtable (id, name, modifiedtime) "
        + "values ('id1', 'Joe Smith', '2017-01-12 14:01:28.1')",
        "insert into testtable (id, name, modifiedtime) "
        + "values ('id2', 'Bill Jones', '2017-01-12 14:55:23.2')", // only match to return
        "insert into testtable (id, name, modifiedtime) "
        + "values ('id3', 'Mary Brown', '2017-01-12 14:32:03.6')",
        "insert into testtable (id, name, modifiedtime) "
        + "values ('id4', 'Jill Black', '2017-01-12 14:45:13.9')");

    Timestamp sinceTime = Timestamp.valueOf("2017-01-12 12:46:02.7");
    try (DatabaseAccess databaseAccess = new DatabaseAccess.Builder()
        .setConnectionFactory(factory)
        .setSql("select id, name, modifiedtime as " + ColumnManager.TIMESTAMP_COLUMN
            + " from testtable where modifiedtime > ?")
        .setCheckpoint(new IncrementalCheckpoint(sinceTime.getTime()))
        .setTimezone(earlierTimeZone)
        .build()) {
      assertTrue(databaseAccess.next());
      assertEquals(
          ImmutableMap.of("id", "id2", "name", "Bill Jones",
              ColumnManager.TIMESTAMP_COLUMN, Timestamp.valueOf("2017-01-12 12:55:23.2")),
          databaseAccess.getAllColumnValues());
      assertFalse(databaseAccess.next()); // and no other records
    }
  }

  @Test
  public void testChanges_zeroTimestamp() throws Exception {
    executeUpdate(conn,
        "create table testtable "
            + "(id varchar(2048) unique not null, name varchar(2048), modifiedtime timestamp)",
        "insert into testtable (id, name, modifiedtime) "
            + "values ('id1', 'Joe Smith', '2017-01-12 14:01:28.1')",
        "insert into testtable (id, name, modifiedtime) "
            + "values ('id2', 'Bill Jones', '2017-01-12 14:55:23.2')", // only match to return
        "insert into testtable (id, name, modifiedtime) "
            + "values ('id3', 'Mary Brown', '2017-01-12 14:32:03.6')",
        "insert into testtable (id, name, modifiedtime) "
            + "values ('id4', 'Jill Black', '2017-01-12 14:45:13.9')");

    Timestamp sinceTime = Timestamp.valueOf("2017-01-12 14:46:02.7");
    try (DatabaseAccess databaseAccess = new DatabaseAccess.Builder()
        .setConnectionFactory(factory)
        .setSql("select id, name, modifiedtime as "
            + ColumnManager.TIMESTAMP_COLUMN
            + " from testtable where modifiedtime > ?")
        .setCheckpoint(new IncrementalCheckpoint(0))
        .build()) {
      assertTrue(databaseAccess.next());
      assertTrue(databaseAccess.next());
      assertTrue(databaseAccess.next());
      assertTrue(databaseAccess.next());
      assertFalse(databaseAccess.next());
    }
  }

  @Test
  public void testChanges_nullCheckpoint() throws Exception {
    executeUpdate(conn,
        "create table testtable "
            + "(id varchar(2048) unique not null, name varchar(2048), modifiedtime timestamp)",
        "insert into testtable (id, name, modifiedtime) "
            + "values ('id2', 'Bill Jones', '2017-01-12 14:55:23.2')");

    DatabaseAccess.Builder databaseAccess = new DatabaseAccess.Builder()
        .setConnectionFactory(factory)
        .setSql("select id, name, modifiedtime as "
            + ColumnManager.TIMESTAMP_COLUMN
            + " from testtable where modifiedtime > ?")
        .setCheckpoint(null);

    thrown.expect(NullPointerException.class);
    databaseAccess.build();
  }

  @Test
  public void testAll_typeClob() throws Exception {
    String targetContent = "This is some clob content.";
    executeUpdate(conn,
        "create table testtable (id varchar(32) unique not null, clob_data clob)");
    Clob targetClob = conn.createClob();
    try (PreparedStatement stmt = conn.prepareStatement(
            "insert into testtable (id, clob_data) values ('id1', ?)")) {
      targetClob.setString(1, targetContent);
      stmt.setClob(1, targetClob);
      stmt.execute();
    } finally {
      targetClob.free();
    }

    try (DatabaseAccess databaseAccess = new DatabaseAccess.Builder()
        .setConnectionFactory(factory)
        .setSql("select id, clob_data from testtable")
        .setCheckpoint(new FullCheckpoint())
        .build()) {
      assertTrue(databaseAccess.next());
      Map<String, Object> content = databaseAccess.getAllColumnValues();
      assertEquals(ImmutableSet.of("id", "clob_data"), content.keySet());
      assertEquals("id1", content.get("id")); // check this record only
      assertEquals(targetContent, new String((byte[]) content.get("clob_data"), UTF_8));
      assertFalse(databaseAccess.next()); // and no other records
    }
  }

  @Test
  public void testAll_typeNClob() throws Exception {
    String targetContent = "This is some nclob content.";
    executeUpdate(conn,
        "create table testtable (id varchar(32) unique not null, nclob_data nclob)");
    NClob targetClob = conn.createNClob();
    try (PreparedStatement stmt = conn.prepareStatement(
            "insert into testtable (id, nclob_data) values ('id1', ?)")) {
      targetClob.setString(1, targetContent);
      stmt.setClob(1, targetClob);
      stmt.execute();
    } finally {
      targetClob.free();
    }

    try (DatabaseAccess databaseAccess = new DatabaseAccess.Builder()
        .setConnectionFactory(factory)
        .setSql("select id, nclob_data from testtable")
        .setCheckpoint(new FullCheckpoint())
        .build()) {
      assertTrue(databaseAccess.next());
      Map<String, Object> content = databaseAccess.getAllColumnValues();
      assertEquals(ImmutableSet.of("id", "nclob_data"), content.keySet());
      assertEquals("id1", content.get("id")); // check this record only
      assertEquals(targetContent, new String((byte[]) content.get("nclob_data"), UTF_8));
      assertFalse(databaseAccess.next()); // and no other records
    }
  }

  @Test
  public void testAll_typeVarbinary() throws Exception {
    byte[] binaryData = new byte[123];
    new Random().nextBytes(binaryData);
    executeUpdate(conn,
        "create table testtable (id varchar(32) unique not null, data varbinary(128))");
    try (PreparedStatement stmt = conn.prepareStatement(
            "insert into testtable (id, data) values ('id1', ?)")) {
      stmt.setObject(1, binaryData);
      stmt.execute();
    }

    try (DatabaseAccess databaseAccess = new DatabaseAccess.Builder()
        .setConnectionFactory(factory)
        .setSql("select id, data from testtable")
        .setCheckpoint(new FullCheckpoint())
        .build()) {
      assertTrue(databaseAccess.next());
      Map<String, Object> content = databaseAccess.getAllColumnValues();
      assertEquals(ImmutableSet.of("id", "data"), content.keySet());
      assertEquals("id1", content.get("id")); // check this record only
      String expected = BaseEncoding.base64().encode(binaryData);
      String actual = BaseEncoding.base64().encode((byte[]) content.get("data"));
      assertEquals(expected, actual);
      assertFalse(databaseAccess.next()); // and no other records
    }
  }

  @Test
  public void testAll_typeArray() throws Exception {
    executeUpdate(conn,
        "create table testtable (id varchar(32) unique not null, data array)",
        "insert into testtable (id, data) values ('id1', ('joe', 'smith', 'jsmith'))");

    try (DatabaseAccess databaseAccess = new DatabaseAccess.Builder()
        .setConnectionFactory(factory)
        .setSql("select id, data from testtable")
        .setCheckpoint(new FullCheckpoint())
        .build()) {
      assertTrue(databaseAccess.next());
      assertEquals(
          ImmutableMap.of("id", "id1", "data", Arrays.asList("joe", "smith", "jsmith")),
          databaseAccess.getAllColumnValues());
      assertFalse(databaseAccess.next());
    }
  }

  @Test
  public void testAll_typeArray_null() throws Exception {
    executeUpdate(conn,
        "create table testtable (id varchar(32) unique not null, data array)",
        "insert into testtable (id) values ('id1')");

    try (DatabaseAccess databaseAccess = new DatabaseAccess.Builder()
        .setConnectionFactory(factory)
        .setSql("select id, data from testtable")
        .setCheckpoint(new FullCheckpoint())
        .build()) {
      assertTrue(databaseAccess.next());
      assertEquals(
          ImmutableMap.of("id", "id1"),
          databaseAccess.getAllColumnValues());
      assertFalse(databaseAccess.next());
    }
  }

  @Test
  public void testAll_typeArray_intValues() throws Exception {
    executeUpdate(conn,
        "create table testtable (id varchar(32) unique not null, name varchar(128), data array)",
        "insert into testtable (id, name, data) values ('id1', 'Joe Smith', (1234, 2345, 3456))");

    try (DatabaseAccess databaseAccess = new DatabaseAccess.Builder()
        .setConnectionFactory(factory)
        .setSql("select id, data from testtable")
        .setCheckpoint(new FullCheckpoint())
        .build()) {
      assertTrue(databaseAccess.next());
      assertEquals(
          ImmutableMap.of("id", "id1", "data", Arrays.asList(1234, 2345, 3456)),
          databaseAccess.getAllColumnValues());
      assertFalse(databaseAccess.next());
    }
  }

  private void executeUpdate(Connection conn, String... sqls) throws SQLException {
    try (Statement stmt = conn.createStatement()) {
      for (String sql : sqls) {
        stmt.execute(sql);
      }
    }
  }
}
