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

import static com.google.api.client.util.Preconditions.checkArgument;
import static com.google.api.client.util.Preconditions.checkNotNull;
import static com.google.api.client.util.Preconditions.checkState;
import static java.nio.charset.StandardCharsets.UTF_8;

import com.google.common.base.Strings;
import com.google.common.collect.ImmutableMap;
import com.google.common.io.ByteStreams;
import com.google.common.io.CharStreams;
import java.io.IOException;
import java.io.InputStream;
import java.io.Reader;
import java.sql.Array;
import java.sql.Blob;
import java.sql.Clob;
import java.sql.Connection;
import java.sql.NClob;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.SQLXML;
import java.sql.Types;
import java.util.Arrays;
import java.util.Calendar;
import java.util.TimeZone;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * Maintains the [connection/prepared statement/result set] that are used to access the
 * database so that they can all be created and closed locally herein.
 *
 * <p>The access object can optionally be created with timestamp indicating that the query
 * should be infused with the timestamp in its "where" clause. This is used for incremental
 * update queries.
 */
/*
 * AutoCloseable for the benefit of the tests. Usually DatabaseAccess
 * is closed by the CloseableIterable wrapped around it.
 */
class DatabaseAccess implements AutoCloseable {
  private static final Logger logger = Logger.getLogger(DatabaseAccess.class.getName());

  private ConnectionFactory connectionFactory;
  private final Connection connection;
  private final PreparedStatement preparedStatement;
  private final ResultSet resultSet;
  private final ImmutableMap<String, Integer> columnTypeMap;
  private ImmutableMap<String, Object> allColumnValues;
  private final TimeZone dbTimeZone;

  private DatabaseAccess(Builder builder) throws SQLException {
    boolean initialized = false;
    logger.log(Level.INFO, "Executing Sql statement: [{0}], checkpoint: [{1}]",
        new Object[]{builder.sql, builder.checkpoint});
    this.dbTimeZone = builder.dbTimeZone;
    this.connectionFactory = builder.connectionFactory;
    try {
      connection = connectionFactory.createConnection();
      preparedStatement = connection.prepareStatement(builder.sql);
      // TODO(jlacey): Add DocCheckpoint class to fetch single records using unique key columns.
      builder.checkpoint.setParameters(preparedStatement, dbTimeZone);
      resultSet = preparedStatement.executeQuery();
      columnTypeMap = createColumnTypeMap(resultSet);
      initialized = true;
    } finally {
      if (!initialized) {
        try {
          close();
        } catch (Exception e) {
          // this means that the "catch" was executed, so the error is being thrown
        }
      }
    }
  }

  static class Builder {
    private ConnectionFactory connectionFactory;
    private String sql;
    private Checkpoint checkpoint;
    private TimeZone dbTimeZone = TimeZone.getDefault(); // default to local dbTimeZone

    Builder setConnectionFactory(ConnectionFactory connectionFactory) {
      this.connectionFactory = connectionFactory;
      return this;
    }

    Builder setSql(String sql) {
      this.sql = sql;
      return this;
    }

    Builder setCheckpoint(Checkpoint checkpoint) {
      this.checkpoint = checkpoint;
      return this;
    }

    Builder setTimezone(TimeZone dbTimeZone) {
      this.dbTimeZone = dbTimeZone;
      return this;
    }

    DatabaseAccess build() throws SQLException {
      checkNotNull(connectionFactory, "Connection factory cannot be null.");
      checkArgument(!Strings.isNullOrEmpty(sql), "Sql query cannot be null/empty.");
      checkNotNull(checkpoint, "Checkpoint cannot be null.");
      checkNotNull(dbTimeZone, "Timezone cannot be null.");
      return new DatabaseAccess(this);
    }
  }

  boolean next() throws SQLException, IOException {
    checkState(resultSet != null && !resultSet.isClosed(),
        "Result set is closed during a next().");
    if (!resultSet.next()) {
      return false;
    }
    setAllColumnValues();
    return true;
  }

  /**
   * This map will allow conversion of the column name (label) to the Sql type for use when
   * fetching the column value (e.g. a Blob column will be fetched differently than an integer).
   *
   * @param rs the result set to analyze
   * @return map of column labels to Sql type values
   * @throws SQLException on Sql errors
   */
  private ImmutableMap<String, Integer> createColumnTypeMap(ResultSet rs) throws SQLException {
    ImmutableMap.Builder<String, Integer> columnMap = ImmutableMap.builder();
    ResultSetMetaData rsMetaData = rs.getMetaData();
    for (int i = 1; i <= rsMetaData.getColumnCount(); i++) {
      columnMap.put(rsMetaData.getColumnLabel(i), rsMetaData.getColumnType(i));
    }
    return columnMap.build();
  }

  /**
   * Store the column values of the current record.
   *
   * @throws SQLException on SQL error
   */
  private void setAllColumnValues() throws SQLException, IOException {
    ImmutableMap.Builder<String, Object> builder = ImmutableMap.builder();
    for (String key : columnTypeMap.keySet()) {
      Object value = getSingleColumnValue(key);
      if (value != null) { // skip any missing values
        builder.put(key, value);
      }
    }
    allColumnValues = builder.build();
  }

  ImmutableMap<String, Object> getAllColumnValues() {
    checkState(allColumnValues != null, "Column values fetched before next().");
    return allColumnValues;
  }

  /**
   * Get a single column value.
   *
   * @param col name of the record column
   * @return the value of the specified column
   * @throws SQLException on SQL error
   */
  private Object getSingleColumnValue(String col) throws SQLException, IOException {
    if (col.equals(ColumnManager.TIMESTAMP_COLUMN)) {
      if (columnTypeMap.get(col) != Types.TIMESTAMP) {
        throw new IOException(
            "Reserved column name '" + ColumnManager.TIMESTAMP_COLUMN + "' must be a TIMESTAMP.");
      }
      return resultSet.getTimestamp(col, Calendar.getInstance(dbTimeZone));
    }

    Object value = null;
    switch (columnTypeMap.get(col)) {
      case Types.ARRAY:
        Array array = resultSet.getArray(col);
        if (array != null) {
          value = Arrays.asList((Object[]) array.getArray());
        }
        break;
      case Types.BLOB:
        Blob blob = resultSet.getBlob(col);
        if (blob != null) {
          try (InputStream lob = blob.getBinaryStream()){
            value = ByteStreams.toByteArray(lob);
          } finally {
            try {
              blob.free();
            } catch (Exception e) {
              logger.log(Level.FINEST, "Error closing BLOB: ", e);
            }
          }
        }
        break;
      case Types.CLOB:
        Clob clob = resultSet.getClob(col);
        if (clob != null) {
          try (Reader reader = clob.getCharacterStream()){
            value = CharStreams.toString(reader).getBytes(UTF_8);
          } finally {
            try {
              clob.free();
            } catch (Exception e) {
              logger.log(Level.FINEST, "Error closing CLOB: ", e);
            }
          }
        }
        break;
      case Types.NCLOB:
        NClob nclob = resultSet.getNClob(col);
        if (nclob != null) {
          try (Reader reader = nclob.getCharacterStream()){
            value = CharStreams.toString(reader).getBytes(UTF_8);
          } finally {
            try {
              nclob.free();
            } catch (Exception e) {
              logger.log(Level.FINEST, "Error closing NCLOB: ", e);
            }
          }
        }
        break;
      case Types.BINARY:
        value = resultSet.getBytes(col);
        break;
      case Types.VARBINARY: // TODO(jlacey): Use getBytes when we have coverage for LONGVARBINARY.
      case Types.LONGVARBINARY:
        try (InputStream lob = resultSet.getBinaryStream(col)) {
          if (lob != null) {
            value = ByteStreams.toByteArray(lob);
          }
        }
        break;
      case Types.SQLXML:
        SQLXML sqlxml = resultSet.getSQLXML(col);
        if (sqlxml != null) {
          try {
            value = sqlxml.toString();
          } finally {
            try {
              sqlxml.free();
            } catch (Exception e) {
              logger.log(Level.FINEST, "Error closing SQLXML: ", e);
            }
          }
        }
        break;
      case Types.REF:
      case Types.STRUCT:
      case Types.JAVA_OBJECT:
        logger.log(Level.INFO, "Column type {0,number,#} not supported, skipping column {1}",
            new Object[] {columnTypeMap.get(col), col});
        break;
      default:
        value = resultSet.getObject(col);
    }
    return value;
  }

  @Override
  public void close() {
    if (resultSet != null) {
      try {
        resultSet.close();
      } catch (SQLException e) {
        logger.log(Level.WARNING, "Error closing result set: ", e);
      }
    }
    if (preparedStatement != null) {
      try {
        preparedStatement.close();
      } catch (SQLException e) {
        logger.log(Level.WARNING, "Error closing prepared statement: ", e);
      }
    }
    if (connection != null) {
      try {
        connectionFactory.releaseConnection(connection);
      } catch (SQLException e) {
        logger.log(Level.WARNING, "Error closing database connection: ", e);
      }
    }
  }
}
