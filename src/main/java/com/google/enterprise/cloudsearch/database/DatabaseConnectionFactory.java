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
import static com.google.common.base.Preconditions.checkState;
import static java.sql.DriverManager.getConnection;

import com.google.api.client.repackaged.com.google.common.annotations.VisibleForTesting;
import com.google.enterprise.cloudsearch.sdk.InvalidConfigurationException;
import com.google.enterprise.cloudsearch.sdk.config.Configuration;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * Standard connection factory for accessing a JDBC database.
 *
 * <p>The connection Url is required. The driver name is optional if the JDBC is installed properly
 * on modern systems. The user/password are optional if the system does not require them. If
 * optional parameters are omitted, then appropriate exceptions should be generated if they are in
 * fact required.</p>
 *
 * <ul>
 *   <li>"db.driverClass" - The JDBC driver for the connector.</li>
 *   <li>"db.url" - The database URL.</li>
 *   <li>"db.user" - The database user that the connector uses to query the database.</li>
 *   <li>"db.password" - The password for the database user that the connector uses to query the
 *   database.</li>
 * </ul>
 */
class DatabaseConnectionFactory implements ConnectionFactory {

  @VisibleForTesting
  static final String DB_DRIVERCLASS = "db.driverClass";
  static final String DB_URL = "db.url";
  static final String DB_USER = "db.user";
  static final String DB_PASSWORD = "db.password";

  private static final Logger logger = Logger.getLogger(DatabaseConnectionFactory.class.getName());

  private final String connectionUrl;
  private final String dbUser;
  private final String dbPassword;

  DatabaseConnectionFactory() {
    checkState(Configuration.isInitialized(), "configuration not initialized");
    connectionUrl = Configuration.getString(DB_URL, null).get().trim();
    checkArgument(!connectionUrl.isEmpty(), "Database connection Url cannot be empty.");
    dbUser = Configuration.getString(DB_USER, "").get().trim();
    dbPassword = Configuration.getString(DB_PASSWORD, "").get().trim();
    String driverName = Configuration.getString(DB_DRIVERCLASS, "").get().trim();
    if (!driverName.isEmpty()) {
      try {
        Class.forName(driverName);
        logger.log(Level.INFO, "Database driver successfully loaded: [{0}]", driverName);
      } catch (ClassNotFoundException e) {
        throw new InvalidConfigurationException("Error loading database driver: " + driverName, e);
      }
    }
  }

  @Override
  public Connection createConnection() throws SQLException {
    if (dbUser.isEmpty()) {
      return getConnection(connectionUrl);
    } else {
      return getConnection(connectionUrl, dbUser, dbPassword);
    }
  }

  @Override
  public void releaseConnection(Connection connection) throws SQLException {
    if (connection != null) {
      connection.close();
    }
  }
}