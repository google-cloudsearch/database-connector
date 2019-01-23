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

import java.sql.Connection;
import java.sql.SQLException;

/**
 * Create and dispose of database connections. This framework allows for tracking multiple
 * connections as well as testing reusable connections.
 */
interface ConnectionFactory {

  /**
   * Get a single connection. Typically this would be a new connection, but can also be a prior
   * connection if testing, etc.
   *
   * @return a connection
   * @throws SQLException on connection error
   */
  Connection createConnection() throws SQLException;

  /**
   * Close a connection that was acquired by {@link #createConnection()}.
   *
   * <p>May also ignore the call if testing, etc.</p>
   *
   * @param connection the connection to close
   */
  void releaseConnection(Connection connection) throws SQLException;
}
