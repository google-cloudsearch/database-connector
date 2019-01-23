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

import com.google.api.client.util.Key;
import com.google.enterprise.cloudsearch.sdk.RepositoryException;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.Map;
import java.util.TimeZone;

/** Storage for checkpoint (pagination and related parameters) used by full traversals. */
public class FullCheckpoint extends Checkpoint {
  static FullCheckpoint parse(byte[] payload) throws RepositoryException {
    return parse(payload, FullCheckpoint.class);
  }

  @Key
  private String pagination;

  @Key
  private long offset;

  public FullCheckpoint() {
    pagination = Pagination.NONE.toString();
    offset = 0;
  }

  @Override
  public byte[] get() {
    if (getPagination().equals(Pagination.OFFSET)) {
      return super.get();
    } else {
      return null;
    }
  }

  FullCheckpoint setPagination(Pagination pagination) {
    this.pagination = pagination.toString();
    return this;
  }

  Pagination getPagination() {
    return Pagination.fromString(pagination);
  }

  FullCheckpoint setOffset(long offset) {
    this.offset = offset;
    return this;
  }

  long getOffset() {
    return offset;
  }

  @Override
  public void setParameters(PreparedStatement stmt, TimeZone timeZone) throws SQLException {
    switch (Pagination.fromString(pagination)) {
      case OFFSET:
        stmt.setLong(1, offset);
        break;
      default:
        // do nothing
    }
  }

  /**
   * Update the checkpoint.
   *
   * @param allColumnValues the database record key/values from the result set
   */
  @Override
  public void updateCheckpoint(Map<String, Object> allColumnValues) {
    if (getPagination().equals(Pagination.OFFSET)) {
      ++offset;
    }
  }

  /** Reset the pagination to NONE, which forces a null checkpoint byte array. */
  @Override
  public void resetCheckpoint() {
    setPagination(Pagination.NONE);
  }

  @Override
  public boolean isPageable() {
    return getPagination().equals(Pagination.OFFSET);
  }
}
