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
import com.google.common.annotations.VisibleForTesting;
import com.google.enterprise.cloudsearch.sdk.RepositoryException;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.util.Calendar;
import java.util.Map;
import java.util.TimeZone;

/** Storage for checkpoint (last update time) used by incremental updates. */
public class IncrementalCheckpoint extends Checkpoint {
  static IncrementalCheckpoint parse(byte[] payload) throws RepositoryException {
    return parse(payload, IncrementalCheckpoint.class);
  }

  @Key
  private long lastUpdateTime;

  private long traversalStartTime;

  public IncrementalCheckpoint() {
  }

  IncrementalCheckpoint(long lastUpdateTime) {
    setLastUpdateTime(lastUpdateTime);
  }

  private void setLastUpdateTime(long lastUpdateTime) {
    this.lastUpdateTime = lastUpdateTime;
  }

  @VisibleForTesting
  long getLastUpdateTime() {
    return lastUpdateTime;
  }

  void setTraversalStartTime(long traversalStartTime) {
    this.traversalStartTime = traversalStartTime;
  }

  @Override
  public void setParameters(PreparedStatement stmt, TimeZone timeZone) throws SQLException {
    stmt.setTimestamp(1, new Timestamp(lastUpdateTime), Calendar.getInstance(timeZone));
  }

  /**
   * Update the checkpoint last timestamp to the most recent but only if this column is present.
   *
   * @param allColumnValues the database record key/values from the result set
   */
  @Override
  public void updateCheckpoint(Map<String, Object> allColumnValues) {
    long recordTimestamp;
    Object recordValue = allColumnValues.get(ColumnManager.TIMESTAMP_COLUMN);
    if (recordValue == null) {
      recordTimestamp = traversalStartTime;
    } else {
      recordTimestamp = ((Timestamp) recordValue).getTime();
    }
    if (recordTimestamp > getLastUpdateTime()) {
      setLastUpdateTime(recordTimestamp);
    }
  }

  /** Incremental checkpoints are persistent across traversals. */
  @Override
  public void resetCheckpoint() {
  }

  @Override
  public boolean isPageable() {
    return false;
  }
}
