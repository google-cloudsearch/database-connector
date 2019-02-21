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

import static com.google.common.base.Preconditions.checkState;
import static java.nio.charset.StandardCharsets.UTF_8;

import com.google.api.client.http.ByteArrayContent;
import com.google.api.services.cloudsearch.v1.model.Item;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.ArrayListMultimap;
import com.google.common.collect.Multimap;
import com.google.enterprise.cloudsearch.sdk.CheckpointCloseableIterable;
import com.google.enterprise.cloudsearch.sdk.Connector;
import com.google.enterprise.cloudsearch.sdk.InvalidConfigurationException;
import com.google.enterprise.cloudsearch.sdk.RepositoryException;
import com.google.enterprise.cloudsearch.sdk.config.Configuration;
import com.google.enterprise.cloudsearch.sdk.indexing.IndexingItemBuilder;
import com.google.enterprise.cloudsearch.sdk.indexing.IndexingItemBuilder.FieldOrValue;
import com.google.enterprise.cloudsearch.sdk.indexing.IndexingItemBuilder.ItemType;
import com.google.enterprise.cloudsearch.sdk.indexing.IndexingService.ContentFormat;
import com.google.enterprise.cloudsearch.sdk.indexing.IndexingService.RequestMode;
import com.google.enterprise.cloudsearch.sdk.indexing.template.ApiOperation;
import com.google.enterprise.cloudsearch.sdk.indexing.template.Repository;
import com.google.enterprise.cloudsearch.sdk.indexing.template.RepositoryContext;
import com.google.enterprise.cloudsearch.sdk.indexing.template.RepositoryDoc;
import java.io.IOException;
import java.sql.SQLException;
import java.util.Collection;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.concurrent.atomic.AtomicReference;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * {@link Repository} object that performs all database accesses during traversals.
 *
 * <p>All general database repository functions and access methods are here. Supports all required
 * calls from the SDK
 * {@link com.google.enterprise.cloudsearch.sdk.indexing.template.FullTraversalConnector}
 * class for both the full traversal and (optional) incremental traversal.
 *
 * <ul>
 *   <li>{@value TRAVERSE_UPDATE_MODE} - Specifies which traversal mode to use: SYNCHRONOUS or
 *       ASYNCHRONOUS (the default is to use the SDK configuration, which by default is
 *       SYNCHRONOUS).
 * </ul>
 */
class DatabaseRepository implements Repository {

  // define all database repository configuration parameters
  public static final String TRAVERSE_UPDATE_MODE = "traverse.updateMode";

  private static final Logger logger = Logger.getLogger(DatabaseRepository.class.getName());

  private ConnectionFactory connectionFactory;
  private ColumnManager columnManager;
  private RequestMode requestMode = RequestMode.SYNCHRONOUS;
  private final long startTimestamp;
  private final Helper databaseRepositoryHelper;

  DatabaseRepository() {
    this(new Helper());
  }

  /**
   * Provides a way for the connection factory (and other objects) to be injected.
   *
   * @param databaseRepositoryHelper can be mocked for testing
   */
  DatabaseRepository(Helper databaseRepositoryHelper) {
    this.databaseRepositoryHelper = databaseRepositoryHelper;
    startTimestamp = databaseRepositoryHelper.getCurrentTime();
  }

  /** Initialize objects based on configuration parameters. */
  @Override
  public void init(RepositoryContext context) throws RepositoryException {
    checkState(Configuration.isInitialized(), "configuration not initialized");
    connectionFactory = databaseRepositoryHelper.getConnectionFactory();
    columnManager = ColumnManager.fromConfiguration(context);
    requestMode =
        Configuration.getValue(TRAVERSE_UPDATE_MODE, RequestMode.UNSPECIFIED, RequestMode::valueOf)
            .get();
  }

  @Override
  public CheckpointCloseableIterable<ApiOperation> getIds(byte[] checkpoint)
      throws RepositoryException {
    throw new UnsupportedOperationException("Method not supported.");
  }

  @Override
  public ApiOperation getDoc(Item item) throws RepositoryException {
    throw new UnsupportedOperationException("Method not supported.");
  }

  @Override
  public boolean exists(Item item) throws RepositoryException {
    throw new UnsupportedOperationException("Method not supported.");
  }

  @Override
  public void close() {
    // N/A
  }

  /**
   * Get a result set iterator that supply all the records of the database.
   *
   * <p>A connection will be opened for each call. This connection will be left open throughout the
   * database operation.
   *
   * @return iterator of database records converted to docs
   * @throws RepositoryException on access errors
   */
  @Override
  public CheckpointCloseableIterable<ApiOperation> getAllDocs(byte[] checkpoint)
      throws RepositoryException {
    logger.log(Level.FINE, "Start getAllDocs, checkpoint: {0}",
        checkpoint == null ? null : new String(checkpoint, UTF_8));
    FullCheckpoint currentCheckpoint;
    if (checkpoint == null) {
      currentCheckpoint = new FullCheckpoint().setPagination(columnManager.getPagination());
    } else {
      currentCheckpoint = FullCheckpoint.parse(checkpoint);
      Pagination checkpointPagination = currentCheckpoint.getPagination();
      Pagination configPagination = columnManager.getPagination();
      if (!configPagination.equals(checkpointPagination)) {
        if (checkpointPagination.equals(Pagination.INVALID)) {
          logger.log(Level.WARNING, "Ignoring checkpoint {0}. Pagination must be one of {1}",
              new Object[] { currentCheckpoint, Pagination.VALUES });
        } else {
          logger.log(Level.INFO, "Ignoring checkpoint {0}. Configured pagination is {1}",
              new Object[] { currentCheckpoint, configPagination });
        }
        currentCheckpoint = new FullCheckpoint().setPagination(configPagination);
      }
    }

    DatabaseAccess databaseAccess;
    try {
      databaseAccess = new DatabaseAccess.Builder()
          .setConnectionFactory(connectionFactory)
          .setSql(columnManager.getAllRecordsSql())
          .setCheckpoint(currentCheckpoint)
          .build();
    } catch (SQLException e) {
      throw new RepositoryException.Builder()
          .setErrorMessage("Error with SQL query").setCause(e).build();
    }
    logger.log(Level.FINE, "End getAllDocs");
    return getRepositoryDocIterable(databaseAccess, currentCheckpoint);
  }

  /**
   * Get all changed records since last incremental update.
   *
   * <p> Create a {@link DatabaseAccess} object using the incremental update Sql query, create
   * the appropriate iterator building {@link RepositoryDoc} objects, and return an
   * {@link CheckpointCloseableIterable} object with an updated check point back to the
   * {@link Connector} for processing.
   *
   * @param checkpoint the timestamp of the last incremental update
   * @return a fully formed {@link CheckpointCloseableIterable} object or {@code null} if no
   * new updates
   * @throws RepositoryException on access errors
   */
  @Override
  public CheckpointCloseableIterable<ApiOperation> getChanges(byte[] checkpoint)
      throws RepositoryException {
    // this is a no-op if incremental updates are not defined
    if (!columnManager.isUsingIncrementalUpdates()) {
      return null;
    }

    logger.log(Level.FINE, "Start getChanges, checkpoint: {0}",
        checkpoint == null ? null : new String(checkpoint, UTF_8));
    IncrementalCheckpoint currentCheckpoint;
    if (checkpoint == null) {
      currentCheckpoint = new IncrementalCheckpoint(startTimestamp);
    } else {
      currentCheckpoint = IncrementalCheckpoint.parse(checkpoint);
    }
    currentCheckpoint.setTraversalStartTime(databaseRepositoryHelper.getCurrentTime());

    DatabaseAccess databaseAccess;
    try {
      databaseAccess = new DatabaseAccess.Builder()
          .setConnectionFactory(connectionFactory)
          .setSql(columnManager.getIncUpdateSql())
          .setCheckpoint(currentCheckpoint)
          .setTimezone(columnManager.getIncUpdateTimezone())
          .build();
    } catch (SQLException e) {
      throw new RepositoryException.Builder()
          .setErrorMessage("Error with SQL query").setCause(e).build();
    }

    logger.log(Level.FINE, "End getChanges");
    return getRepositoryDocIterable(databaseAccess, currentCheckpoint);
  }

  /**
   * Get a {@link RepositoryDoc} iterable of either "regular" or "blob" type based on configuration.
   *
   * @param databaseAccess object containing the appropriate result set
   * @param checkpoint the {@link Checkpoint} for updating with result set data
   * @return the correct type of iterable
   */
  @VisibleForTesting
  ResultSetCloseableIterable<ApiOperation> getRepositoryDocIterable(
      DatabaseAccess databaseAccess, Checkpoint checkpoint) {
    String blobColumn = columnManager.getBlobColumn();
    if (blobColumn.isEmpty()) {
      return new RepositoryDocIterable(databaseAccess, checkpoint);
    } else {
      return new RepositoryDocBlobIterable(databaseAccess, checkpoint);
    }
  }

  /**
   * General template for returning a result set iterator.
   *
   * <p>The instance method to be implemented in subclasses for defining the return objects is
   * {@link #createResultSetRecord(Map)}.
   *
   * @param <T> database record return type
   */
  private abstract static class ResultSetCloseableIterable<T>
      implements CheckpointCloseableIterable<T> {
    // TODO(jlacey): Maybe make the SDK's CloseableIterableOnce public and extend that here.

    private final DatabaseAccess access;
    private final AtomicReference<Iterator<T>> resultSetIterator;
    final Checkpoint checkpoint;
    private final boolean isPageable;
    private boolean hasMore = false;

    ResultSetCloseableIterable(DatabaseAccess access, Checkpoint checkpoint) {
      this.access = access;
      this.resultSetIterator = new AtomicReference<>(new ResultSetIterator());
      this.checkpoint = checkpoint;
      this.isPageable = checkpoint.isPageable();
    }

    @Override
    public Iterator<T> iterator() {
      Iterator<T> temp = resultSetIterator.getAndSet(null);
      if (temp == null) {
        throw new IllegalStateException("iterator is exhausted");
      }
      return temp;
    }

    /**
     * Generate a repository object from the current record of the result set.
     *
     * @param allColumnValues the database record key/values from the result set
     * @return repository object
     * @throws IOException when the repository has a record fetch error
     */
    abstract T createResultSetRecord(Map<String, Object> allColumnValues) throws IOException;

    @Override
    public byte[] getCheckpoint() {
      if (!hasMore) {
        checkpoint.resetCheckpoint();
      }
      return checkpoint.get();
    }

    @Override
    public boolean hasMore() {
      return hasMore;
    }

    /**
     * Close the result set, prepared statement and connection.
     */
    @Override
    public void close() {
      access.close();
    }

    /**
     * Note: this iterator on the result set is not thread safe.
     */
    private class ResultSetIterator implements Iterator<T> {

      boolean nextLoaded = false;

      @Override
      public boolean hasNext() {
        if (nextLoaded) {
          return true;
        }
        try {
          nextLoaded = access.next();
          if (nextLoaded && isPageable) {
            hasMore = true;
          }
        } catch (IOException | SQLException e) {
          logger.log(Level.WARNING, "Error getting next database record: ", e);
          nextLoaded = false;
        }
        return nextLoaded;
      }

      @Override
      public T next() {
        if (hasNext()) {
          try {
            return createResultSetRecord(access.getAllColumnValues());
          } catch (IOException e) {
            throw new RuntimeException("Error creating record from result set.", e);
          } finally {
            nextLoaded = false;
          }
        }
        throw new NoSuchElementException();
      }
    }
  }

  /**
   * This subclass is used for returning database records as {@link RepositoryDoc} objects,
   * (as with {@link Repository#getAllDocs()}).
   */
  private class RepositoryDocIterable extends ResultSetCloseableIterable<ApiOperation> {

    RepositoryDocIterable(DatabaseAccess access, Checkpoint checkpoint) {
      super(access, checkpoint);
    }

    /**
     * Generate a {@link RepositoryDoc} from the current record of the result set.
     *
     * @param allColumnValues the database record key/values from the result set
     * @return repository document object
     */
    @Override
    RepositoryDoc createResultSetRecord(Map<String, Object> allColumnValues) throws IOException {
      return new RepositoryDoc.Builder()
          .setItem(createItem(allColumnValues, checkpoint))
          .setContent(createContent(allColumnValues), ContentFormat.HTML)
          .setRequestMode(requestMode)
          .build();
    }
  }

  /**
   * This subclass is used for returning database records with a Blob field content as
   * {@link RepositoryDoc} objects, (as with {@link Repository#getAllDocs()}).
   */
  private class RepositoryDocBlobIterable extends ResultSetCloseableIterable<ApiOperation> {

    RepositoryDocBlobIterable(DatabaseAccess access, Checkpoint checkpoint) {
      super(access, checkpoint);
    }

    /**
     * Generate a {@link RepositoryDoc} with Blob content from the current record of the result set.
     *
     * @param allColumnValues the database record key/values from the result set
     * @return repository document object
     * @throws IOException when the repository has a document fetch error
     */
    @Override
    RepositoryDoc createResultSetRecord(Map<String, Object> allColumnValues)
        throws IOException {
      // TODO(normang): Future: if Blob is defined content columns may be used for metadata content.
      RepositoryDoc.Builder document =
          new RepositoryDoc.Builder()
              .setItem(createItem(allColumnValues, checkpoint))
              .setRequestMode(requestMode);
      ByteArrayContent content = createBlobContent(allColumnValues);
      if (content != null) {
        document.setContent(content, ContentFormat.RAW);
      }
      return document.build();
    }
  }

  /**
   * Create an {@link Item} populated with the current record.
   *
   * <p>If the passed {@link Checkpoint} is not null, then attempt to update the last timestamp
   * value with a newer one. This is used for incremental updates using a timestamp field.
   *
   * @param allColumnValues the database record key/values from the result set
   * @param checkpoint the current {@link Checkpoint} object
   * @return a fully formed {@link Item}
   */
  private Item createItem(Map<String, Object> allColumnValues, Checkpoint checkpoint) {
    if (checkpoint != null) {
      checkpoint.updateCheckpoint(allColumnValues);
    }

    Multimap<String, Object> multiMapValues = ArrayListMultimap.create();
    for (Map.Entry<String, Object> entry : allColumnValues.entrySet()) {
      Object value = entry.getValue();
      if (value instanceof Collection) {
        multiMapValues.putAll(entry.getKey(), (Collection<?>) value);
      } else {
        multiMapValues.put(entry.getKey(), value);
      }
    }
    return IndexingItemBuilder.fromConfiguration(
        UniqueKey.makeUniqueId(columnManager.getUniqueKey(), allColumnValues)) // name
        .setValues(multiMapValues)
        .setAcl(columnManager.createAclIfEnabled(allColumnValues))
        .setItemType(ItemType.CONTENT_ITEM)
        .setSourceRepositoryUrl(FieldOrValue.withValue(columnManager.getViewUrl(allColumnValues)))
        .build();
  }

  /**
   * Create an item's html content from the current record's content columns.
   *
   * @param allColumnValues the database record key/values from the result set
   * @return the record's item content
   */
  private ByteArrayContent createContent(Map<String, Object> allColumnValues) {
    ByteArrayContent content;
    Map<String, Object> keyValues = new HashMap<>();
    for (String col : columnManager.getContentSqlCols()) {
      if (allColumnValues.containsKey(col)) {
        keyValues.put(col, allColumnValues.get(col));
      }
    }
    String htmlContent = columnManager.applyContentTemplate(keyValues);
    content = databaseRepositoryHelper.getContentFromHtml(htmlContent); // allows for mocking
    return content;
  }

  /**
   * Create an item's single Blob column content from the current record.
   *
   * @param allColumnValues the database record key/values from the result set
   * @return the record's item content, or {@code null} if blob data missing
   */
  private ByteArrayContent createBlobContent(Map<String, Object> allColumnValues) {
    byte[] bytes;
    Object value = allColumnValues.get(columnManager.getBlobColumn());
    if (value == null) {
      return null;
    } else if (value instanceof String) {
      bytes = ((String) value).getBytes(UTF_8);
    } else if (value instanceof byte[]) {
      bytes = (byte[]) value;
    } else {
      throw new InvalidConfigurationException( // allow SDK to send dashboard notification
          "Invalid Blob column type. Column: " + columnManager.getBlobColumn()
          + "; object type: " + value.getClass().getSimpleName());
    }
    return new ByteArrayContent(null, bytes);
  }

  /**
   * Used to inject a connection factory (among other objects) into this repository.
   */
  @VisibleForTesting
  static class Helper {

    ConnectionFactory getConnectionFactory() {
      return new DatabaseConnectionFactory();
    }

    long getCurrentTime() {
      return System.currentTimeMillis();
    }

    ByteArrayContent getContentFromHtml(String htmlContent) {
      return new ByteArrayContent("text/html", htmlContent.getBytes(UTF_8));
    }
  }
}
