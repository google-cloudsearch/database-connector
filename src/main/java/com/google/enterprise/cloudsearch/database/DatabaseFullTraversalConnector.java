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

import com.google.enterprise.cloudsearch.sdk.indexing.IndexingApplication;
import com.google.enterprise.cloudsearch.sdk.indexing.template.FullTraversalConnector;
import java.io.IOException;

/**
 * Google Cloud Search Connector for Databases using the Full Traversal Template
 */
public class DatabaseFullTraversalConnector {

  public static void main(String[] args) throws IOException, InterruptedException {
    IndexingApplication application = new IndexingApplication.Builder(
        new FullTraversalConnector(new DatabaseRepository()), args).build();
    application.start();
  }
}
