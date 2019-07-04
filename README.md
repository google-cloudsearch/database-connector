# Google Cloud Search Database Connector

The Google Cloud Search Database Connector enables indexing content from any SQL database with a 
JDBC 4.0 (or later compliant driver) with support for ACLs and change & delete detection.

## Build instructions


1. Build the connector

   a. Clone the connector repository from GitHub:
      ```
      git clone https://github.com/google-cloudsearch/database-connector.git
      cd database-connector
      ```

   b. Checkout the desired version of the connector and build the ZIP file:
      ```
      git checkout tags/v1-0.0.4
      mvn package
      ```
      (To skip the tests when building the connector, use `mvn package -DskipTests`)

2. Run the connector
   ```
   java \
      -cp "target/google-cloudsearch-database-connector-v1-0.0.4.jar:mysql-connector-java-5.1.41-bin.jar" \
      com.google.enterprise.cloudsearch.database.DatabaseFullTraversalConnector \
      -Dconfig=mysql.config
   ```

   Where `mysql-connector-java-5.1.41-bin.jar` is the JDBC 4.0 driver for the database being used,
   and `mysql.config` is the configuration file containing the parameters for the connector
   execution.

   **Note:** If the configuration file is not specified, a default file name
   `connector-config.properties` will be assumed. Refer to
   [configuration documentation](https://developers.google.com/cloud-search/docs/guides/database-connector#configureDB)
   for specifics and parameter details.

4. Install the connector

   To install the connector for testing or production, copy the ZIP file from the
   target directory to the desired machine and unzip it in the desired directory.

## Running integration tests

The integration tests check the correct interoperability between the database connector and the
Cloud Search APIs. They are run using the Failsafe plug-in and follow its naming conventions (i.e.,
they are suffixed with "IT").

To run them, issue the command

```
mvn verify \
    -DskipITs=false \
    -Dapi.test.serviceAccountPrivateKeyFile=<PATH_TO_SERVICE_ACCOUNT_KEY> \
    -Dapi.test.sourceId=<DATA_SOURCE_ID>
```

where

- `api.test.serviceAccountKey` is the path to JSON file containing the credentials of a service
  account that can access the APIs.

- `api.test.sourceId` is the ID of the data source to which the test will sync
  the data.

The following is an example of a complete command:

```
mvn verify \
    -DskipITs=false \
    -Dapi.test.serviceAccountPrivateKeyFile=key.json \
    -Dapi.test.sourceId=01cb7dfca117fbb591360a2b6e46912e
```

For further information on configuration and deployment of this connector, see
[Deploy a Database Connector](https://developers.google.com/cloud-search/docs/guides/database-connector).
