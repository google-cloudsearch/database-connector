# Google Cloud Search Database Connector

The Google Cloud Search Database Connector enables indexing content from any SQL database with a
JDBC 4.0 (or later compliant driver) with support for ACLs and change & delete detection.

Before running the database connector, you should review the [access control list options](https://developers.google.com/cloud-search/docs/guides/database-connector#aclOptions).



## Build instructions

1. Build the connector

   a. Clone the connector repository from GitHub:
      ```
      git clone https://github.com/google-cloudsearch/database-connector.git
      cd database-connector
      ```

   b. Checkout the desired version of the connector and build the ZIP file:
      ```
      git checkout tags/v1-0.0.5
      mvn package
      ```
      (To skip the tests when building the connector, use `mvn package -DskipTests`)


2. Install the connector

   The `mvn package` command creates a ZIP file containing the
   connector and its dependencies with a name like
   `google-cloudsearch-database-connector-v1-0.0.5.zip`.

   a. Copy this ZIP file to the location where you want to install the connector.

   b. Unzip the connector ZIP file. A directory with a name like
      `google-cloudsearch-database-connector-v1-0.0.5` will be created.

   c. Change into this directory. You should see the connector jar file,
      `google-cloudsearch-database-connector-v1-0.0.5.jar`, as well as a `lib`
      directory containing the connector's dependencies.


3. Configure the connector

   a. Create a file containing the connector configuration parameters. Refer to the
   [configuration documentation](https://developers.google.com/cloud-search/docs/guides/database-connector#configureDB)
   for specifics and for parameter details.


4. Run the connector

   The connector should be run from the unzipped installation directory, **not** the source
   code's `target` directory.

   ```
   java \
      -cp "google-cloudsearch-database-connector-v1-0.0.5.jar:mysql-connector-java-5.1.41-bin.jar" \
      com.google.enterprise.cloudsearch.database.DatabaseFullTraversalConnector \
      -Dconfig=mysql.config
   ```

   Where `mysql-connector-java-5.1.41-bin.jar` is the JDBC 4.0 driver for the database being used,
   and `mysql.config` is the configuration file containing the parameters for the connector
   execution.

   You can download the `mysql-connector-java` from this URL: (https://mvnrepository.com/artifact/mysql/mysql-connector-java). Keep the jar file in the same directory where you are keeping google-cloudsearch-database-connector jar.

   **Note:** If the configuration file is not specified, a default file name of
   `connector-config.properties` will be assumed.


For further information on configuration and deployment of this connector, see
[Deploy a Database Connector](https://developers.google.com/cloud-search/docs/guides/database-connector).

