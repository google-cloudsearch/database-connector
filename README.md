# Google Cloud Search Database Connector

The Google Cloud Search Database Connector allows for easy indexing of content from any SQL database with a JDBC 4.0 compliant driver. This includes support for access control lists (ACLs) and change & delete detection.

Before using the database connector, it's recommended to review the [access control list options](https://developers.google.com/cloud-search/docs/guides/database-connector#aclOptions). to ensure your data is being indexed properly.

## Building the Connector

To build the Google Cloud Search Database Connector, follow these steps:

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

   (If you want to skip the tests during the build process, use mvn package -DskipTests)

   The `mvn package` command creates a ZIP file containing the
   connector and its dependencies with a name like
   `google-cloudsearch-database-connector-v1-0.0.5.zip`.

2. Installing the Connector

   To install the Google Cloud Search Database Connector, follow these steps:

   a. Copy the ZIP file to the desired installation location.

   b. Unzip the connector ZIP file. This creates a directory with a name like
   `google-cloudsearch-database-connector-v1-0.0.5` will be created.

   c. Change into the newly created directory. You should see the connector JAR file,
   `google-cloudsearch-database-connector-v1-0.0.5.jar`, as well as a `lib`
   directory containing the connector's dependencies.

3. Configuring the Connector

To configure the Google Cloud Search Database Connector, follow these steps:

a. Create a file with the connector configuration parameters. Refer to the
[configuration documentation](https://developers.google.com/cloud-search/docs/guides/database-connector#configureDB)
for specifics and for parameter details.

4. Running the Connector

To run the Google Cloud Search Database Connector, use the following command:

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

**Note:** If the configuration file is not specified, a default file name of
`connector-config.properties` will be assumed.

For further information on configuration and deployment of this connector, see
[Deploy a Database Connector](https://developers.google.com/cloud-search/docs/guides/database-connector).
