# Spark SQL over REST service

The *Spark SQL over REST* service exposes a Spark SQL local context over REST to enable querying CSV and Parquet files using [Spark SQL](http://spark.apache.org/docs/latest/sql-programming-guide.html).

## Prerequisites

This project assumes working installations of:

* JDK 1.8
* Maven 3.x

## <a name="starting"></a>Starting the Spark SQL over REST service

Go to root folder of the *Spark SQL over REST* service and run the maven goal spring-boot:run:

````
$ mvn spring-boot:run
````

Now you can open in the browser: [http://localhost:8080/tables](http://localhost:8080/tables)

You should be able to see a list of available tables.

## How to package for deployment and run executable

To build the project, run the following command

```bash
mvn clean install
```

To execute the  *Spark SQL over REST service*, run the following command

```bash
java -jar target/adt.spark.provider-0.0.1-SNAPSHOT.jar
```

## How to query CSV files

Check your application.properties file to see where you should place your CSV files.  You may see entries like the ones below:

```ini
csv.path=classpath:datasources/*.csv
#csv.path=file:///opt/datasources/*.csv
```

By default, drop your CSV file on src/main/resources/datasources/ folder.  You can also update the csv.path with the location of your choice as shown in the commented out line.  You must do this when working with the executable binary.

### Important points about your CSV files

* They MUST include a Header with the name for the fields
* DATE columns must use the dash (-) separator and the date must be in the following format: YYYY-MM-DD (year-month-day)
* TIMESTAMP columns must use a dash (-) separator for the date portion of the timestamp, and the date must be in the following format: YYYY-MM-DD (year-month-day). The hh:mm:ss (hour-minute-second) portion of the timestamp must use a colon (:) separator.

## How to query Parquet files

First, you need to tell the service to load parquet files by updating your application properties file as shown below:

```ini
parquet.path=file:///opt/datasources/table1/partition1/,file:///opt/datasources/table1/partition2/,file:///opt/datasources/table2/partition1/
parquet.tablenames=table1,table2
```

Note that you should configure the paths where your parquet files are located as well as the table names to use for those paths as shown in the example above. If a path contains a table name, it is loaded as part of that table.

## Load data direct from Amazon S3 buckets

You can load the data directly from Amazon S3 buckets if instead of file do you use the s3a protocol on the data source URL. For example.

```ini
csv.path=s3a://aktiun-data/*.csv
parquet.path=s3a://aktiun-data/DA_IRS_1989_2016_State_Zip_County_AllNoAGI_geo_pivot/geolevel\=1/,s3a://aktiun-data/DA_IRS_1989_2016_State_Zip_County_AllNoAGI_geo_pivot/geolevel\=6/,s3a://aktiun-data/DA_IRS_1989_2016_State_Zip_County_AllNoAGI_geo_pivot/geolevel\=8/
parquet.tablenames=DA_IRS_1989_2016_State_Zip_County_AllNoAGI_geo_pivot
```

### Amazon S3 autentication

Note that do you have to avoid putting your Amazon S3 credentials on any file that could be committed to a public repository so do you can pass your credentials as parameters in the following way:

```bash
java -Daws.accesskey=YOUR_ACCESS_KEY -Daws.secretkey=YOUR_SECRET_KEY -jar adt.spark.provider-0.0.1-SNAPSHOT.jar
```

## How to test

Using [ChartFactor Studio](https://chartfactor.com/studio), you can use the Spark SQL provider and URL http://localhost:8080 (no need to provide an OAuth Client key) to visualize your data files.
