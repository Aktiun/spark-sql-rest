package com.aktiun.adt.spark.provider;


import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.TimeZone;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.DataFrameReader;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.StructType;
import org.json.simple.parser.ParseException;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.support.PropertySourcesPlaceholderConfigurer;
import org.springframework.core.io.Resource;
import org.springframework.core.io.support.ResourcePatternResolver;

import org.springframework.beans.factory.annotation.Value;
import com.aktiun.adt.spark.provider.CsvSchema;
import com.amazonaws.auth.AWSCredentials;
import com.amazonaws.auth.AWSStaticCredentialsProvider;
import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.regions.Regions;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import com.amazonaws.services.s3.model.ObjectListing;
import com.amazonaws.services.s3.model.S3ObjectSummary;


@Configuration
public class ApplicationConfig {
	
	@Autowired
	private ResourcePatternResolver resourceResolver;

	@Value("${csv.path:classpath:datasources/*.csv}")
	private String csvPath;

	@Value("${parquet.path:classpath:datasources/*.parquet}")
	private String parquetPath;
	
	@Value("${parquet.tablenames:}")
	private String parquetTablenames;

	@Value("${aws.accesskey:}")
	private String awsAccesskey;

	@Value("${aws.secretkey:}")
	private String awsSecretkey;

	@Bean
    public SparkConf sparkConf() {
        SparkConf sparkConf = new SparkConf()
                .setAppName("Spark SQL Provider")
                .setMaster("local")
     		    .set("spark.sql.session.timeZone", "UTC")
                .set("spark.sql.inMemoryColumnarStorage.compressed", "true")
                .set("spark.sql.inMemoryColumnarStorage.batchSize", "20000")
				.set("spark.sql.shuffle.partitions","10")
				.set("spark.hadoop.fs.s3a.impl","org.apache.hadoop.fs.s3a.S3AFileSystem")
				.set("spark.hadoop.fs.s3a.access.key",awsAccesskey)
				.set("spark.hadoop.fs.s3a.secret.key",awsSecretkey);
        
        return sparkConf;
    }
	
	private CsvSchema schema = new CsvSchema();

    @Bean
    public JavaSparkContext javaSparkContext() {
        return new JavaSparkContext(sparkConf());
    }

	@Bean
    public SparkSession sparkSession() {
		// Avoid local time assumption for any time
		TimeZone.setDefault(TimeZone.getTimeZone("UTC"));
	
		// Create new Spark Session
		SparkSession sparkSession = SparkSession
				.builder()
				.sparkContext(javaSparkContext().sc())
				.appName("Spark SQL Provider")
				.getOrCreate();

		createCsvDatasets(sparkSession);
		createParquetDatasets(sparkSession);

        return sparkSession;
	}

	private void createCsvDatasets(SparkSession sparkSession) {
		if (csvPath.isBlank()) return;
		String path = csvPath.endsWith("/*.csv") ? csvPath.replace("/*.csv", "") : csvPath;

		try {
			List<String> files = new ArrayList<>();
			List<String> urls = new ArrayList<>();
			Boolean isS3 = csvPath.startsWith("s3a://");

			
			if (isS3) {
				AWSCredentials credentials = new BasicAWSCredentials(
					awsAccesskey,
					awsSecretkey
				);

				String bucket = csvPath.replace("s3a://", "").split("/")[0];
		
				AmazonS3 s3client = AmazonS3ClientBuilder
					.standard()
					.withCredentials(new AWSStaticCredentialsProvider(credentials))
					.withRegion(Regions.US_EAST_1)
					.build();

				ObjectListing objectListing = s3client.listObjects(bucket);
				for(S3ObjectSummary os : objectListing.getObjectSummaries()) {
					if (os.getKey().endsWith(".csv")) {
						files.add(os.getKey());
					}
				}
			} else {
				// get the list of all CSV files
				Resource[] resources = resourceResolver.getResources(csvPath);
				for(Resource res : resources) {
					files.add(res.getFilename());
					urls.add(res.getURL().toString());
				}
			}

			for(int i = 0; i < files.size(); i++) {
				String res = files.get(i);
				String name = res.replace(".csv", "").replace("-", "_");			
				StructType csvSchema = null;
				try {
					csvSchema = this.schema.GetSchema(name);
				} catch (ParseException e) { } 
				
				// Load CSV file
				DataFrameReader df = sparkSession.read()
						.format("com.databricks.spark.csv")
						.option("timestampFormat","yyyy-MM-dd HH:mm:ssZ")
						.option("dateFormat","yyyy-MM-dd HH:mm:ssZ")
						.option("header", true)
						.option("inferSchema", true);
				
				// Set a custom schema if there is any defined for the source
				if (csvSchema != null)
					df.schema(csvSchema);
				
				// Create table
				Dataset<Row> table = isS3 ? df.load(path + "/" + res) : df.load(urls.get(i));
				table.createOrReplaceTempView(name);
				sparkSession.sqlContext().cacheTable(name);
			}
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

	private void createParquetDatasets(SparkSession sparkSession) {
		try {
			if ((!parquetPath.isBlank() && parquetTablenames.isBlank()) ||
				(parquetPath.isBlank() && !parquetTablenames.isBlank())) {
					throw new Exception("If do you want to read parquet files the parquet.path and " +
						"parquet.tablenames configuration properties have to be defined, otherwise " +
						"left both undefined.");
				}

			if (!parquetPath.isBlank() && !parquetTablenames.isBlank()){
				List<TablePath> tablePaths = obtainTablePaths();
				for (TablePath tablePath : tablePaths) {
					Dataset<Row> table = sparkSession.sqlContext().read().parquet(tablePath.getPathArray());
					table.createOrReplaceTempView(tablePath.getName());
					sparkSession.sqlContext().cacheTable(tablePath.getName());
				}
			}
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

	private List<TablePath> obtainTablePaths() {
		List<TablePath> tablepaths = new ArrayList<TablePath>();
		List<String> paths = Arrays.asList(parquetPath.split(","));
		if (!parquetTablenames.isEmpty()) {
			List<String> tables = Arrays.asList(parquetTablenames.split(","));
			for (String tableStr : tables) {
				TablePath tablepath = new TablePath();
				tablepath.setName(tableStr);
				for (String path : paths) {
					if (path.contains(tableStr)) {
						tablepath.addToPaths(path);
					}
				}
				tablepaths.add(tablepath);
			}
		} else {
			String msg = "\n\nPlease define the parquet.tablenames property.  It is a comma separated list of table names.\n";
			msg += "Table names should be substrings of their respective file paths.\n\n";
			throw new RuntimeException(msg);
		}

		return tablepaths;
	}
	
    @Bean
    public static PropertySourcesPlaceholderConfigurer propertySourcesPlaceholderConfigurer() {
        return new PropertySourcesPlaceholderConfigurer();
    }
}
