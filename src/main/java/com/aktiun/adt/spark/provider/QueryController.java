package com.aktiun.adt.spark.provider;

import java.util.ArrayList;
import java.util.List;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.CrossOrigin;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.ResponseBody;
import org.springframework.web.bind.annotation.RestController;

@RestController
public class QueryController {

	@Autowired
	private SparkSession sparkSession;

	@CrossOrigin
	@RequestMapping(value = "/query", method = RequestMethod.POST)
	public @ResponseBody QueryResponse query(@RequestBody QueryRequest request) {
		QueryResponse response = new QueryResponse();
		System.out.println("Query=" + request.getQuery());
		try {
			Dataset<Row> result = sparkSession.sqlContext().sql(request.getQuery());

			System.out.println("====================================Result====================================");
//			print the results one by one
			result.show();
			System.out.println("============================================================================");

			List<String> rows = result.na().fill("null").toJSON().collectAsList();
      		String schema = result.schema().json();

			response.setData(rows);
			response.setSchema(schema);
			response.setCode("200");
			response.setMessage("");
		} catch (Exception e) {
			e.printStackTrace();
			response.setCode("500");
			response.setMessage(e.getMessage());
		}
		return response;
	}

	@CrossOrigin
	@RequestMapping(value = "/tables", method = RequestMethod.GET)
	public @ResponseBody List<DataSourceInfo> tables() {
		List<DataSourceInfo> datasources = new ArrayList<DataSourceInfo>();
		String[] tableNames = sparkSession.sqlContext().tableNames();
		for (String table : tableNames) {
			String schema = "{\"type\":\"struct\",\"fields\":[]}";
			datasources.add(new DataSourceInfo(table, schema));
		}
		return datasources;
	}
	
	@CrossOrigin
	@RequestMapping(value = "/tables/{id}", method = RequestMethod.GET)
	public @ResponseBody DataSourceInfo tables(@PathVariable("id") String id) {
		Dataset<Row> df = sparkSession.sqlContext().table(id);
		String schema = df.schema().json();
		DataSourceInfo response = new DataSourceInfo(id, schema);
		return response;
	}

}
