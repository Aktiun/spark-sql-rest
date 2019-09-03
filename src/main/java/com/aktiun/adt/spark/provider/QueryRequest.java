package com.aktiun.adt.spark.provider;

import java.io.Serializable;

public class QueryRequest implements Serializable {
	private static final long serialVersionUID = 1L;
	
	private String query;
	
	public String getQuery() {
		return query;
	}
	public void setQuery(String query) {
		this.query = query;
	}
	
}
