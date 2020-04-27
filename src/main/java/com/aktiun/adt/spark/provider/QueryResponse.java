package com.aktiun.adt.spark.provider;

import java.io.Serializable;
import java.util.List;

public class QueryResponse implements Serializable {
	
	private static final long serialVersionUID = 1L;

	private String code;
	private String message;
    private String schema;
	private List<String> data;
	public String getCode() {
		return code;
	}
	public void setCode(String code) {
		this.code = code;
	}
	public String getMessage() {
		return message;
	}
	public void setMessage(String message) {
		this.message = message;
	}
	public List<String> getData() {
		return data;
	}
	public void setData(List<String> data) {
		this.data = data;
	}
	public String getSchema() {
		return this.schema;
	}
	public void setSchema(String schema) {
		this.schema = schema;
	}
	
}
