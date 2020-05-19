package com.aktiun.adt.spark.provider;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.UnsupportedEncodingException;
import org.apache.spark.sql.types.DataType;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.Metadata;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.json.simple.JSONArray;
import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;
import java.util.HashMap;

public class CsvSchema{

	public CsvSchema() {
		super();
	}
	
    public HashMap<String, Object> GetSchema(String jsonSchema) throws org.json.simple.parser.ParseException, UnsupportedEncodingException, IOException {
      try {
        InputStream inputStream = this.getClass()
                        .getClassLoader()
                        .getResourceAsStream("datasources/" +jsonSchema + ".json");
        JSONParser jsonParser = new JSONParser();
        JSONArray fields = (JSONArray) jsonParser.parse(new InputStreamReader(inputStream, "UTF-8"));
        
        System.out.println("[----CUSTOM LOG----]:  LOADING SCHEMA FOR: " + jsonSchema + ".csv");
        return this.BuildSchema(fields, jsonSchema);
      } catch(Exception e) {
        System.out.println("[----CUSTOM LOG----]:  INCORRECT SCHEMA OR NOT FOUND FOR: " + jsonSchema + ".csv");
      }

      return null;
    }
    
   
    private HashMap<String, Object> BuildSchema(JSONArray fields, String schemaName) {       
        HashMap<String, Object> schema = new HashMap<>();
        StructType customSchema = new StructType();
        
        for (int i = 0; i < fields.size(); i++){
            JSONObject field =(JSONObject) fields.get(i);
            String name = (String) field.get("name");
            String type = (String) field.get("type");
            String format = (String) field.get("format");
            Boolean nullable = (Boolean) field.get("nullable");
            DataType dtype = this.getDataType(type);

            if (format != null) {
              String key = type + "Format";

              if(schema.containsKey(key)) {
                String lastFormat = (String) schema.get(key);

                if (!lastFormat.equals(format)) {
                  System.out.println("[----CUSTOM LOG----]:  MULTIPLE FORMAT FOR " +
                  type.toUpperCase() + " FOUND IN: " + schemaName + ".csv");
                } 
              } else {
                schema.put(key, format);
              }
            }

            StructField sfield = new StructField(name, dtype, nullable, Metadata.empty());
            customSchema = customSchema.add(sfield);
        }

        schema.put("schema", customSchema);
        return schema;
    }

	
	private DataType getDataType(String typeName) {
	    switch (typeName) {
	    case "int":
	    case "integer":
	        return DataTypes.IntegerType;
	    case "long":
	        return DataTypes.LongType;
	    case "float":
	        return DataTypes.FloatType;
	    case "boolean":
	    case "bool":
	        return DataTypes.BooleanType;
	    case "double":
	        return DataTypes.DoubleType;
	    case "string":
	        return DataTypes.StringType;
	    case "date":
	        return DataTypes.DateType;
	    case "timestamp":
	    case "datetime":
	        return DataTypes.TimestampType;
	    case "short":
	        return DataTypes.ShortType;
	    case "object":
	        return DataTypes.BinaryType;
	    default:
	    		System.out.println("[----CUSTOM LOG----]:  Using default for type: " + typeName);
	        return DataTypes.BinaryType;
	    }
	}
}
