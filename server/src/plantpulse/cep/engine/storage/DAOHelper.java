package plantpulse.cep.engine.storage;

import java.io.File;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Map;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.json.JSONArray;
import org.json.JSONObject;

import com.datastax.driver.core.BoundStatement;
import com.datastax.driver.core.Row;

import plantpulse.cep.engine.storage.insert.cassandra.CassandraColumns;

/**
 * DAOHelper
 * 
 * @author lsb
 * 
 */
abstract public class DAOHelper {

	private static final Log log = LogFactory.getLog(DAOHelper.class);

	/**
	 * TimeoutBackup failed SQL
	 * 
	 * @param table
	 * @param content
	 */
	public void backup(String table, String content) {
		try {
			String path = System.getProperty("java.io.tmpdir");
			File backup = new File(path + "/" + table + "_" + System.currentTimeMillis() + ".txt");
			backup.createNewFile();
			// FileUtils.write(backup, content);
			log.warn("TimeoutBackup successfully for failed insert sql : table=[" + table + "], file=[" + backup.getPath() + "]");
		} catch (Exception ex) {
			log.error("Table[" + table + "] backup failed : " + ex.getMessage(), ex);
		}
	}

	public static void setConvertValueForBoundStatement(BoundStatement query, String name, String txt, String java_type) throws Exception {
		try {
			//
			if (java_type.equals("int")) {
				query.setInt(name, Integer.parseInt(txt));
			} else if (java_type.equals("double")) {
				query.setDouble(name, Double.parseDouble(txt));
			} else if (java_type.equals("long")) {
				query.setLong(name, Long.parseLong(txt));
			} else if (java_type.equals("float")) {
				query.setFloat(name, Float.parseFloat(txt));
			} else if (java_type.equals("timestamp")) {
				query.setTimestamp(name, new Date(Long.parseLong(txt)));
			} else if (java_type.equals("string") || java_type.equals("varchar") || java_type.equals("text")) {
				query.setString(name, txt);
			} else if (java_type.equals("boolean")) {
				query.setBool(name, Boolean.parseBoolean(txt));
			} else {
				throw new Exception("Unsupport cassandra boundstatement java_type = [" + java_type + "]");
			}
		} catch (NumberFormatException nfe) {
			throw new Exception("Value convert to number failed : value=[" + txt + "]", nfe);
		}
	}



	/**
	 * OPCDataTypeConverter clone
	 * 
	 * @param typeName
	 * @return
	 */
	public static String getJavaType(String typeName) {

		if (typeName.equals("Integer") == true) {
			return "int";
		} else if (typeName.equals("Boolean") == true) {
			return "boolean";
		} else if (typeName.equals("Character") == true) {
			return "int";
		} else if (typeName.equals("Short") == true) {
			return "int";
		} else if (typeName.equals("Float") == true) {
			return "float";
		} else if (typeName.equals("Integer") == true) {
			return "int";
		} else if (typeName.equals("Double") == true) {
			return "double";
		} else if (typeName.equals("Long") == true) {
			return "long";
		} else if (typeName.equals("Date") == true) {
			return "timestamp";
		} else if (typeName.equals("String") == true) {
			return "string";
		} else if (typeName.equals("JIString") == true) {
			return "string";
		} else if (typeName.equals("JIUnsignedInteger") == true) {
			return "long";
		} else if (typeName.equals("JIUnsignedShort") == true) {
			return "int";
		} else if (typeName.equals("JIUnsignedByte") == true) {
			return "int";
		} else {
			return "unsupport[" + typeName + "]";
		}
	}

	public static String getFieldNameWithCondition(String typeName) {
		if (typeName.equals("Integer") == true 
				|| typeName.equals("Boolean") == true
				|| typeName.equals("Character") == true
				|| typeName.equals("Short") == true
				|| typeName.equals("Float") == true
				|| typeName.equals("Integer") == true
				||typeName.equals("Double") == true
				|| typeName.equals("Long") == true) {
			return "TO_DOUBLE(" + CassandraColumns.VALUE + ")";
		}else{
			return CassandraColumns.VALUE;
		}
	}

	public String getSamplingFunctionNameForData() {
			return "TO_DOUBLE(" + CassandraColumns.VALUE + ")";
	};
	
	public String getAggregationFunctionNameForData(String aggregation) {
		if (aggregation.equals("COUNT") == true) {
			return CassandraColumns.COUNT;
		} else if (aggregation.equals("FIRST") == true) {
			return CassandraColumns.FIRST ;
		} else if (aggregation.equals("LAST") == true) {
			return CassandraColumns.LAST ;
		} else if (aggregation.equals("MIN") == true) {
			return CassandraColumns.MIN;
		} else if (aggregation.equals("MAX") == true) {
			return CassandraColumns.MAX;
		} else if (aggregation.equals("AVG") == true) {
			return CassandraColumns.AVG;
		} else if (aggregation.equals("COUNT") == true) {
			return CassandraColumns.COUNT;
		} else if (aggregation.equals("SUM") == true) {
			return CassandraColumns.SUM;
		} else if (aggregation.equals("STDDEV") == true) {
			return CassandraColumns.STDDEV;
		} else if (aggregation.equals("MEDIAN") == true) {
			return CassandraColumns.MEDIAN;
		} else {
			return "unsupport[" + aggregation + "]";
		}
	};
	
	public String getSamplingFunctionName() {
			return "to_double("+ CassandraColumns.VALUE + ")";
	}
	
	public String getAggregationFunctionName(String aggregation) {
		if (aggregation.equals("COUNT") == true) {
			return CassandraColumns.COUNT;
		} else if (aggregation.equals("FIRST") == true) {
			return "to_double("+ CassandraColumns.FIRST + ")";
		} else if (aggregation.equals("LAST") == true) {
			return "to_double("+ CassandraColumns.LAST + ")";
		} else if (aggregation.equals("MIN") == true) {
			return CassandraColumns.MIN;
		} else if (aggregation.equals("MAX") == true) {
			return CassandraColumns.MAX;
		} else if (aggregation.equals("AVG") == true) {
			return CassandraColumns.AVG;
		} else if (aggregation.equals("COUNT") == true) {
			return CassandraColumns.COUNT;
		} else if (aggregation.equals("SUM") == true) {
			return CassandraColumns.SUM;
		} else if (aggregation.equals("STDDEV") == true) {
			return CassandraColumns.STDDEV;
		} else if (aggregation.equals("MEDIAN") == true) {
			return CassandraColumns.MEDIAN;
		} else {
			return "unsupport[" + aggregation + "]";
		}
	}

	public static void addJSONValueFromRow(Row row, JSONArray json, String java_type) throws NumberFormatException {
		String value = row.getString(CassandraColumns.VALUE);
		if (java_type.equals("Integer") == true) {
			json.add(Integer.parseInt(value));
		} else if (java_type.equals("Boolean") == true) {
			json.add(Boolean.parseBoolean(value));
		} else if (java_type.equals("Character") == true) {
			json.add(Integer.parseInt(value));
		} else if (java_type.equals("Short") == true) {
			json.add(Integer.parseInt(value));
		} else if (java_type.equals("Float") == true) {
			json.add(Float.parseFloat(value));
		} else if (java_type.equals("Double") == true) {
			json.add(Double.parseDouble(value));
		} else if (java_type.equals("Integer") == true) {
			json.add(Integer.parseInt(value));
		} else if (java_type.equals("Long") == true) {
			json.add(Long.parseLong(value));
		} else if (java_type.equals("Date") == true) {
			json.add(Long.parseLong(value));
		} else if (java_type.equals("JIString") == true || java_type.equals("String") == true) {
			json.add(value);
		} else if (java_type.equals("JIUnsignedInteger") == true) {
			json.add(Long.parseLong(value));
		} else if (java_type.equals("JIUnsignedShort") == true) {
			json.add(Integer.parseInt(value));
		} else if (java_type.equals("JIUnsignedByte") == true) {
			json.add(Integer.parseInt(value));
		} else {
			//
		}
	}

	public static void addMapValueFromRow(Row row, Map<String, Object> map, String java_type) throws NumberFormatException {
		String value = row.getString(CassandraColumns.VALUE);
		if (java_type.equals("Integer") == true) {
			map.put("value", Integer.parseInt(value));
		} else if (java_type.equals("Boolean") == true) {
			map.put("value", Boolean.parseBoolean(value));
		} else if (java_type.equals("Character") == true) {
			map.put("value", Integer.parseInt(value));
		} else if (java_type.equals("Short") == true) {
			map.put("value", Integer.parseInt(value));
		} else if (java_type.equals("Float") == true) {
			map.put("value", Float.parseFloat(value));
		} else if (java_type.equals("Double") == true) {
			map.put("value", Double.parseDouble(value));
		} else if (java_type.equals("Integer") == true) {
			map.put("value", Integer.parseInt(value));
		} else if (java_type.equals("Long") == true) {
			map.put("value", Long.parseLong(value));
		} else if (java_type.equals("Date") == true) {
			map.put("value", Long.parseLong(value));
		} else if (java_type.equals("JIString") == true || java_type.equals("String") == true) {
			map.put("value", value);
		} else if (java_type.equals("JIUnsignedInteger") == true) {
			map.put("value", Long.parseLong(value));
		} else if (java_type.equals("JIUnsignedShort") == true) {
			map.put("value", Integer.parseInt(value));
		} else if (java_type.equals("JIUnsignedByte") == true) {
			map.put("value", Integer.parseInt(value));
		} else {
			//
		}
	}

	public static void addJSONValueFromRow(Row row, JSONObject json, String java_type, String filed) throws NumberFormatException {
		double value = row.getDouble(filed);
		json.put(filed, value);
	}


	public String getReplacedValue(String txt, String java_type) {
		if(txt == null){
			return "null";
		}
		//
		if (java_type.equals("int") || java_type.equals("long") || java_type.equals("double") || java_type.equals("float")) {
			return "" + txt + "";
		}
		if (java_type.equals("timestamp")) {
			return "'" + txt + "'";
		}
		if (java_type.equals("boolean")) {
			return "" + txt + "";
		}
		//
		if (java_type.equals("string")) {
			if (txt.equals("true") || txt.equals("false")) {
				return "" + txt + "";
			} else {
				return "'" + txt + "'";
			}
		}
		return "'" + txt + "'";
	}

	public static String getColumnTypeForCassandra(String java_type) {
		if (java_type.equals("string") == true) {
			return "varchar";
		} else if (java_type.equals("long") == true) {
			return "bigint";
		}
		return java_type;
	}

	public static long dateStringToTimestamp(String date_str) throws ParseException {
		SimpleDateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd hh:mm:ss");
		Date parsedDate = dateFormat.parse(date_str);
		return parsedDate.getTime();
	}

}
