package plantpulse.server.mvc;

import java.util.HashMap;
import java.util.Map;

import plantpulse.json.JSONObject;

/**
 * JSONResult
 * 
 * @author lenovo
 *
 */
public class JSONResult {

	public static final String SUCCESS = "SUCCESS";
	public static final String WARNING = "WARNING";
	public static final String ERROR = "ERROR";

	private String status;
	private String message;
	private Map<String, Object> data = new HashMap<String, Object>();

	public JSONResult() {

	}

	public JSONResult(String status, String message) {
		super();
		this.status = status;
		this.message = message;
	}

	public JSONResult(String status, String message, Map<String, Object> data) {
		super();
		this.status = status;
		this.message = message;
		this.data = data;
	}

	public String getStatus() {
		return status;
	}

	public void setStatus(String status) {
		this.status = status;
	}

	public String getMessage() {
		return message;
	}

	public void setMessage(String message) {
		this.message = message;
	}

	public void setDataValue(String key, Object value) {
		this.data.put(key, value);
	}

	public Map<String, Object> getData() {
		return data;
	}

	public void setData(Map<String, Object> data) {
		this.data = data;
	}
	
	public String toString() {
		return JSONObject.fromObject(this).toString();
	}

}
