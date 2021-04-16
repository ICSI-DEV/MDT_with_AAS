package plantpulse.server.mvc.api;

import plantpulse.json.JSONArray;
import plantpulse.json.JSONObject;

public class APIResponse {
	
	private JSONObject response = new JSONObject();
	
	public static int SUCCESS = 200;
	public static int ERROR   = 500;
	
	public APIResponse(){
		//
	};
	
	public APIResponse(int code, String message, JSONObject data){
		response.put("code",    code);
		response.put("message", message);
		response.put("data",    data);
	};
	
	public void setSuccess(){
		response.put("code",  SUCCESS);
	}
	
	public void setError(String message){
		response.put("code",  ERROR);
		response.put("message", message);
	}
	
	public void setCode(int code){
		response.put("code",    code);
	}
	
	public void setMessage(String message){
		response.put("message",    message);
	}
	
	public void setData(JSONObject data){
		response.put("data",    data);
	}
	
	public void setData(JSONArray array){
		response.put("data",    array);
	}
	
	public String toString(){
		return response.toString();
	}

}
