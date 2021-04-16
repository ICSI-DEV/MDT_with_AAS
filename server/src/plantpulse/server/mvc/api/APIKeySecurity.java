package plantpulse.server.mvc.api;

import org.json.JSONObject;

import plantpulse.cep.engine.config.ConfigurationManager;

abstract public class APIKeySecurity {
	
	public String getApiKey() throws Exception {
		return ConfigurationManager.getInstance().getApplication_properties().getProperty("api.key");
	}

	public boolean auth(String query) throws Exception {
		JSONObject json = JSONObject.fromObject(query);
		String _api_key = json.getString("_api_key");
		String reg_key = getApiKey() ;
		if (reg_key.equals(_api_key)) {
			return true;
		} else {
			throw new Exception("API_KEY failed to authenticate is incorrect : _api_key=[" + _api_key + "]");
		}
	}

	public boolean auth(JSONObject json) throws Exception {
		String _api_key = json.getString("_api_key");
		String reg_key = getApiKey() ;
		if (reg_key.equals(_api_key)) {
			return true;
		} else {
			throw new Exception("API_KEY failed to authenticate is incorrect : _api_key=[" + _api_key + "]");
		}
	}

}
