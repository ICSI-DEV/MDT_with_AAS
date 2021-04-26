package plantpulse.plugin.aas.utils;

import plantpulse.json.JSONObject;

public class ConstantsJSON {

	private static JSONObject config;

	public static JSONObject getConfig() {
		return config;
	}

	public static void setConfig(JSONObject config) {
		ConstantsJSON.config = config;
	}


}
