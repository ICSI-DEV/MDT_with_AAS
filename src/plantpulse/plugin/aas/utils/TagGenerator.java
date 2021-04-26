package plantpulse.plugin.aas.utils;

import java.io.IOException;
import java.net.URLEncoder;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.json.JSONArray;
import org.json.JSONObject;


public class TagGenerator {
	
	private static final Log log = LogFactory.getLog(TagGenerator.class);
	
	private static final String VTAG_ADD_PATH = "/api/v1/vopc/tag/add";
	
	public static void generate(JSONObject config, JSONArray array){
		int count = 0;
		for(int i=0; i < array.size(); i++){
			JSONObject tag = array.getJSONObject(i);
			try {
				//
				tag.put("interval", "1000");
				tag.put("_api_key", config.getString("api_key"));
				tag.put("description", URLEncoder.encode(tag.getString("description"), "UTF-8"));
				//
				String url =  config.getString("http_url") +  VTAG_ADD_PATH;
				String result = RestUtils.post(url, tag.toString());
				log.info("Tag [" + tag.getString("tag_name")  + "] RESULT = [" + result + "]");
			} catch (IOException e) {
				log.warn("Tag generate error : " + tag.toString(), e);
			}
			count++;
		}
		log.info("TAG : COUNT=[" + count + "]");
	}

}
