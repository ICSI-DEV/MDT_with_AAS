package plantpulse.plugin.aas.config;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;

import org.apache.commons.io.FileUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import plantpulse.json.JSONObject;


public class JSONConfigLoader {

	private static final Log log = LogFactory.getLog(JSONConfigLoader.class);

	public static JSONObject load(String config_path) throws Exception {
		BufferedReader br = new BufferedReader(new FileReader(config_path));
		try {
		    StringBuilder sb = new StringBuilder();
		    String line = br.readLine();

		    while (line != null) {
		        sb.append(line);
		        sb.append(System.lineSeparator());
		        line = br.readLine();
		    }
		    return JSONObject.fromObject(sb.toString());
		}catch(Exception ex){
			log.error("JSON Configuration Load Error : " + ex.getMessage(), ex);
			throw ex;
		} finally {
		    br.close();
		}
	}

	public static JSONObject fileLoad(String config_path) throws Exception {
		try {
			File file = new File(config_path);
		    return JSONObject.fromObject(FileUtils.readFileToString(file, "UTF-8"));
		}catch(Exception ex){
			log.error("JSON Configuration Load Error : " + ex.getMessage(), ex);
			throw ex;
		}
	}



}
