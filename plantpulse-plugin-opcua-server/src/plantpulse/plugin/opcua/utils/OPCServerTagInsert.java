package plantpulse.plugin.opcua.utils;

import java.io.File;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import plantpulse.json.JSONArray;
import plantpulse.plugin.opcua.config.CSVLoader;


public class OPCServerTagInsert {

	private static final Log log = LogFactory.getLog(OPCServerTagInsert.class);

	private String path = null;

	public void setPath(String path) {
		this.path = path;
	}

	public String getPath() {
		return path;
	}

	public JSONArray insertTagInfo(){
		JSONArray tag_list = null;
		try {
			File file = new File(path);
			tag_list = CSVLoader.readCSV(file);
			log.error("CSV File 태그데이터 " + tag_list.size() + "개 조회");
		}catch(Exception e) {
			log.error("CSV File 태그데이터 추가 실패",e);
		}

		return tag_list;
	}




}
