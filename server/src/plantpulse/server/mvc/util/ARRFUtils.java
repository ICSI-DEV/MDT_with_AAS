package plantpulse.server.mvc.util;

import java.io.File;
import java.io.IOException;

import org.apache.commons.io.FileUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.json.JSONArray;

/**
 * 머신러닝 파일 생성 유틸
 * 
 * @author leesa
 *
 */
public class ARRFUtils {
	
	private static final Log log = LogFactory.getLog(ARRFUtils.class);

	
	public static File createFileARFF(JSONArray array) throws Exception {
		File file = null;
		try {
			long start = System.currentTimeMillis();
			long row_count = array.size();

			StringBuffer buffer = new StringBuffer();
			buffer.append("@relation tag").append("\n");

			buffer.append("@attribute Value NUMERIC").append("\n");
			buffer.append("@attribute Date DATE 'yyyy-MM-dd_HH:mm:ss'").append("\n");
			
			buffer.append("@data").append("\n");

			//
			String prev_date = "";
			for (int i = 0; i < array.size(); i++) {
				JSONArray data = array.getJSONArray(i);
				String date = DateUtils.fmtARFF((Long) data.get(0));
				String value = (new Double(data.get(1).toString())).toString();
				if (!prev_date.equals(date)) {
					buffer.append("" + value + "," + date).append("\n");
					prev_date = date;
				}
			};

			//
			String file_name = "ML_TAG_POINT_" + System.currentTimeMillis() + ".arff";
			file = new File(System.getProperty("java.io.tmpdir") + "/plantpulse/" + file_name);
			FileUtils.writeStringToFile(file, buffer.toString());

			log.info("ARFF file create completed. : row_count=[" + row_count + "], exec_time=[" + (System.currentTimeMillis() - start) + "]");

		} catch (IOException e) {
			log.error("ARFF file create error : " + e.getMessage(), e);
		}

		return file;
	}

}
