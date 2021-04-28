package plantpulse.cep.engine.utils;

import java.io.FileOutputStream;
import java.io.OutputStreamWriter;
import java.util.ArrayList;
import java.util.Map;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import au.com.bytecode.opencsv.CSVWriter;
import plantpulse.cep.dao.TagDAO;

public class CSVUtils {

	private static final Log log = LogFactory.getLog(CSVUtils.class);

	/**
	 * write
	 * 
	 * @param list
	 * @param file_path
	 */
	public static void writeForSnapshot(String[] header_array, ArrayList<Map<String, Object>> list, String file_path) {
		try {
			CSVWriter cw = null;
			try {
				cw = new CSVWriter(new OutputStreamWriter(new FileOutputStream(file_path), "UTF-8"));

				// 헤더 작성
				TagDAO tag_dao = new TagDAO();
				String[] tag_names = new String[header_array.length];
				for (int h = 0; h < header_array.length; h++) {
					if (h == 0) {
						tag_names[h] = "TIMESTAMP";
					} else {
						tag_names[h] = tag_dao.selectTag(header_array[h]).getTag_name();
					}
				}
				cw.writeNext(tag_names);

				// 데이터 작성
				for (Map<String, Object> m : list) {
					String[] values = new String[header_array.length];
					for (int i = 0; i < header_array.length; i++) {
						Object value = m.get(header_array[i]);
						if (value == null) {
							values[i] = "";
						} else {
							values[i] = String.valueOf(m.get(header_array[i]));
						}
					}
					cw.writeNext(values);
				}
				;
				log.info("CVS File write completed : file_path=[" + file_path + "], header_array=[" + header_array.length + "], row=[" + list.size() + "]");
			} catch (Exception e) {
				throw e;
			} finally {
				cw.close();
			}
		} catch (Exception e) {
			log.error("CSV File write error : " + e.getMessage(), e);
		}
	}
	
	
	public static void writeForData(String[] header_array, ArrayList<Map<String, Object>> list, String file_path) {
		try {
			CSVWriter cw = null;
			try {
				//
				cw = new CSVWriter(new OutputStreamWriter(new FileOutputStream(file_path), "UTF-8"));

				// 헤더 작성 (대문자)
				String[] header_array_upper = new String[header_array.length];
				for(int h=0; h < header_array.length; h++){
					header_array_upper[h] = header_array[h].toUpperCase();
				}
				cw.writeNext(header_array_upper);

				// 데이터 작성
				for (Map<String, Object> m : list) {
					String[] values = new String[header_array.length];
					for (int i = 0; i < header_array.length; i++) {
						Object value = m.get(header_array[i]);
						if (value == null) {
							values[i] = "";
						} else {
							values[i] = String.valueOf(m.get(header_array[i]));
						}
					}
					cw.writeNext(values);
				}
				;
				log.info("CVS File write completed : file_path=[" + file_path + "], header_array=[" + header_array.length + "], row=[" + list.size() + "]");
			} catch (Exception e) {
				throw e;
			} finally {
				cw.close();
			}
		} catch (Exception e) {
			log.error("CSV File write error : " + e.getMessage(), e);
		}
	}
}