package plantpulse.plugin.opcua.config;

import java.io.File;
import java.io.FileReader;
import java.util.List;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.supercsv.cellprocessor.Optional;
import org.supercsv.cellprocessor.ift.CellProcessor;
import org.supercsv.io.CsvListReader;
import org.supercsv.io.ICsvListReader;
import org.supercsv.prefs.CsvPreference;

import plantpulse.json.JSONArray;
import plantpulse.json.JSONObject;


public class CSVLoader {

	private static final Log log = LogFactory.getLog(CSVLoader.class);


	/**
	 *
	 * @param valueKeyArray
	 * @param inputPath
	 * @return
	 * @throws Exception
	 */
	public static JSONArray readCSV(File file) throws Exception {

		JSONArray array = new JSONArray();

		ICsvListReader listReader = null;
        try {

                listReader = new CsvListReader(new FileReader(file), CsvPreference.STANDARD_PREFERENCE);

                listReader.getHeader(true); // skip the header (can't be used with CsvListReader)
                final CellProcessor[] processors = new CellProcessor[] {
                		new Optional(),
                		new Optional(),
                		new Optional(),
                		new Optional()
                };

                List<Object> customerList;
                JSONObject tag;
                while( (customerList = listReader.read(processors)) != null ) {
                        tag = new JSONObject();
        				//기본 속성
                        tag.put("tag_id",listReader.get(1));
        				tag.put("opc_id",listReader.get(2));
        				tag.put("tag_name",listReader.get(3));
        				tag.put("type",listReader.get(4));
                        array.add(tag);
                }

        }catch(Exception e) {
        	log.error(e,e);
        }finally {
                if( listReader != null ) {
                        listReader.close();
                }
        }

		return array;
	}


}
