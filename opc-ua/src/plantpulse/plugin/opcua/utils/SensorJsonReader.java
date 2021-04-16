package plantpulse.plugin.opcua.utils;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import plantpulse.json.JSONArray;
import plantpulse.json.JSONObject;
import plantpulse.plugin.opcua.config.JSONConfigLoader;
import plantpulse.plugin.opcua.dao.TagDAO;

/**
 * PropertiesUtils
 * @author lenovo
 *
 */
public class SensorJsonReader {

	private static final Log log = LogFactory.getLog(TagDAO.class);


	public static JSONObject getSensor(String path) throws Exception {
		JSONObject config = JSONConfigLoader.fileLoad(path);

		JSONObject sensorConfig = new JSONObject();
		JSONObject sensor = new JSONObject();
		JSONObject collector = new JSONObject();
		JSONArray temp1List;
		JSONArray temp2List;
		JSONArray fireList;
		JSONArray fluxList;
		JSONArray vibeList;
		JSONArray list;

		try {

			Constants.setLOGPATH(config.get("logpath").toString());

			JSONArray asset_arr =  config.getJSONArray("sensor_list");
			JSONArray sensor_arr;
			JSONArray eqpt_sensor;
			JSONObject obj;
			JSONObject obj2;
			for(Object o:asset_arr){

				obj = (JSONObject)o;
				eqpt_sensor = new JSONArray();

//				temp1List = new JSONArray();
//				temp2List = new JSONArray();
//				fireList = new JSONArray();
//				fluxList = new JSONArray();
//				vibeList = new JSONArray();



				sensor_arr =  obj.getJSONArray("eqpt_sensor");
				if(sensor_arr != null){
					for(Object o2:sensor_arr){
						list = new JSONArray();
						obj2 = (JSONObject)o2;
						obj2.put("timecycle", obj.get("timecycle"));
						obj2.put("company" ,config.get("company"));
						eqpt_sensor.add(obj2);
						sensor.put(obj2.get("sensor_id") ,obj2 );

						list.add(obj2);
						collector.put("COLLECTOR_"+obj.get("asset_id")+"_"+obj2.get("sensor_id"), list);
					}

				}

				obj.put("eqpt_sensor", eqpt_sensor);
			}

		}catch (Exception e) {
			log.error(e,e);
		}

		sensorConfig.put("collector",collector);
		sensorConfig.put("api",config.getJSONObject("api"));
		sensorConfig.put("tags",config.getJSONArray("tags"));
		return sensorConfig;
	}

}
