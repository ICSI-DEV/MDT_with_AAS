package plantpulse.server.mvc.opc;

import java.util.List;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.json.JSONArray;
import org.json.JSONObject;

import plantpulse.cep.service.client.StorageClient;
import plantpulse.domain.OPC;

public class OPConnectionAndCountUtils {
	
	private static final Log log = LogFactory.getLog(OPConnectionAndCountUtils.class);
	
	public static String addPointCountAndConnectionStatus(OPC opc, List<OPC> opcList) {

		//연결 상태 및 데이터 갯수 추가  
		StorageClient client = new StorageClient(); 
		JSONArray array = new JSONArray();
		for(int i=0; opcList != null && i < opcList.size(); i++){
			OPC cur_opc = opcList.get(i);
			JSONObject json = JSONObject.fromObject(cur_opc);
	
			//1.OPC 데이터 카운트
			try {
			    long opc_point_count = client.forSelect().countPointTotalByOPC(cur_opc);
			    json.put("opc_point_count", opc_point_count);
			} catch (Exception e) {
				log.error("Point count by OPC getting exception : " + e.getMessage(), e);
			}
			//
			
			//2.연결 상태
			try {
			JSONObject opc_last_update = client.forSelect().selectOPCPointLastUpdate(cur_opc);
			long current_timesatmp = System.currentTimeMillis();
			if(opc_last_update.containsKey("last_update_timestamp") ){
				long update_timesatmp = opc_last_update.getLong("last_update_timestamp");
				long updated_ms = current_timesatmp - update_timesatmp;
				json.put("opc_con_updated_ms", updated_ms);
				json.put("opc_con_updated_mm", ((updated_ms / 1000) / 60));
				if(updated_ms >= ((60*1000) * 10)){ //10 분 이상
					json.put("opc_con_status", "WARN");
				}else if(updated_ms >= ((60*1000) * 30)){ //30분 이상
					json.put("opc_con_status", "ERROR");
				}else if(updated_ms < ((60*1000) * 10)){ //10분 이하
					json.put("opc_con_status", "OK");
				}
			}else{
				json.put("opc_con_updated_mm",  "-");
				json.put("opc_con_updated_ms", 0);
				json.put("opc_con_status",    "UNKNOWN");
			}
			
			} catch (Exception e) {
				log.error("OPC Connection status getting exception : " + e.getMessage(), e);
			}
			//
			array.add(json);
		};
		
		return array.toString();
	}
	
	

}
