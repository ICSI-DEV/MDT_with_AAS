package plantpulse.cep.service;

import java.util.List;
import java.util.Map;
import java.util.UUID;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.json.JSONArray;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import plantpulse.cep.service.client.StorageClient;

@Service
public class BLOBService {

	private static final Log log = LogFactory.getLog(BLOBService.class);


	@Autowired
	private StorageClient storage_client;


	public BLOBService() {
		
	};
	
	
	public JSONArray selectBLOBList(String tag_id, String from, String to, int limit) throws Exception {
		try {
			List<Map<String, Object>> list =  storage_client.forSelect().selectBLOBList(tag_id, from, to, limit);
			return JSONArray.fromObject(list);
		} catch (Exception e) {
			log.error("Select BLOB data list error : " + e.getMessage(), e);
			throw e;
		}
	}
	
	public Map<String, Object> selectBLOBByObjectId(UUID object_id) throws Exception {
		try {
			Map<String, Object> map =  storage_client.forSelect().selectBLOBByObjectId(object_id);
			return map;
		} catch (Exception e) {
			log.error("Select BLOB object by object_id error : " + e.getMessage(), e);
			throw e;
		}
	}
	
	public Map<String, Object> selectBLOBObject(UUID object_id) throws Exception {
		try {
			Map<String, Object> map =  storage_client.forSelect().selectBLOBObject(object_id);
			return map;
		} catch (Exception e) {
			log.error("Select BLOB object error : " + e.getMessage(), e);
			throw e;
		}
	};


}
