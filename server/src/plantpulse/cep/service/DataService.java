package plantpulse.cep.service;

import java.util.List;
import java.util.Map;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.json.JSONArray;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import plantpulse.cep.service.client.StorageClient;
import plantpulse.domain.Tag;

@Service
public class DataService {

	private static final Log log = LogFactory.getLog(DataService.class);

	@Autowired
	private StorageClient storage_client;

	public JSONArray selectTagDataList(Tag tag, String from, String to, int limit, String condition) throws Exception {
		try {
			List<Map<String, Object>> list =  storage_client.forSelect().selectPointList(tag, from, to, limit, condition);
			return JSONArray.fromObject(list);
		} catch (Exception e) {
			log.error("Select tag data list error : " + e.getMessage(), e);
			throw e;
		}
	}

}
