package plantpulse.cep.service;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.json.JSONArray;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import plantpulse.cep.service.client.StorageClient;

@Service
public class LogService {

	private static final Log log = LogFactory.getLog(LogService.class);

	@Autowired
	private StorageClient storage_client;

	public JSONArray selectLogList(String from, String  to, String app_name,  String  level, String  message, int limit) throws Exception {
		try {
			return storage_client.forSelect().selectSystemLogList(from, to, app_name, level, message, limit);
		} catch (Exception e) {
			log.error("Select system log list error : " + e.getMessage(), e);
			throw e;
		}
	}

}
