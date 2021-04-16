package plantpulse.cep.engine.messaging.listener.kafka;

import java.util.HashMap;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.json.JSONObject;

import plantpulse.cep.domain.BLOB;
import plantpulse.cep.engine.config.ConfigurationManager;
import plantpulse.cep.service.client.StorageClient;


/**
 * KafkaBLOBProcessor
 * @author leesa
 *
 */
public class KafkaBLOBProcessor {
	
	private static final Log log = LogFactory.getLog(KafkaBLOBProcessor.class);
	
	private StorageClient client = new StorageClient();
    
	
	public void process(String key, byte[] value) throws Exception {
		try {
    		//
			JSONObject meta = JSONObject.fromObject(key);
		
			String api_key = ConfigurationManager.getInstance().getApplication_properties().getProperty("api.key");
			if(!api_key.equals(meta.getString("_api_key"))){
				throw new Exception("[_api_key] this is an unauthenticated message with an invalid value."); 
			};
			
			//log.error(meta.toString());
			
			//카산드라에 저장
			BLOB blob = new BLOB(
					meta.getString("tag_id"),
					meta.getLong("timestamp"),
					value, 
					meta.getString("_file_path"),
					meta.getLong("_file_size"),
					meta.getString("_mime_type"),
					new HashMap<String,String>(meta.getJSONObject("attribute")));
			
			
			client.forInsert().insertBLOB(blob);
			
		} catch (Exception e) {
			log.error("Kafka BLOB processing error : " + e.getMessage(), e);
			throw e;
		}
		
	}

}
