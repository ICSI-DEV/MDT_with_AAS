package plantpulse.cep.listener.push;

import java.util.List;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import plantpulse.cep.service.client.StorageClient;

/**
 * PushCacheMap
 * 
 * TODO Guava? Cassandra 를 통해 Time-to-expire 로직으로 변경. Using TTL
 * 
 * @author lsb
 *
 */

public class PushCacheMap {

	private static final Log log = LogFactory.getLog(PushCacheMap.class);

	private static class PushCacheMapHolder {
		static PushCacheMap instance = new PushCacheMap();
	}

	public static PushCacheMap getInstance() {
		return PushCacheMapHolder.instance;
	};
	
	private StorageClient client = new StorageClient();

	public void add(String url, String json) {
		try {
			if(!url.startsWith("/data/pointmap")){
				client.forInsert().insertListenerPushCache(System.currentTimeMillis(), url, json);
			};
		} catch (Exception e) {
			log.error("Add push json cache failed : " +  e.getMessage(), e);
		}
	}

	public List<String> getHistoryList(String url, int limit)  {
		 try {
			return client.forSelect().selectListenerPushCacheList(url, limit);
		} catch (Exception e) {
			log.error("Getting Push json cache failed : " +  e.getMessage(), e);
			return null;
		}
	}


}
