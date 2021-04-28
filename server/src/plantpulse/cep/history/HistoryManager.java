package plantpulse.cep.history;

import java.util.List;

import plantpulse.cep.listener.push.PushCacheMap;


/**
 * HistoryManager
 * 
 * 
 * @author lsb
 *
 */
public class HistoryManager {

	/**
	 * PushHistoryList
	 * 
	 * @param url
	 * @return
	 * @throws Exception 
	 */
	public static List<String> getPushHistoryList(String url, int limit) {
		return PushCacheMap.getInstance().getHistoryList(url, limit);
	}

}
