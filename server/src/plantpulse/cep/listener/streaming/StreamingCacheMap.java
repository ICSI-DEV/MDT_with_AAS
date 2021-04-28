package plantpulse.cep.listener.streaming;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

/**
 * StreamingCacheMap
 * 
 * @author lsb
 *
 */
@SuppressWarnings({ "rawtypes", "unchecked" })
public class StreamingCacheMap {

	private static final Log log = LogFactory.getLog(StreamingCacheMap.class);

	private static StreamingCacheMap instance = new StreamingCacheMap();

	private final Map<String, List<Map<String, Object>>> cacheMap = Collections.synchronizedMap(new HashMap<String, List<Map<String, Object>>>());

	private final int LIMIT_SIZE = 100;

	private StreamingCacheMap() {

	}

	public static StreamingCacheMap getInstance() {
		if (instance == null)
			instance = new StreamingCacheMap();
		return instance;
	}

	public void addFirstMap(String id, String label, String[] values) {
		List list = null;
		if (cacheMap.containsKey(id)) {
			list = cacheMap.get(id);
		} else {
			list = Collections.synchronizedList(new LinkedList());
			log.info("Create streaming url : id=" + id);
		}
		//
		Map data = new HashMap();
		data.put("label", label);
		data.put("value", values);
		list.add(0, data);
		if (list.size() > LIMIT_SIZE) {
			list.remove(LIMIT_SIZE);
		}
		cacheMap.put(id, list);
	}

	public Map getFirstMap(String id) {
		List list = cacheMap.get(id);
		if (list == null)
			return null;
		return (Map) list.get(0);
	}

	public List getHistoryList(String id) {
		List newList = Collections.synchronizedList(new ArrayList());
		List list = cacheMap.get(id);
		if (list == null)
			return null;
		for (int i = 1; i < list.size(); i++) {
			newList.add(list.get(i));
		}
		return newList;
	}

	public Map<String, List<Map<String, Object>>> getCacheMap() {
		return cacheMap;
	}

}
