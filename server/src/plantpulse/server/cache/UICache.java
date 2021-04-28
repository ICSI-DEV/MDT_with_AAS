package plantpulse.server.cache;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectOutputStream;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.json.JSONObject;

/**
 * UICache
 * 
 * @param <String>
 * @param <JSONObject>
 */
public class UICache {
	
	private static final Log log = LogFactory.getLog(UICache.class);

	private static class UICacheHolder {
		static UICache instance = new UICache();
	}

	public static UICache getInstance() {
		return UICacheHolder.instance;
	};

	private Map<String, JSONObject> json_cache    = new ConcurrentHashMap<String,JSONObject>();
	private Map<String, Object> object_cache = new ConcurrentHashMap<String,Object>();
	
	public UICache() {
		
	};
	
	public void putJSON(String key, JSONObject value) {
		log.info("UICache JSON cached : key = [" + key + "]");
		json_cache.put(key, value);
		info();
	}
	
	public boolean hasJSON(String key) {
		return (json_cache.get(key) == null) ? false : true;
	};
	
	
	public  JSONObject getJSON(String key) {
		return json_cache.get(key);
	};
	
	//-------------------------------------------------------------------------------------------
	public void putObject(String key, Object value) {
		log.info("UICache Object cached : key = [" + key + "], class_name=["  + value.getClass().getName() + "]");
		object_cache.put(key, value);
		info();
	}
	
	public boolean hasObject(String key) {
		return (object_cache.get(key) == null) ? false : true;
	};
	
	
	public Object getObject(String key) {
		return object_cache.get(key);
	};
	//-------------------------------------------------------------------------------------------
	
	public  void cleanUp() {
		json_cache =  new ConcurrentHashMap<String,JSONObject>();
		object_cache =  new ConcurrentHashMap<String,Object>();
		log.info("UICache clean up.");
		info();
	};


	public  Map<String, JSONObject> getMap() {
		return json_cache;
	};
	
	public void info() {
		ByteArrayOutputStream baos=new ByteArrayOutputStream();
        ObjectOutputStream oos = null;
	    try {
	    	oos = new ObjectOutputStream(baos);
	    	oos.writeObject(json_cache);
	        log.info("UICache info : json_size=[" + json_cache.size() + "], object_size=[" + object_cache.size() + "], data_size=[" + baos.size() + "]bytes");
	    } catch(IOException e){
	        log.error(e);
	    } finally {
	    	try {
				oos.close();
			} catch (IOException e) {
				e.printStackTrace();
			}
	    }
	}

}
