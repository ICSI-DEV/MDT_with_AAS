package plantpulse.plugin.opcua.cache;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

public class TagCacheFactory {


	private static class TagCacheFactoryHolder {
		static TagCacheFactory instance = new TagCacheFactory();
	}

	public static TagCacheFactory getInstance() {
		return TagCacheFactoryHolder.instance;
	}

    public  Map<String, Object> map = new ConcurrentHashMap<>();

    public void put(String string, Object obj) {
        map.put(string, obj);
    }

    public Map<String, Object> getMap(){
    	return map;
    }

    public Object get(String str) {
        return map.get(str);
    }

    public void remove(String string) {
        map.remove(string);
    }

    public void removeAll() {
    	for(String key : map.keySet()){
    		map.remove(key);
    	}
    }

}
