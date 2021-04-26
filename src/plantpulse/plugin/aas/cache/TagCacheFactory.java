package plantpulse.plugin.aas.cache;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import plantpulse.domain.Tag;

public class TagCacheFactory {


	private static class TagCacheFactoryHolder {
		static TagCacheFactory instance = new TagCacheFactory();
	}

	public static TagCacheFactory getInstance() {
		return TagCacheFactoryHolder.instance;
	}

    public  Map<String, Tag> map = new ConcurrentHashMap<>();

    public void put(String string, Tag obj) {
        map.put(string, obj);
    }

    public Map<String, Tag> getMap(){
    	return map;
    }

    public Tag get(String str) {
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
