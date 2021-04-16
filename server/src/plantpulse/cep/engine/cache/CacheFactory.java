package plantpulse.cep.engine.cache;

import java.util.HashMap;
import java.util.Map;

public class CacheFactory {

	private static class CacheFactoryHolder {
		static CacheFactory instance = new CacheFactory();
	}

	public static CacheFactory getInstance() {
		return CacheFactoryHolder.instance;
	}

	private Map<String, Object> cache = new HashMap<String, Object>();

	public Object get(String key) {
		return cache.get(key);
	}

	public void set(String key, Object value) {
		this.cache.put(key, value);
	}

}
