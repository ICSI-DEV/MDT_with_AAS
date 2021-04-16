package plantpulse.server.filter.cache;

import java.util.Map;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.eclipse.collections.impl.map.mutable.ConcurrentHashMap;


public class ComressionCacheMapFactory  {
	
	private static final Log log = LogFactory.getLog(ComressionCacheMapFactory.class);
	
	private static class ComressionCacheMapFactoryHolder {
		static ComressionCacheMapFactory instance = new ComressionCacheMapFactory();
	}

	public static ComressionCacheMapFactory getInstance() {
		return ComressionCacheMapFactoryHolder.instance;
	}
	
	private Map<String, String> cache = new ConcurrentHashMap<String, String>();

	public Map<String, String> getCache() {
		return cache;
	}

	public void setCache(Map<String, String> cache) {
		this.cache = cache;
	}
	
	
};