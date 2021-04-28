package plantpulse.cep.engine.cache;

import java.util.concurrent.TimeUnit;

import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;

import plantpulse.event.opc.Point;

public class GuavaCache {
	
	LoadingCache<String, Point> cache = CacheBuilder.newBuilder()
		       .maximumSize(1000)
		       .expireAfterWrite(10, TimeUnit.MINUTES)
		       //.removalListener(MY_LISTENER)
		       .build(
		           new CacheLoader<String, Point>() {
		             public Point load(String key) throws Exception {
		               return new Point();
		             }
		           });

}
