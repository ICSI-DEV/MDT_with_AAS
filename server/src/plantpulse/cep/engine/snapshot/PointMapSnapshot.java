package plantpulse.cep.engine.snapshot;

import java.util.Map;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import com.google.common.collect.ImmutableMap;

import plantpulse.event.opc.Point;

/**
 * PointMapSnapshot
 * @author leesa
 *
 */
public class PointMapSnapshot {
	
	private static final Log log = LogFactory.getLog(PointMapSnapshot.class);
	
	public  Map<String, Point>  snapshot() {
		/*
		Map<String, Point> point_map = null;
		int failed = 0;
		while(point_map == null) {
			try {
				
				point_map = new ConcurrentHashMap<String, Point>(); //메모리
				point_map.putAll(PointMapFactory.getInstance().getPointMap());
				
			}catch(java.util.ConcurrentModificationException ce) {
				failed++;
			};
		}
		
		if(failed > 0) {
			log.info("Point map snapshot failed with concurrent modification exception. retry snapshot count = [" + failed + "]");
		}
		return point_map;
		*/
		
		Map<String, Point> point_map = ImmutableMap.copyOf(PointMapFactory.getInstance().getPointMap());
		return point_map;
	}
	
	

}
