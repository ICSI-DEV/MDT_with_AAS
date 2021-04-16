package plantpulse.cep.engine.snapshot;

import java.util.Map;

import org.eclipse.collections.impl.map.mutable.ConcurrentHashMap;

import plantpulse.event.opc.Point;

/**
 * Point Map Factory
 * @author lsb
 *
 */
public class PointMapFactory {

	private static class PointMapFactoryHolder {
		static PointMapFactory instance = new PointMapFactory();
	}

	public static PointMapFactory getInstance() {
		return PointMapFactoryHolder.instance;
	}

	//
	
	//private RMap<String, Point> point_map = RedisInMemoryClient.getInstance().getPointMap("POINT_MAP");
	private Map<String, Point> point_map = new ConcurrentHashMap<String, Point>();

	public Map<String, Point> getPointMap() {
		return point_map;
	}

	public void setPointMap(Map<String, Point> point_map) {
		this.point_map = point_map;
	}
	

}
