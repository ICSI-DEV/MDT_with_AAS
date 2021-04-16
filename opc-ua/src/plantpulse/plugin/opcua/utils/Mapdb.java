package plantpulse.plugin.opcua.utils;

import java.util.concurrent.ConcurrentMap;

import org.mapdb.DB;
import org.mapdb.DBMaker;

public class Mapdb {

	private DB db;

	private ConcurrentMap map;

	private static class SensorStatusCacheHolder {
		static Mapdb instance = new Mapdb();
	}

	public static Mapdb getInstance() {
		return SensorStatusCacheHolder.instance;
	}

	public void connection(){
		db = DBMaker.fileDB("mapdb")
				.allocateStartSize(256 * 1024*1024)
				.allocateIncrement(128 * 1024*1024)
				.checksumHeaderBypass()
				.make();
		map = db.hashMap("map").createOrOpen();
	}

	public void close() {
		db.close();
	}

    public void put(String string, Object obj) {
        map.put(string, obj);
    }

    public ConcurrentMap getMap(){
    	return map;
    }

    public Object get(String str) {
        return map.get(str);
    }

    public void remove(String string) {
        map.remove(string);
    }

}
