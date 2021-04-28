package plantpulse.plugin.opcua.cache;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import org.eclipse.milo.opcua.stack.core.types.builtin.DataValue;
import org.eclipse.milo.opcua.stack.core.types.builtin.Variant;

public class LastValueMap {

	private static class LastValueMapHolder {
		static LastValueMap instance = new LastValueMap();
	}

	public static LastValueMap getInstance() {
		return LastValueMapHolder.instance;
	}

    public  Map<String, DataValue> map = new ConcurrentHashMap<>();

    public void put(String string, DataValue obj) {
        map.put(string, obj);
    }

    public Map<String, DataValue> getMap(){
    	return map;
    }

    public DataValue get(String str) {
        return map.get(str);
    }

    public void remove(String string) {
        map.remove(string);
    }

}
