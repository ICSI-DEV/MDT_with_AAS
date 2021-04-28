package plantpulse.plugin.opcua.cache;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import org.eclipse.milo.opcua.sdk.server.nodes.UaFolderNode;

public class OPCUAInfoCacheFactory {


	private static class OPCUAInfoCacheFactoryHolder {
		static OPCUAInfoCacheFactory instance = new OPCUAInfoCacheFactory();
	}

	public static OPCUAInfoCacheFactory getInstance() {
		return OPCUAInfoCacheFactoryHolder.instance;
	}

    public  Map<String, UaFolderNode> map = new ConcurrentHashMap<>();

    public void put(String string, UaFolderNode obj) {
        map.put(string, obj);
    }

    public Map<String, UaFolderNode> getMap(){
    	return map;
    }

    public UaFolderNode get(String str) {
        return map.get(str);
    }

    public void remove(UaFolderNode string) {
        map.remove(string);
    }

    public void removeAll() {
    	for(String key : map.keySet()){
    		map.remove(key);
    	}
    }

}
