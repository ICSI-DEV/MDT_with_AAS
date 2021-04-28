package plantpulse.cep.engine.utils;

import java.util.HashMap;
import java.util.Map;

/**
 * ObjectCreationChecker
 * 
 * @author lsb
 *
 */
public class ObjectCreationChecker {

	private static class ObjectCreationCheckerHolder {
		static ObjectCreationChecker instance = new ObjectCreationChecker();
	}

	public static ObjectCreationChecker getInstance() {
		return ObjectCreationCheckerHolder.instance;
	}

	private Map<String, Boolean> map = new HashMap<String, Boolean>();

	public void setObject(String object, boolean create) {
		map.put(object, create);
	}

	public boolean hasObject(String key) {
		return map.containsKey(key);
	}

	public Object getObject(String object) {
		return map.get(object);
	}

	public void reset() {
		this.map = null;
	}

}
