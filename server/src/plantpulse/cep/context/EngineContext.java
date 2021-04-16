package plantpulse.cep.context;

import java.util.HashMap;
import java.util.Map;

/**
 * EngineContext
 * 
 * @author lenovo
 *
 */
public class EngineContext {

	private static class EngineContexHolder {
		static EngineContext instance = new EngineContext();
	}

	public static EngineContext getInstance() {
		return EngineContexHolder.instance;
	}

	private Map<String, Object> props = new HashMap<String, Object>();

	public Map<String, Object> getProps() {
		return props;
	}

	public void setProps(Map<String, Object> props) {
		this.props = props;
	}

	public String getProperty(String key) {
		return (String) props.get(key);
	}

}
