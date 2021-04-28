package plantpulse.cep.engine.storage.insert;

import java.util.Properties;

import plantpulse.cep.engine.utils.PropertiesUtils;

public class BatchProperties {

	public static final String BATCH_PROPERTIES_PATH = "/batch.properties";

	public static String getProperty(String key) {
		Properties prop = PropertiesUtils.read(BATCH_PROPERTIES_PATH);
		return prop.getProperty(key);
	}

	public static String getString(String key) {
		return getProperty(key);
	}

	public static int getInt(String key) {
		return Integer.parseInt(getProperty(key));
	}

	public static boolean getBoolean(String key) {
		return Boolean.parseBoolean(getProperty(key));
	}

}
