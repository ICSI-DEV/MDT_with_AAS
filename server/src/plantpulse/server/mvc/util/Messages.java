package plantpulse.server.mvc.util;

import java.util.MissingResourceException;
import java.util.ResourceBundle;

/**
 * Messages class
 */
public class Messages {

	private static final String BUNDLE_NAME = "plantpulse.server.mvc.util.messages"; //

	private static final ResourceBundle RESOURCE_BUNDLE = ResourceBundle.getBundle(BUNDLE_NAME);

	private Messages() {
	}

	/**
	 * Get string for key
	 *
	 * @param key
	 * @return string
	 */
	public static String getString(String key) {
		try {
			return RESOURCE_BUNDLE.getString(key);
		} catch (MissingResourceException e) {
			return '!' + key + '!';
		}
	}
}
