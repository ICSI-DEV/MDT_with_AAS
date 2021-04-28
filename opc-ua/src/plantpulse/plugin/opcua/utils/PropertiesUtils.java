package plantpulse.plugin.opcua.utils;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.net.URL;
import java.util.Properties;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

/**
 * PropertiesUtils
 * 
 * @author lenovo
 *
 */
public class PropertiesUtils {

	private static final Log log = LogFactory.getLog(PropertiesUtils.class);

	/**
	 * 
	 * @param path
	 *            EX) name.propert?��?�ies
	 */
	public static Properties read(String path) {
		Properties properties = new Properties();
		InputStream in = null;
		try {
			ClassLoader classLoader = Thread.currentThread().getContextClassLoader();
			in = classLoader.getResourceAsStream(path);
			if (in == null)
				throw new IOException("Not foud properties file.");
			properties.load(in);
			in.close();
		} catch (IOException e) {
			log.error("Properties loading filed : path=[" + path + "], message=" + e.getMessage(), e);
		} finally {
			if (in != null) {
				try {
					in.close();
				} catch (IOException e) {
					log.error("Properties read io close error : " + e.getMessage(), e);
				}
			}
		}
		return properties;
	}

	public static Properties write(String path, Properties properties) {
		FileOutputStream out = null;
		try {
			ClassLoader classLoader = Thread.currentThread().getContextClassLoader();
			URL url = classLoader.getResource(path);
			File file = new File(url.toURI());
			out = new FileOutputStream(file);
			properties.store(out, null);
			log.info("Properties store success : path=[" + path + "]");
		} catch (Exception e) {
			log.error("Properties store filed : path=[" + path + "], message=" + e.getMessage(), e);
		} finally {
			if (out != null) {
				try {
					out.close();
				} catch (IOException e) {
					log.error("Properties read io close error : " + e.getMessage(), e);
				}
			}
		}
		return properties;
	}

}
