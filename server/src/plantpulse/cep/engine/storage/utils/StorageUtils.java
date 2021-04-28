package plantpulse.cep.engine.storage.utils;

import java.text.SimpleDateFormat;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.util.Date;

import org.json.JSONObject;

public class StorageUtils {
	
	public static Object millis2Date(long timestamp) {
		SimpleDateFormat format = new SimpleDateFormat("HH:mm:ss.SSS");
		return format.format(timestamp);
	}

	public static boolean isNull(JSONObject data, String key) {
		try {
			data.getDouble(key);
			return false;
		} catch (Exception ex) {
			return true;
		}
	}
	
	/**
	 * 
	 * @return
	 */
	public static LocalDateTime getLocalDateTime() {
		Date timestamp = new Date();
		return timestamp.toInstant().atZone(ZoneId.systemDefault()).toLocalDateTime();
	};

}
