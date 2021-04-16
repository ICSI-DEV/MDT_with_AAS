package plantpulse.cep.listener.streaming;

import java.util.List;
import java.util.Map;

/**
 * StreamingUtils
 * 
 * @author lsb
 *
 */
public class StreamingUtils {

	/**
	 * getSteamingStringValue
	 * 
	 * @param id
	 * @return
	 */
	public static String getSteamingStringValue(String id) {
		StreamingService service = new StreamingServiceImpl();
		return service.getStreamingStringValue(id);
	}

	/**
	 * getSteamingHistoryList
	 * 
	 * @param id
	 * @return
	 */
	public static List<Map<String, Object>> getStreamingHistoryList(String id) {
		StreamingService service = new StreamingServiceImpl();
		return service.getHistoryList(id);
	}

}
