package plantpulse.cep.listener.streaming;

import java.util.List;
import java.util.Map;

/**
 * StreamingService
 * 
 * @author lsb
 *
 */
public interface StreamingService {

	/**
	 * addFirstValue
	 * 
	 * @param id
	 * @param label
	 * @param values
	 */
	public void addFirstValue(String id, String label, String[] values);

	/**
	 * getFirstValue
	 * 
	 * @param id
	 * @return
	 */
	public Map<String, Object> getFirstValue(String id);

	/**
	 * getHistoryList
	 * 
	 * @param id
	 * @return
	 */
	public List<Map<String, Object>> getHistoryList(String id);

	/**
	 * getStreamingStringValue
	 * 
	 * @param id
	 * @return
	 */
	public String getStreamingStringValue(String id);

	/**
	 * getStreamingServletURL
	 * 
	 * @param id
	 * @return
	 */
	public String getStreamingServletURL(String id);

}
