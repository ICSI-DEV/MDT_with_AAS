package plantpulse.cep.listener.streaming;

import java.util.List;
import java.util.Map;

/**
 * StreamingServiceImpl
 * 
 * @author lsb
 *
 */
@SuppressWarnings({ "unchecked" })
public class StreamingServiceImpl implements StreamingService {

	private StreamingCacheMap streamingCacheMap = StreamingCacheMap.getInstance();

	/*
	 * (non-Javadoc)
	 * 
	 * @see
	 * org.realdisplay.cep.engine.listener.service.streaming.StreamingService#
	 * setFirstValue(java.lang.String, java.lang.String, java.lang.String[])
	 */
	@Override
	public void addFirstValue(String id, String label, String[] values) {
		streamingCacheMap.addFirstMap(id, label, values);
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see
	 * org.realdisplay.cep.engine.listener.service.streaming.StreamingService#
	 * getFirstValue(java.lang.String)
	 */
	@Override
	public Map<String, Object> getFirstValue(String id) {
		return streamingCacheMap.getFirstMap(id);
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see
	 * org.realdisplay.cep.engine.listener.service.streaming.StreamingService#
	 * getHistoryList(java.lang.String)
	 */
	@Override
	public List<Map<String, Object>> getHistoryList(String id) {
		return streamingCacheMap.getHistoryList(id);
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see
	 * org.realdisplay.cep.engine.listener.service.streaming.StreamingService#
	 * getStreamingStringValue(java.lang.String)
	 */
	@Override
	public String getStreamingStringValue(String id) {
		Map<String, Object> data = getFirstValue(id);
		if (data == null)
			return "&label=&value=|||||||||||||||||||";
		String label = (String) data.get("label");
		String[] values = (String[]) data.get("value");
		StringBuffer buffer = new StringBuffer();
		if (label != null && values != null) {
			buffer.append("&label=");
			buffer.append(label);
			buffer.append("&value=");
			for (int j = 0; j < values.length; j++) {
				buffer.append(values[j]);
				buffer.append("|");
			}
		}
		return buffer.toString();
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see
	 * org.realdisplay.cep.engine.listener.service.streaming.StreamingService#
	 * getStreamingServletURL(java.lang.String)
	 */
	@Override
	public String getStreamingServletURL(String id) {
		return "/streaming?id=" + id; // TODO HTTP Streaming URL 변경 지점
	}

}
