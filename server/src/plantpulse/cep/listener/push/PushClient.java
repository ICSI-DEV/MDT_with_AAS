package plantpulse.cep.listener.push;

import java.net.URISyntaxException;
import java.util.Map;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.json.JSONArray;
import org.json.JSONObject;

import plantpulse.cep.engine.CEPEngineManager;

/**
 * PushClient
 * 
 * @author leesangboo
 *
 */
public class PushClient {

	private static Log log = LogFactory.getLog(PushClient.class);

	private String uri = null;

	public PushClient(String uri) throws URISyntaxException {
		this.uri = uri;
	}

	private void sendString(String string) {
		try {
			
			// MQ STOMP 에도 전달
			CEPEngineManager.getInstance().getMessageBroker().getSTOMPPublisher().send(uri, string);

			// 푸시 캐시맵 비활성화
			PushCacheMap.getInstance().add(uri, string);
			
			log.debug("PushURI=" + uri + ", Data=" + string);
			
		} catch (Exception e) {
			log.error("Websocket string send failed.", e);
			;
		}
	}

	public void sendJSON(JSONObject json) {
		sendString(json.toString());
	}

	public void sendJSONArray(JSONArray json) {
		sendString(json.toString());
	}

	public void sendJSON(@SuppressWarnings("rawtypes") Map map) {
		JSONObject json = new JSONObject();
		json.putAll(map);
		sendJSON(json);
	}

	public void sendText(String message) {
		sendString(message);
	}

}
