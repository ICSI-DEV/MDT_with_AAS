package plantpulse.cep.service;

import java.util.List;
import java.util.Map;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.json.JSONObject;
import org.springframework.stereotype.Service;

import com.espertech.esper.client.EventBean;
import com.espertech.esper.client.UpdateListener;

import plantpulse.cep.dao.ServerDAO;
import plantpulse.cep.domain.Query;
import plantpulse.cep.domain.Server;
import plantpulse.cep.engine.CEPEngineManager;
import plantpulse.cep.listener.ResultServiceManager;
import plantpulse.cep.listener.push.PushClient;
import plantpulse.cep.query.QueryFactory;

/**
 * StreamService
 * 
 * @author lenovo
 * 
 */
@Service
public class StreamService {

	private static final Log log = LogFactory.getLog(StreamService.class);

	public static final String LOG_STREAM_BASE_URL = "/stream/";

	private QueryService query_service;

	private ServerDAO dao = new ServerDAO();

	public StreamService() {
		query_service = new QueryService();
	}

	public List<Server> getServerList(Map<String, Object> params) throws Exception {
		return dao.getServerList(params);
	}

	/**
	 * 스트리밍 시작
	 * 
	 * @param host
	 * @param type
	 * @return
	 */
	public String onStreaming(String host, String type) {
		//
		final String ws_url = LOG_STREAM_BASE_URL + host + "/" + type;
		final String epl = "SELECT * FROM Log.win:length(1) WHERE host='" + host + "' AND type='" + type + "'";
		//
		Query query = new Query();
		query.setId("STREAM_" + host + "/" + type);
		query.setEpl(epl);
		try {
			query_service.run(query, new UpdateListener() {
				@Override
				public void update(com.espertech.esper.client.EventBean[] newEvents, com.espertech.esper.client.EventBean[] oldEvents) {
					try {
						EventBean event = newEvents[0];
						if (event != null) {
							String json_text = CEPEngineManager.getInstance().getProvider().getEPRuntime().getEventRenderer().renderJSON("json", event);
							JSONObject json = JSONObject.fromObject(JSONObject.fromObject(json_text).getJSONObject("json").toString());
							PushClient client = ResultServiceManager.getPushService().getPushClient(ws_url);
							client.sendJSON(json);
						}
					} catch (Exception e) {
						log.error("StreamService.onStreaming > query_service.run error : " + e.getMessage(), e);
					}
				}
			});
		} catch (Exception e) {
			log.error("StreamService.onStreaming error : " + e.getMessage(), e);
		}
		//
		return ws_url;
	}

	/**
	 * 스트리밍 종료
	 * 
	 * @param host
	 * @param type
	 */
	public void offStreaming(String host, String type) {
		//
		try {
			Query query = QueryFactory.getInstance().getMap().get("STREAM_" + host + "/" + type);
			query_service.remove(query);
		} catch (Exception e) {
			log.error("StreamService.offStreaming error : " + e.getMessage(), e);
		}
	}

}
