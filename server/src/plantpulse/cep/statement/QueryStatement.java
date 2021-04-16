package plantpulse.cep.statement;

import java.util.Date;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.json.JSONArray;
import org.json.JSONObject;

import com.espertech.esper.client.EventBean;
import com.espertech.esper.client.UpdateListener;

import plantpulse.cep.domain.Query;
import plantpulse.cep.engine.CEPEngineManager;
import plantpulse.cep.engine.functions.Functions;
import plantpulse.cep.listener.ResultServiceManager;
import plantpulse.cep.listener.push.PushClient;

/**
 * QueryStatement
 * 
 * @author lsb
 *
 */
public class QueryStatement implements UpdateListener {

	private static final Log log = LogFactory.getLog(QueryStatement.class);

	public static final String MODE_META   = "M"; // 쿼리 메뉴에서 사용
	public static final String MODE_SINGLE = "S"; // 그래프 생성시 단일행 출력시
	public static final String MODE_ARRAY  = "A"; // 그래프 생성시 멀티행 출력시

	private String mode = MODE_META;
	private Query query;

	public QueryStatement(Query query) {
		this.query = query;
	}

	public QueryStatement(Query query, String mode) {
		this.query = query;
		this.mode = mode;
	}

	@Override
	public void update(EventBean[] newEvents, EventBean[] oldEvents) {
		try {
			if (newEvents == null) {
				return;
			}
			;

			PushClient client = ResultServiceManager.getPushService().getPushClient(query.getUrl());

			if (mode.equals(MODE_META)) {
				JSONObject obj = new JSONObject();
				obj.put("length", newEvents.length);
				obj.put("timestamp", Functions.format_datetime(new Date().getTime()));

				JSONArray newArray = new JSONArray();
				for (int i = 0; newEvents != null && i < newEvents.length; i++) {
					EventBean event = newEvents[i];
					if (event != null) {
						String json_text = CEPEngineManager.getInstance().getProvider().getEPRuntime().getEventRenderer().renderJSON("json", event);
						JSONObject json = JSONObject.fromObject(JSONObject.fromObject(json_text).getJSONObject("json").toString());

						newArray.add(json);
					}
				}

				obj.put("data", newArray);
				client.sendJSON(obj);

			} else if (mode.equals(MODE_SINGLE)) {

				EventBean event = newEvents[0];
				if (event != null) {
					String json_text = CEPEngineManager.getInstance().getProvider().getEPRuntime().getEventRenderer().renderJSON("json", event);
					JSONObject json = JSONObject.fromObject(JSONObject.fromObject(json_text).getJSONObject("json").toString());

					client.sendJSON(json);
				}

			} else if (mode.equals(MODE_ARRAY)) {
				if (newEvents.length >= 1) {
					JSONArray newArray = new JSONArray();
					for (int i = 0; newEvents != null && i < newEvents.length; i++) {
						EventBean event = newEvents[i];
						if (event != null) {
							String json_text = CEPEngineManager.getInstance().getProvider().getEPRuntime().getEventRenderer().renderJSON("json", event);
							JSONObject json = JSONObject.fromObject(JSONObject.fromObject(json_text).getJSONObject("json").toString());

							newArray.add(json);
						}
					}
					client.sendJSONArray(newArray);
				}
			}

		} catch (Exception ex) {
			log.error("Statement listener error : " + ex.getMessage(), ex);
		}

	}

}
