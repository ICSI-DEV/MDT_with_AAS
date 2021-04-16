package plantpulse.cep.engine.monitoring.statement;

import java.util.HashMap;
import java.util.Map;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.json.JSONObject;

import com.espertech.esper.client.EventBean;

import plantpulse.cep.engine.monitoring.MonitorConstants;
import plantpulse.cep.listener.ResultServiceManager;
import plantpulse.cep.listener.push.PushClient;

/**
 * EngineMetricStatement
 * 
 * @author lsb
 *
 */
public class EngineMetricSummaryStatement extends Statement {

	private static final Log log = LogFactory.getLog(EngineMetricSummaryStatement.class);

	public EngineMetricSummaryStatement(String serviceId, String name) {
		super(serviceId, name);
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see org.realdisplay.cep.engine.monitoring.EPLProvider#getEPL()
	 */
	@Override
	public String getEPL() {
		return " SELECT format_datetime(timestamp) as timestamp, inputCount, scheduleDepth, sum(inputCountDelta) as inputCountDelta "
				+ " FROM com.espertech.esper.client.metric.EngineMetric.win:time_batch(1 min) " + " WHERE engineURI='" + serviceId + "' OUTPUT LAST ";
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see
	 * com.espertech.esper.client.UpdateListener#update(com.espertech.esper.
	 * client.EventBean[], com.espertech.esper.client.EventBean[])
	 */
	@Override
	public void update(EventBean[] newEvents, EventBean[] oldEvents) {
		try {

			if (newEvents == null) {
				return;
			}

			if (newEvents != null && newEvents.length > 0) {
				EventBean event = newEvents[0];
				if (event != null) {
					Map<String, Object> data_map = new HashMap<String, Object>();
					String label = event.get("timestamp").toString();
					String input_count = (Double.valueOf(event.get("inputCount").toString()).intValue()) + "";
					String input_count_delta = (Double.valueOf(event.get("inputCountDelta").toString()).intValue()) + "";
					String schedule_depth = (Double.valueOf(event.get("scheduleDepth").toString()).intValue()) + "";

					data_map.put("timestamp", label);
					data_map.put("input_count", input_count);
					data_map.put("input_count_delta", input_count_delta);
					data_map.put("schedule_depth", schedule_depth);

					//
					JSONObject json = new JSONObject();
					json = JSONObject.fromObject(data_map);

					PushClient client1 = ResultServiceManager.getPushService().getPushClient(MonitorConstants.PUSH_URL_ENGINE_SUMMARY + "/" + serviceId);
					client1.sendJSON(json);
				}
			}
		} catch (Exception ex) {
			log.error("EngineMetricSummaryStatement error : " + ex.getMessage(), ex);
		}
	}

}
