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
public class StatementPerformanceStatement extends Statement {

	private static final Log log = LogFactory.getLog(StatementPerformanceStatement.class);

	public StatementPerformanceStatement(String serviceId, String name) {
		super(serviceId, name);
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see org.realdisplay.cep.engine.monitoring.EPLProvider#getEPL()
	 */
	@Override
	public String getEPL() {
		return "SELECT current_timestamp() as timestamp, Math.round(avg(cpuTime  / 1000000.0)) as cpu_avg,  Math.round(avg(wallTime / 1000000.0)) as wall_avg FROM com.espertech.esper.client.metric.StatementMetric.win:time(60 sec) "
				+ " WHERE engineURI='" + serviceId + "' " + "  output last every 5 sec ";
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
			for (int i = 0; i < newEvents.length; i++) {
				EventBean event = newEvents[i];
				if (event != null) {Map<String, Object> data_map = new HashMap<String, Object>();
						data_map.put("timestamp", event.get("timestamp"));
						data_map.put("cpu_avg",   event.get("cpu_avg"));
						data_map.put("wall_avg",  event.get("wall_avg"));

						JSONObject json = JSONObject.fromObject(data_map);
						PushClient client1 = ResultServiceManager.getPushService().getPushClient(MonitorConstants.PUSH_URL_STATEMENT_PERFORMANCE);
						client1.sendJSON(json);
				}
			}

		} catch (Exception ex) {
			log.error("StatementPerformanceStatement error : " + ex.getMessage(), ex);
		}
	}

}
