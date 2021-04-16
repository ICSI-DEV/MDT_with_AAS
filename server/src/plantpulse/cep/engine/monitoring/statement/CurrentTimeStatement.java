package plantpulse.cep.engine.monitoring.statement;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import com.espertech.esper.client.EventBean;

import plantpulse.cep.engine.monitoring.MonitorConstants;
import plantpulse.cep.listener.ResultServiceManager;
import plantpulse.cep.listener.push.PushClient;
import plantpulse.server.mvc.util.DateUtils;

/**
 * CurrentTimeStatement
 * 
 * @author lsb
 *
 */
public class CurrentTimeStatement extends Statement {

	private static final Log log = LogFactory.getLog(CurrentTimeStatement.class);

	public CurrentTimeStatement(String serviceId, String name) {
		super(serviceId, name);
	}

	@Override
	public String getEPL() {
		return "SELECT current_timestamp() as timestamp FROM com.espertech.esper.client.metric.EngineMetric.win:length(10)";
	}

	@Override
	public void update(EventBean[] newEvents, EventBean[] oldEvents) {
		try {
			EventBean event = newEvents[0];
			if (event != null) {
				String current_date_time = DateUtils.fmtISO((Long) event.get("timestamp"));
				PushClient client1 = ResultServiceManager.getPushService().getPushClient(MonitorConstants.PUSH_URL_CURRENT_TIME + "/" + serviceId);
				client1.sendText(current_date_time);
			}
		} catch (Exception ex) {
			log.error(ex.getMessage(), ex);
		}
	}

}
