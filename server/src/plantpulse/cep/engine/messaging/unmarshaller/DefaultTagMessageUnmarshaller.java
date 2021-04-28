package plantpulse.cep.engine.messaging.unmarshaller;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.json.JSONObject;

import plantpulse.cep.engine.monitoring.metric.MetricNames;
import plantpulse.cep.engine.monitoring.metric.MonitoringMetrics;
import plantpulse.event.opc.Point;

/**
 * DefaultTagMessageUnmarshaller
 * 
 * @author lsb
 * 
 */
public class DefaultTagMessageUnmarshaller implements MessageUnmarshaller {

	//
	private static final Log log = LogFactory.getLog(DefaultTagMessageUnmarshaller.class);

	/*
	 * (non-Javadoc)
	 * 
	 * @see
	 * com.espertech.esperio.jms.JMSMessageUnmarshaller#unmarshal(com.espertech
	 * .esper.event.EventAdapterService, javax.jms.Message)
	 */
	@Override
	public Point unmarshal(JSONObject event) throws Exception {
		try {
			//
			//String _event_type  = event.getString("_event_type");
			//String _event_class = event.getString("_event_class");
			JSONObject _event_data = event.getJSONObject("_event_data");
			//
			if(event.containsKey("_event_timestamp")) {
				long   _event_timestamp = event.getLong("_event_timestamp");
			    MonitoringMetrics.getHistogram(MetricNames.NETWORK_LATENCY).update((System.currentTimeMillis() - _event_timestamp));
			}
		    //
			return (Point) JSONObject.toBean(_event_data, Point.class);
			//
		} catch (Exception ex) {
			throw new Exception("Error unmarshalling message : " + ex.toString(), ex);
		}
	}

}