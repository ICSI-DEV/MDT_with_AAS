package plantpulse.cep.engine.messaging.listener.kafka;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.json.JSONObject;

import com.codahale.metrics.Histogram;
import com.codahale.metrics.Meter;
import com.codahale.metrics.UniformReservoir;

import plantpulse.cep.engine.config.ConfigurationManager;
import plantpulse.cep.engine.messaging.backup.TimeoutMessageBackup;
import plantpulse.cep.engine.messaging.listener.MessageProcessEnable;
import plantpulse.cep.engine.messaging.unmarshaller.MessageUnmarshaller;
import plantpulse.cep.engine.monitoring.metric.MetricNames;
import plantpulse.cep.engine.monitoring.metric.MonitoringMetrics;
import plantpulse.cep.engine.monitoring.statistics.DataFlowErrorCount;
import plantpulse.cep.engine.pipeline.DataFlowPipe;
import plantpulse.cep.engine.stream.processor.PointStreamStastics;
import plantpulse.event.opc.Point;

/**
 * KafkaTypeBaseListener
 * @author leesa
 *
 */
public class KafkaTypeBaseListener extends MessageProcessEnable {

	private static final Log log = LogFactory.getLog(KafkaTypeBaseListener.class);

	//private AsynchronousExecutor ex = new AsynchronousExecutor(); 
	
	private DataFlowPipe dataflow;
	private MessageUnmarshaller unmarshaller;
	
	private TimeoutMessageBackup backup = new TimeoutMessageBackup();

	public KafkaTypeBaseListener(DataFlowPipe dataflow, MessageUnmarshaller unmarshaller) {
		super();
		this.dataflow = dataflow;
		this.unmarshaller = unmarshaller;
	}

	public void onEvent(String query) throws Exception {
		
		try {
			//

			JSONObject event = JSONObject.fromObject(query);

			String api_key = ConfigurationManager.getInstance().getApplication_properties().getProperty("api.key");
			if(!api_key.equals(event.getString("_api_key"))){
				throw new Exception("[_api_key] this is an unauthenticated message with an invalid value."); 
			};
			
			Point point = unmarshaller.unmarshal(event);
			
			//
			MonitoringMetrics.getHistogram(MetricNames.MEESSAGE_LATENCY).update((System.currentTimeMillis() - point.getTimestamp()));
			MonitoringMetrics.getMeter(MetricNames.MESSAGE_IN).mark();
			MonitoringMetrics.getMeter(MetricNames.MESSAGE_IN_KAFKA).mark();
			
			//
			super.enable(point ,backup, dataflow);
			
		} catch (Exception e) {
			DataFlowErrorCount.ERROR_KAFKA_SUBSCRIBE.incrementAndGet();
			log.error("KafkaTypeBaseListener message receive error : " + e.getMessage(), e);
		}
	}

}
