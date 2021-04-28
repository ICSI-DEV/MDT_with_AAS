package plantpulse.cep.engine.messaging.listener.stomp;

import javax.jms.MessageListener;
import javax.jms.TextMessage;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.json.JSONObject;

import plantpulse.cep.engine.CEPEngineManager;
import plantpulse.cep.engine.async.AsynchronousExecutor;
import plantpulse.cep.engine.messaging.backup.TimeoutMessageBackup;
import plantpulse.cep.engine.messaging.listener.MessageListenerStatus;
import plantpulse.cep.engine.messaging.listener.MessageProcessEnable;
import plantpulse.cep.engine.messaging.unmarshaller.MessageUnmarshaller;
import plantpulse.cep.engine.monitoring.metric.MetricNames;
import plantpulse.cep.engine.monitoring.metric.MonitoringMetrics;
import plantpulse.cep.engine.monitoring.statistics.DataFlowErrorCount;
import plantpulse.cep.engine.pipeline.DataFlowPipe;
import plantpulse.cep.engine.stream.processor.PointStreamStastics;
import plantpulse.event.opc.Point;

/**
 * STOMPTypeBaseListener
 * 
 * @author lsb
 * 
 */
public class STOMPTypeBaseListener extends MessageProcessEnable implements MessageListener {

	private static final Log log = LogFactory.getLog(STOMPTypeBaseListener.class);

	private AsynchronousExecutor ex = new AsynchronousExecutor(); 
	
	private DataFlowPipe dataflow;
	private MessageUnmarshaller unmarshaller;
	
	private TimeoutMessageBackup backup = new TimeoutMessageBackup();

	public STOMPTypeBaseListener(DataFlowPipe dataflow,  MessageUnmarshaller unmarshaller) {
		super();
		this.dataflow = dataflow;
		this.unmarshaller = unmarshaller;
	}

	@Override
	public void onMessage(javax.jms.Message message) {
		try {
			
    		//
			TextMessage textmessage = (TextMessage) message;
			
			
    		
			//
    		String query = textmessage.getText();
			JSONObject event = JSONObject.fromObject(query);
			Point point = unmarshaller.unmarshal(event);
			
			//
			MonitoringMetrics.getHistogram(MetricNames.MEESSAGE_LATENCY).update((System.currentTimeMillis() - point.getTimestamp()));
			MonitoringMetrics.getMeter(MetricNames.MESSAGE_IN).mark();
			MonitoringMetrics.getMeter(MetricNames.MESSAGE_IN_STOMP).mark();
			
			//
			MessageListenerStatus.TOTAL_IN_COUNT.incrementAndGet();
			MessageListenerStatus.TOTAL_IN_BYTES.addAndGet(textmessage.getText().getBytes().length);
    		MessageListenerStatus.TOTAL_IN_COUNT_BY_STOMP.incrementAndGet();;
			
    		//
    		//
			super.enable(point ,backup, dataflow);
			
			
			//
		} catch (Exception e) {
			DataFlowErrorCount.ERROR_STOMP_SUBSCRIBE.incrementAndGet();
			log.error("STOMPTypeBaseListener message receive error : " + e.getMessage(), e);
		}
	}

}
