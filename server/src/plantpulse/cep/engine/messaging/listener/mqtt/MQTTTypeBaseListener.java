package plantpulse.cep.engine.messaging.listener.mqtt;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.eclipse.paho.client.mqttv3.IMqttDeliveryToken;
import org.eclipse.paho.client.mqttv3.MqttCallback;
import org.eclipse.paho.client.mqttv3.MqttMessage;
import org.json.JSONObject;

import plantpulse.cep.engine.CEPEngineManager;
import plantpulse.cep.engine.logging.EngineLogger;
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
 * MQTTTypeBaseListener
 *
 * @author lsb
 *
 */
public class MQTTTypeBaseListener extends MessageProcessEnable implements MqttCallback {

	private static final Log log = LogFactory.getLog(MQTTTypeBaseListener.class);

	private DataFlowPipe dataflow;
	
	private MessageUnmarshaller unmarshaller;
	
	private TimeoutMessageBackup backup = new TimeoutMessageBackup();
	
	//private AsynchronousExecutor ex = new AsynchronousExecutor(); //가용한 프로세서 갯수로 Pool을 생성할 경우 CPU 사용률의 60% 정도로 자동으로 설정됨.

	public MQTTTypeBaseListener(DataFlowPipe dataflow,  MessageUnmarshaller unmarshaller) {
		super();
		this.dataflow = dataflow;
		this.unmarshaller = unmarshaller;
	}
	
	@Override
	public void messageArrived(String topic, MqttMessage message) throws Exception {
		try {
			

			
			//
			String query = new String(message.getPayload(), "UTF-8");
			JSONObject event = JSONObject.fromObject(query);
			Point point = unmarshaller.unmarshal(event);
			
			//
			MonitoringMetrics.getHistogram(MetricNames.MEESSAGE_LATENCY).update((System.currentTimeMillis() - point.getTimestamp()));
			MonitoringMetrics.getMeter(MetricNames.MESSAGE_IN).mark();
			MonitoringMetrics.getMeter(MetricNames.MESSAGE_IN_MQTT).mark();
			

			//
			MessageListenerStatus.TOTAL_IN_COUNT.incrementAndGet();
			MessageListenerStatus.TOTAL_IN_BYTES.addAndGet(message.getPayload().toString().getBytes().length);
    		MessageListenerStatus.TOTAL_IN_COUNT_BY_MQTT.incrementAndGet();
			
    		//
    		//
			super.enable(point ,backup, dataflow);
			
		} catch (Exception e) {
			DataFlowErrorCount.ERROR_MQTT_SUBSCRIBE.incrementAndGet();
			log.error("MQTT listener  message receive error : " + e.getMessage(), e);
		}
	}

	@Override
	public void connectionLost(Throwable ex) {
		log.warn("MQTT listener connection losted.", ex);
		EngineLogger.error("MQTT 메세지 리스너의 연결이 유실되었습니다. 자동으로 연결 복구를 시작합니다.");
	}

	@Override
	public void deliveryComplete(IMqttDeliveryToken arg0) {
		//
	}
	

}
