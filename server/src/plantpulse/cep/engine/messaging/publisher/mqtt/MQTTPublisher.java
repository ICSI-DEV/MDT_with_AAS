package plantpulse.cep.engine.messaging.publisher.mqtt;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.eclipse.paho.client.mqttv3.IMqttDeliveryToken;
import org.eclipse.paho.client.mqttv3.MqttCallback;
import org.eclipse.paho.client.mqttv3.MqttClient;
import org.eclipse.paho.client.mqttv3.MqttConnectOptions;
import org.eclipse.paho.client.mqttv3.MqttException;
import org.eclipse.paho.client.mqttv3.MqttMessage;
import org.eclipse.paho.client.mqttv3.MqttTopic;
import org.eclipse.paho.client.mqttv3.persist.MemoryPersistence;
import org.realdisplay.framework.util.StringUtils;

import plantpulse.cep.engine.CEPEngineManager;
import plantpulse.cep.engine.config.ConfigurationManager;
import plantpulse.cep.engine.logging.EngineLogger;
import plantpulse.cep.engine.messaging.broker.MessageConstants;
import plantpulse.cep.engine.monitoring.metric.MetricNames;
import plantpulse.cep.engine.monitoring.metric.MonitoringMetrics;
import plantpulse.cep.engine.monitoring.statistics.DataFlowErrorCount;

/**
 * MQTTPublisher
 * @author lsb
 *
 */
public class MQTTPublisher {

	private static final Log log = LogFactory.getLog(MQTTPublisher.class);

	public static final String   TOPIC_BASE_PATH = "";
	public static final int      DEFAULT_QOS = 0;
	public static final boolean  DEFAULT_RETAIND = false;
	public static final long     TOKEN_WAIT_TIMEOUT = 5 * 1000;

	private MqttClient connection = null;
	
	private boolean connected = false;

	public void init() {
		connect();
		log.info("MQTTPublisher initiallized.");
		;
	}

	public void destory() {
		disconnect();
		log.info("MQTTPublisher destoryed.");
		;
	}

	public void connect() {
		try {

			//
			String host = ConfigurationManager.getInstance().getServer_configuration().getMq_host();
			int port = MessageConstants.MQTT_PORT;
			String user = ConfigurationManager.getInstance().getServer_configuration().getMq_user();
			String password = ConfigurationManager.getInstance().getServer_configuration().getMq_password();

			//
			//MqttClientPersistence persistence = new MqttDefaultFilePersistence(System.getProperty("java.io.tmpdir"));
			MemoryPersistence persistence = new MemoryPersistence();
			connection = new MqttClient("tcp://" + host + ":" + port, "PP-PUBLISHER-" + System.currentTimeMillis(), persistence);

			final MqttConnectOptions options = new MqttConnectOptions();
			options.setMqttVersion(MqttConnectOptions.MQTT_VERSION_3_1_1);
			options.setUserName(user);
			options.setPassword(password.toCharArray());
			options.setCleanSession(true);
			options.setConnectionTimeout(0);
			options.setKeepAliveInterval(60);
			options.setMaxInflight(100000); //TODO MQTT 동시 전송 가능한 메세지 갯수 //
			options.setAutomaticReconnect(false);
			connection.setCallback(new MqttCallback(){
				@Override
				public void connectionLost(Throwable ex) {
					log.error("MQTTPublisher connection losted.", ex);
					connected = false;
					MQTTConnectionStatus.CONNECTED.set(false);
					EngineLogger.warn("MQTT 메세지 발행자의 연결이 유실되었습니다. 자동으로 연결 복구를 시작합니다.");
					try {
						connection.connect(options);
						connected = true;
						MQTTConnectionStatus.CONNECTED.set(true);
					} catch ( MqttException e) {
						EngineLogger.error("MQTT 메세지 발행자의 연결 복구 실패 : " + e.getMessage());
					} 
				};
				@Override
				public void deliveryComplete(IMqttDeliveryToken token) {
					
				};
				@Override
				public void messageArrived(String topic, MqttMessage msg) throws Exception {
					
				}
			});
			
			
			connection.connect(options);
			
			if (!connection.isConnected()) {
				throw new Exception("MQTT Connection is NULL.");
			}
			connected = true;
			MQTTConnectionStatus.CONNECTED.set(true);
			
			log.info("MQTT Connection is connected : broker=[" + host + ":" + port  + "]");

		} catch (Exception e) {
			log.error("MQTT Connection failed : " + e.getMessage(), e);
		}
	};
	

	public void send(String uri, String mesg) throws Exception {

		try {
		
			if(!connected){
				log.error("MQTT Publisher connection closed.");
				return;
			};
			
			String destination = StringUtils.cleanPath(TOPIC_BASE_PATH + uri);
			//
			MqttMessage message = new MqttMessage();
			message.setPayload(mesg.getBytes());
			message.setQos(DEFAULT_QOS);
			message.setRetained(DEFAULT_RETAIND);
			//
			MqttTopic topic = connection.getTopic(destination);
			topic.publish(message);
			
			//
			MonitoringMetrics.getMeter(MetricNames.MESSAGE_OUT).mark();
			MonitoringMetrics.getMeter(MetricNames.MESSAGE_OUT_MQTT).mark();
			
			log.debug("MQTT Message sended : destination=[" + destination + "], message=[" + message + "]");;
			
		} catch (Exception e) {
			DataFlowErrorCount.ERROR_MQTT_PUBLISH.incrementAndGet();
			log.error("MQTT message send error : " + e.getMessage(), e);
			throw e;
		}
		
	}

	public void disconnect() {
		try {
			connection.disconnect();
			connection.close();
			
			connected= false;
			MQTTConnectionStatus.CONNECTED.set(false);
		} catch (Exception e) {
			log.error(e);
		}
	}

}
