package plantpulse.cep.engine.messaging.listener.mqtt;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.eclipse.paho.client.mqttv3.MqttClient;
import org.eclipse.paho.client.mqttv3.MqttConnectOptions;
import org.eclipse.paho.client.mqttv3.persist.MemoryPersistence;

import plantpulse.cep.engine.CEPEngineManager;
import plantpulse.cep.engine.config.ConfigurationManager;
import plantpulse.cep.engine.logging.EngineLogger;
import plantpulse.cep.engine.messaging.broker.MessageConstants;
import plantpulse.cep.engine.messaging.listener.MessageListener;
import plantpulse.cep.engine.messaging.unmarshaller.DefaultTagMessageUnmarshaller;
import plantpulse.client.DefaultDestination;

/**
 * MQTTMessageListener
 * 
 * @author lsb
 * 
 */
public class MQTTMessageListener implements MessageListener<MqttClient> {

	private static final Log log = LogFactory.getLog(MQTTMessageListener.class);

	//
	private MqttClient connection = null;

	public Object startListener() throws Exception {

		try {
			//
			String host = ConfigurationManager.getInstance().getServer_configuration().getMq_host();
			int port = MessageConstants.MQTT_PORT;
			String user = ConfigurationManager.getInstance().getServer_configuration().getMq_user();
			String password = ConfigurationManager.getInstance().getServer_configuration().getMq_password();
			
			//
			String clinet_id = "PP-LISTENER-" + System.currentTimeMillis();
			//
			//String tmp_dir = System.getProperty("java.io.tmpdir");
			//MqttDefaultFilePersistence persistence = new MqttDefaultFilePersistence(tmp_dir);
			MemoryPersistence persistence = new MemoryPersistence();
			connection = new MqttClient("tcp://" + host + ":" + port, clinet_id, persistence);

			MqttConnectOptions options = new MqttConnectOptions();
			options.setMqttVersion(MqttConnectOptions.MQTT_VERSION_3_1_1);
			options.setUserName(user);
			options.setPassword(password.toCharArray());
			options.setCleanSession(true);
			options.setConnectionTimeout(0);
			options.setKeepAliveInterval(10);
			options.setAutomaticReconnect(true);
			
			connection.connect(options);
			if (!connection.isConnected()) {
				throw new Exception("MQTT Connection is NULL.");
			};

			//

			connection.setCallback(new MQTTTypeBaseListener(CEPEngineManager.getInstance().getData_flow_pipe(), new DefaultTagMessageUnmarshaller()));
			
			connection.subscribe(DefaultDestination.MQTT);
			
			log.info("MQTTMessageListener is started : destinationName=[" + DefaultDestination.MQTT + "], version=[" +  MqttConnectOptions.MQTT_VERSION_3_1_1 + "]");

			Thread.sleep(100);

		} catch (Exception ex) {
			log.error("MQTTMessageListener message receive thread running error : " + ex.getMessage(), ex);
			EngineLogger.error("MQTT 메세지 리스너 시작에 실패하였습니다.");
			throw ex;
		} finally {
			//
		}
		return connection;
	}

	public void stopListener() {
		try {
			if(connection != null) {
				connection.disconnect();
				connection.close();
			}
		} catch (Exception e) {
			log.error(e);
		}
	}

	public MqttClient getConnection()  {
		return connection;
	}

	public void resetConnection() {
		try {
			if (connection != null) {
				connection.disconnect();
			    connection.close();
			}
		} catch (Exception e) {
			log.error(e);
		}
		connection = null;
	}

}
