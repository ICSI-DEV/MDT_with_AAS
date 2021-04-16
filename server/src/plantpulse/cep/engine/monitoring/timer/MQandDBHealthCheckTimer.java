package plantpulse.cep.engine.monitoring.timer;

import java.util.Properties;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;

import javax.jms.JMSException;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.fusesource.stomp.jms.StompJmsConnectionFactory;

import com.datastax.driver.core.Session;

import plantpulse.cep.engine.config.ConfigurationManager;
import plantpulse.cep.engine.logging.EngineLogger;
import plantpulse.cep.engine.messaging.publisher.mqtt.MQTTConnectionStatus;
import plantpulse.cep.engine.timer.TimerExecuterPool;
import plantpulse.cep.engine.utils.PropertiesUtils;
import plantpulse.client.kafka.KAFKAEventClient;
import plantpulse.dbutils.cassandra.SessionManager;
import plantpulse.json.JSONObject;

/**
 * MQandDBHealthCheckTimer
 * 
 * @author leesa
 *
 */
public class MQandDBHealthCheckTimer  implements MonitoringTimer {

	private static final Log log = LogFactory.getLog(MQandDBHealthCheckTimer.class);
	
	public static final String MONITORING_PROP_PATH = "/monitoring.properties";

	private static final int TIMER_PERIOD = (1000 * 10); //10초마다 실행
	
	private JSONObject STATUS = new JSONObject();

	private ScheduledFuture<?> task;
	
	public void start() {
		
		task = TimerExecuterPool.getInstance().getPool().scheduleAtFixedRate(new Runnable() {

			@Override
			public void run() {
				//
				try {
					
					Properties prop = PropertiesUtils.read(MONITORING_PROP_PATH);
					
					//STOMP TEST  =================================================================
					try{
						if(testSTOMP()){
							STATUS.put("stomp_health", "OK");
						}else{
							STATUS.put("stomp_health", "ERROR");
							EngineLogger.error("STOMP 서버 연결 상태에 문제가 발생하였습니다.");
						}
					}catch(Exception ex){
						log.error("STOMP health check faild.", ex);
					}
					//MQTT TEST  =================================================================
					try{
						if(testMQTT()){
							STATUS.put("mqtt_health", "OK");
						}else{
							STATUS.put("mqtt_health", "ERROR");
							EngineLogger.error("MQTT 서버 연결 상태에 문제가 발생하였습니다.");
						}
					}catch(Exception ex){
						log.error("MQTT health check faild.", ex);
					}
					//KAFKA TEST  =================================================================
					try{
						if(testKAFKA()){
							STATUS.put("kafka_health", "OK");
						}else{
							STATUS.put("kafka_health", "ERROR");
							EngineLogger.error("KAFKA 서버 연결 상태에 문제가 발생하였습니다.");
						}
					}catch(Exception ex){
						log.error("KAFKA health check faild.", ex);
					}
					//DB TEST  =================================================================
					try{
						if(testDB()){
							STATUS.put("db_health", "OK");
						}else{
							STATUS.put("db_health", "ERROR");
							EngineLogger.error("데이터베이스 서버 연결 상태에 문제가 발생하였습니다.");
						}
						
					}catch(Exception ex){
						log.error("DB health check faild.", ex);
					}
					
					log.debug("MQ_DB_MONITORING_STATUS = " + STATUS.toString());
					
					//
				} catch (Exception e) {
					log.error(e);
				}
			}

		}, 1000 * 60 * 1, TIMER_PERIOD, TimeUnit.MILLISECONDS);
	}

	public void stop() {
		task.cancel(true);
	}

	
	public boolean testSTOMP(){
		
		boolean is_success =false;
		javax.jms.Connection connection = null;
		javax.jms.Session session = null;
			try {
				//
				String host = ConfigurationManager.getInstance().getServer_configuration().getMq_host();
				int port = Integer.parseInt(ConfigurationManager.getInstance().getServer_configuration().getMq_port());
				String user = ConfigurationManager.getInstance().getServer_configuration().getMq_user();
				String password = ConfigurationManager.getInstance().getServer_configuration().getMq_password();
				// String destination =
				// ConfigurationManager.getInstance().getConfiguration().getMq_destination();

				StompJmsConnectionFactory factory = new StompJmsConnectionFactory();
				factory.setBrokerURI("tcp://" + host + ":" + port);

				connection = factory.createConnection(user, password);
				connection.start();
				session = connection.createSession(false, javax.jms.Session.AUTO_ACKNOWLEDGE);
				
				
				
				is_success = true;
				

			} catch (Exception e) {
				is_success = false;
			}finally{
				if(session != null)
					try {
						session.close();
					} catch (JMSException e) {
						//log.error("STOMP Helath check error : " + e.getMessage(), e);
					}
				if(connection != null)
					try {
						connection.close();
					} catch (JMSException e) {
						//log.error("STOMP Helath check error : " + e.getMessage(), e);
					}
			}
			return is_success;
   };
   
   
   public boolean testMQTT(){
	   return MQTTConnectionStatus.CONNECTED.get();
	   //TODO 지속적인 연결 및 해제는 H2에 가비지 데이터가 쌓임.
	   
	   /*
	   MqttClient connection = null;
	   boolean is_success =false;
	   try{
		
		String host = ConfigurationManager.getInstance().getServer_configuration().getMq_host();
		int port = MessageConstants.MQTT_PORT;
		String user = ConfigurationManager.getInstance().getServer_configuration().getMq_user();
		String password = ConfigurationManager.getInstance().getServer_configuration().getMq_password();
		
		//
		//String tmp_dir = System.getProperty("java.io.tmpdir");
		//MqttDefaultFilePersistence persistence = new MqttDefaultFilePersistence(tmp_dir);
		MemoryPersistence persistence = new MemoryPersistence();
		connection = new MqttClient("tcp://" + host + ":" + port, "PP-HEALTH-CHECK",  persistence);

		MqttConnectOptions options = new MqttConnectOptions();
		options.setUserName(user);
		options.setPassword(password.toCharArray());
		options.setCleanSession(true);
		options.setAutomaticReconnect(false);
		options.setConnectionTimeout(0);
		options.setKeepAliveInterval(60);
	//	connection.connect(options);
		
		is_success = true;
		
	   }catch(Exception ex){
		   is_success = false;
	   }finally{
		   try {
			if(connection != null && connection.isConnected()) {
				connection.disconnectForcibly(1);
				connection.close();
			};
		} catch (MqttException e) {
			log.error("MQTT Helath check connection disconnect error : " + e.getMessage(), e);
		}
	   }
	   return is_success;
	   */
   };
   
   
   public boolean testKAFKA(){
	   //
	   boolean is_success =false;
	   Producer<String,String> producer = null;
	   try{
		
		   String host = ConfigurationManager.getInstance().getServer_configuration().getMq_host();

			//
		       Properties k_props = new Properties();
				k_props.put("bootstrap.servers", host + ":" + KAFKAEventClient.DEFAULT_PORT);
				k_props.put("acks",   "all");
				k_props.put("batch.size", 16384);
				k_props.put("retries", 3);
			    k_props.put("linger.ms", 5);
			    k_props.put("buffer.memory", 33554432);
			    k_props.put("max.request.size", 10485760); //
			    k_props.put("key.serializer",     plantpulse.kafka.serialization.KeySerializer.class);
			    k_props.put("value.serializer",   plantpulse.kafka.serialization.ValueSerializer.class);
			   
			   //
			   producer = new KafkaProducer<String, String>(k_props);
			   producer.send(new ProducerRecord<String, String>("PING", "OK"));
		       
		       
		       is_success = true;
		
		
	   }catch(Exception ex){
		   is_success = false;
	   }finally{
		   if(producer != null) producer.close(); 
	   }
	   return is_success;
   };
   
   
   public boolean testDB(){
	   //
	   boolean is_success =false;
	   try{
		    //
			String host = ConfigurationManager.getInstance().getServer_configuration().getStorage_host();
			int port = Integer.parseInt(ConfigurationManager.getInstance().getServer_configuration().getStorage_port());
			String user = ConfigurationManager.getInstance().getServer_configuration().getStorage_user();
			String password = ConfigurationManager.getInstance().getServer_configuration().getStorage_password();
			String keyspace = ConfigurationManager.getInstance().getServer_configuration().getStorage_keyspace();
			
			//
			Session session = SessionManager.getSession(host, port, user, password, keyspace);
			if(!session.isClosed()){
				is_success = true;
			}else{
				is_success = false;
			};
			
	   }catch(Exception ex){
		   is_success = false;
	   }finally {
		   
	   }
	   return is_success;
   }

	public JSONObject getSTATUS() {
		return STATUS;
	}
	
}
