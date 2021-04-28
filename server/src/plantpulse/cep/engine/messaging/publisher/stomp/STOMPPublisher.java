package plantpulse.cep.engine.messaging.publisher.stomp;

import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.ReentrantLock;

import javax.jms.DeliveryMode;
import javax.jms.Destination;
import javax.jms.MessageProducer;
import javax.jms.TextMessage;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.eclipse.collections.impl.map.mutable.ConcurrentHashMap;
import org.fusesource.stomp.jms.StompJmsConnectionFactory;
import org.fusesource.stomp.jms.StompJmsDestination;

import plantpulse.cep.engine.CEPEngineManager;
import plantpulse.cep.engine.config.ConfigurationManager;
import plantpulse.cep.engine.monitoring.metric.MetricNames;
import plantpulse.cep.engine.monitoring.metric.MonitoringMetrics;
import plantpulse.cep.engine.monitoring.statistics.DataFlowErrorCount;

public class STOMPPublisher {

	private static final Log log = LogFactory.getLog(STOMPPublisher.class);

	public static final String TOPIC_BASE_PATH = "/topic/";

	private javax.jms.Connection connection = null;
	private javax.jms.Session session = null;
	
	private AtomicBoolean connected = new AtomicBoolean(false);
	
	private AtomicLong failed_count = new AtomicLong(0);
	
	private Map<String,MessageProducer> cache = new ConcurrentHashMap<String,MessageProducer>();
	
	
	private ReentrantLock lock = new ReentrantLock();
	
	public void init() {
		connect();
		log.info("STOMPPublisher initiallized.");
		;
	}

	public void destory() {
		disconnect();
		log.info("STOMPPublisher destoryed.");
		;
	}

	public void connect() {
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
			factory.setForceAsyncSend(true);

			connection = factory.createConnection(user, password);
			connection.setClientID("PP-STOMP-PUBLISHER");
			connection.start();
			session = connection.createSession(false, javax.jms.Session.AUTO_ACKNOWLEDGE);
			
			connected.set(true);
			
			log.info("STOMP Connection is connected : broker=[" + host + ":" + port  + "]");;

		} catch (Exception e) {
			log.error("STOMP Connection failed : " + e.getMessage(), e);
		}
	}

	
	public void send(String uri, String mesg) throws Exception {
		try {
			
		
			//연결 체크
			if(!connected.get()){
    			log.error("STOMP Publisher connection closed.");
    			failed_count.incrementAndGet();
    			return;
            };

			String destination = uri.replaceAll("/", ".");
			destination = destination.substring(1, destination.length());
			destination = TOPIC_BASE_PATH + destination;

			//
			if(!cache.containsKey(destination)) {
				Destination dest = new StompJmsDestination(destination);
				MessageProducer cp = session.createProducer(dest);
				cp.setDeliveryMode(DeliveryMode.NON_PERSISTENT);
				cp.setTimeToLive(2 * 1000);
				cache.put(destination, cp);
			};
			MessageProducer producer = cache.get(destination);
			
			TextMessage msg = session.createTextMessage(mesg);
			producer.send(msg);
		
			//
			MonitoringMetrics.getMeter(MetricNames.MESSAGE_OUT).mark();
			MonitoringMetrics.getMeter(MetricNames.MESSAGE_OUT_STOMP).mark();
			
			
			log.debug("Send stomp message : destination=[" + destination + "], message=[" + mesg + "]");

		} catch (Exception e) {
			connected.set(false);
			DataFlowErrorCount.ERROR_STOMP_PUBLISH.incrementAndGet();
			log.error(e);
			throw e;
		} finally {
			// if(producer != null) producer.close();
		}
	}

	public void disconnect() {
		try {
			
			for (String key : cache.keySet()) {
		             cache.get(key).close();
		     };
			
			if(session != null) {
				session.close();
			};
			
			if(connection != null) {
				connection.stop();
				connection.close();
			};
			
			connected.set(false);
		} catch (Exception e) {
			log.error(e);
		}
	}

	public boolean isConnected() {
		return connected.get();
	};
	
	

}
