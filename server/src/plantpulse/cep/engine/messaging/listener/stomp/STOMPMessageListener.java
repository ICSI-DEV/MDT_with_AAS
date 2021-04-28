package plantpulse.cep.engine.messaging.listener.stomp;

import java.util.concurrent.atomic.AtomicBoolean;

import javax.jms.Connection;
import javax.jms.Destination;
import javax.jms.MessageConsumer;
import javax.jms.Session;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.fusesource.stomp.jms.StompJmsConnectionFactory;
import org.fusesource.stomp.jms.StompJmsDestination;

import plantpulse.cep.engine.CEPEngineManager;
import plantpulse.cep.engine.config.ConfigurationManager;
import plantpulse.cep.engine.logging.EngineLogger;
import plantpulse.cep.engine.messaging.listener.MessageListener;
import plantpulse.cep.engine.messaging.unmarshaller.DefaultTagMessageUnmarshaller;
import plantpulse.client.DefaultDestination;

/**
 * STOMPMessageListener
 * 
 * @author lsb
 * 
 */
public class STOMPMessageListener implements MessageListener<Connection> {

	private static final Log log = LogFactory.getLog(STOMPMessageListener.class);

	public static final String STOMP_ID = "PP";
	
	private Session session = null;
	//
	private Connection connection = null;
	
	private MessageConsumer consumer = null;
	
	private AtomicBoolean connected = new AtomicBoolean(false);

	
	//
	public Object startListener() throws Exception {

		try {

			//

			//
			String host = ConfigurationManager.getInstance().getServer_configuration().getMq_host();
			int port = Integer.parseInt(ConfigurationManager.getInstance().getServer_configuration().getMq_port());
			String user = ConfigurationManager.getInstance().getServer_configuration().getMq_user();
			String password = ConfigurationManager.getInstance().getServer_configuration().getMq_password();

			//
			StompJmsConnectionFactory factory = new StompJmsConnectionFactory();
			factory.setBrokerURI("tcp://" + host + ":" + port);

			connection = factory.createConnection(user, password);
			connection.setClientID("PP-STOMP-LISTENER");
			connection.start();
			session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);

			Destination dest = new StompJmsDestination(DefaultDestination.STOMP);

			consumer = session.createConsumer(dest);
			consumer.setMessageListener(new STOMPTypeBaseListener(CEPEngineManager.getInstance().getData_flow_pipe(), new DefaultTagMessageUnmarshaller()));

			log.info("STOMPMessageListener is started : destinationName=[" + DefaultDestination.STOMP + "], dest=[" + dest.toString() + "]");
		
			connected.set(true);
		  

		} catch (Exception ex) {
			log.error("STOMPMessageListener message receive thread running error : " + ex.getMessage(), ex);
			EngineLogger.error("STOMP 메세지 리스너 시작에 실패하였습니다.");
			throw ex;
		} finally {
			//
		}
		return connection;
	}
	

	public void stopListener() {
		try {
			if(consumer != null) {
				consumer.close();
			};

			if(session != null) {
				session.close();
			};
			
			if(connection != null) {
				connection.stop();
				connection.close();
			};
			
		} catch (Exception e) {
			log.error(e);
		}
	}

	public Connection getConnection() {
		return connection;
	}

	public void resetConnection() {
		try {
			if (connection != null)
				connection.close();
		} catch (Exception e) {
			log.error(e);
		}
		connection = null;
	}

}
