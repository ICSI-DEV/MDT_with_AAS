package plantpulse.cep.engine.messaging.broker;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import plantpulse.cep.engine.config.ConfigurationManager;
import plantpulse.cep.engine.messaging.background.BackgroundMessageListenerThread;
import plantpulse.cep.engine.messaging.backup.TimeoutBackup;
import plantpulse.cep.engine.messaging.listener.kafka.KafkaMessageListener;
import plantpulse.cep.engine.messaging.listener.mqtt.MQTTMessageListener;
import plantpulse.cep.engine.messaging.listener.stomp.STOMPMessageListener;
import plantpulse.cep.engine.messaging.publisher.kafka.KAFKAPublisher;
import plantpulse.cep.engine.messaging.publisher.mqtt.MQTTPublisher;
import plantpulse.cep.engine.messaging.publisher.stomp.STOMPPublisher;
import plantpulse.cep.engine.messaging.rate.MessageRateTimer;

/**
 * MultiProtocolMessageServerBroker
 * 
 * @author lenovo
 * 
 */
public class MultiProtocolMessageServerBroker implements MessageServerBroker {

	//
	private static final Log log = LogFactory.getLog(MultiProtocolMessageServerBroker.class);

	//
	private static class MultiProtocolMessageServerBrokerHolder {
		static MultiProtocolMessageServerBroker instance = new MultiProtocolMessageServerBroker();
	}

	public static MultiProtocolMessageServerBroker getInstance() {
		return MultiProtocolMessageServerBrokerHolder.instance;
	}

	public static final int LISTENER_COUNT = ConfigurationManager.getInstance().getServer_configuration().getMq_listener_count() + 1; //

	public static final boolean USE_KAFKA = ConfigurationManager.getInstance().getServer_configuration().isMq_use_kafka();

	private TimeoutBackup backup = null;
	
	private MessageRateTimer message_rate_timer = null;
	
	boolean started = false;
	boolean is_shutdown = false;

	private STOMPPublisher stomp_publisher = new STOMPPublisher();
	private MQTTPublisher mqtt_publisher = new MQTTPublisher();
	private KAFKAPublisher kafka_publisher = new KAFKAPublisher();

	private STOMPMessageListener stomp_listener = new STOMPMessageListener();
	private MQTTMessageListener mqtt_listener = new MQTTMessageListener();
	private KafkaMessageListener kafka_listener = new KafkaMessageListener();
	
	private ExecutorService messgae_listener_aync_exec = Executors.newFixedThreadPool(3);

	@Override
	public void start() {
		
		backup = new TimeoutBackup();
	    backup.init();
	    
	    //
	    message_rate_timer = new MessageRateTimer();
	    message_rate_timer.start();
		  
		started = true;
	}

	@Override
	public void stop() {
		started = false;
	}

	@Override
	public void initPublisher() {
		stomp_publisher.init();
		mqtt_publisher.init();
		kafka_publisher.init();
	}

	@Override
	public void destoryPublisher() {
		stomp_publisher.destory();
		mqtt_publisher.destory();
		kafka_publisher.destory();
	}

	
	
	@Override
	public void startListener(AtomicBoolean started) {
		try {
			//
			messgae_listener_aync_exec.execute(new BackgroundMessageListenerThread(stomp_listener, started));
			messgae_listener_aync_exec.execute(new BackgroundMessageListenerThread(mqtt_listener, started));
			if (USE_KAFKA) {
				messgae_listener_aync_exec.execute(new BackgroundMessageListenerThread(kafka_listener, started));
			};

			log.info("Multi Protocols message listeners background started.");

		} catch (Exception e) {
			log.error("Multi protocol message listener background starting error : " + e.getMessage(), e);
		}
	}

	@Override
	public void stopListener() {
		//
		message_rate_timer.stop();
		//
		stomp_listener.stopListener();
		mqtt_listener.stopListener();
		if (USE_KAFKA) {
			kafka_listener.stopListener();
		};
		messgae_listener_aync_exec.shutdownNow();
	}

	@Override
	public STOMPPublisher getSTOMPPublisher() {
		return stomp_publisher;
	}

	@Override
	public MQTTPublisher getMQTTPublisher() {
		return mqtt_publisher;
	}

	public boolean isStarted() {
		return started;
	}

	public void setStarted(boolean started) {
		this.started = started;
	}

	@Override
	public STOMPMessageListener getSTOMPListener() {
		return stomp_listener;
	}

	@Override
	public MQTTMessageListener getMQTTListener() {
		return mqtt_listener;
	}

	@Override
	public KAFKAPublisher getKAFKAPublisher() {
		return kafka_publisher;
	}

	@Override
	public TimeoutBackup getBackup() {
		return backup;
	}

	@Override
	public void shutdown() {
		is_shutdown = true;
		stopListener();
		destoryPublisher();
	}

	@Override
	public boolean isShutdown() {
		return is_shutdown;
	}

}
