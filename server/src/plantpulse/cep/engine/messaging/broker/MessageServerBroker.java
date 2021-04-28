package plantpulse.cep.engine.messaging.broker;

import java.util.concurrent.atomic.AtomicBoolean;

import plantpulse.cep.engine.messaging.backup.TimeoutBackup;
import plantpulse.cep.engine.messaging.listener.mqtt.MQTTMessageListener;
import plantpulse.cep.engine.messaging.listener.stomp.STOMPMessageListener;
import plantpulse.cep.engine.messaging.publisher.kafka.KAFKAPublisher;
import plantpulse.cep.engine.messaging.publisher.mqtt.MQTTPublisher;
import plantpulse.cep.engine.messaging.publisher.stomp.STOMPPublisher;

/**
 * MessageServerBroker
 * 
 * @author lenovo
 *
 */
public interface MessageServerBroker {

	public void start();

	public void stop();
	
	public void shutdown();

	public boolean isShutdown();

	public void startListener(AtomicBoolean started);

	public void stopListener();

	public void initPublisher();

	public void destoryPublisher();

	public STOMPPublisher getSTOMPPublisher();

	public MQTTPublisher getMQTTPublisher();
	
	public KAFKAPublisher getKAFKAPublisher();

	public STOMPMessageListener getSTOMPListener();

	public MQTTMessageListener getMQTTListener();

	public TimeoutBackup  getBackup();
}
