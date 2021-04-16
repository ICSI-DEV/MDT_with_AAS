package plantpulse.cep.engine.messaging.listener.kafka;

/**
 * KafkaConsumerRunner
 * @author leesa
 *
 */
public interface KafkaConsumerRunner {
	
	public void run();
	
	public void shutdown();

}
