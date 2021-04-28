package plantpulse.cep.engine.messaging.listener.kafka;

import java.time.Duration;
import java.util.Arrays;
import java.util.Properties;
import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.commons.lang.SerializationUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;

import plantpulse.cep.engine.CEPEngineManager;
import plantpulse.cep.engine.messaging.listener.MessageListenerStatus;
import plantpulse.cep.engine.messaging.unmarshaller.DefaultTagMessageUnmarshaller;
import plantpulse.cep.engine.monitoring.metric.MetricNames;
import plantpulse.cep.engine.monitoring.metric.MonitoringMetrics;

/**
 * KafkaConsumerRunner
 * 
 * @author lsb
 *
 */
public class KafkaConsumerRunnerForPoint implements Runnable, KafkaConsumerRunner {

	private static final Log log = LogFactory.getLog(KafkaConsumerRunnerForPoint.class);

	private final AtomicBoolean closed = new AtomicBoolean(false);

	private static final int POLL_TIMEOUT = 1_000;

	private KafkaConsumer<String, String> consumer;
	private int index;
	private Properties props;
	private String group;
	private String client_id;
	private String topic;

	public KafkaConsumerRunnerForPoint(int index, final Properties props, final String group, final String client_id,
			final String topic) {
		this.index = index;
		this.props = (Properties) SerializationUtils.clone(props);
		this.topic = topic;
		this.group = group;
		this.client_id = client_id;
	}

	public void run() {
		try {

			//
			KafkaTypeBaseListener listener = new KafkaTypeBaseListener(CEPEngineManager.getInstance().getData_flow_pipe(),
					new DefaultTagMessageUnmarshaller());
			//
			props.put("group.id", group);
			props.put("client.id", client_id);
			//
			consumer = new KafkaConsumer<>(props);
			consumer.subscribe(Arrays.asList(topic));

			//
			while (!closed.get()) {
				
				//
		    	if(CEPEngineManager.getInstance().isShutdown()) {
					return;
				};
				
				final ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(POLL_TIMEOUT));

				if (records.count() == 0) {
					// timeout/nothing to read
					try {
						Thread.sleep(1000L);
					} catch (InterruptedException e) {
					}
				} else {
					// Yes, loop over records
					
								records.forEach(record -> {
									try {
										//
										MessageListenerStatus.TOTAL_IN_COUNT.incrementAndGet();
										MessageListenerStatus.TOTAL_IN_BYTES.addAndGet(record.value().getBytes().length);
										MessageListenerStatus.TOTAL_IN_COUNT_BY_KAFKA.incrementAndGet();
										//
										listener.onEvent(record.value());
									} catch (Exception e) {
										log.error("KafkaMessageListener point type message listen error : " + e.getMessage(), e);
									}
								});
								
					// AOUTO COMMIT 활성화로 주석처리
					consumer.commitAsync();
				}
			}
		} catch (Exception e) {
			log.error("Kafka consumer runner starting error : " + e.getMessage(), e);
			// Ignore exception if closing
			if (!closed.get())
				throw e;
		} finally {
			consumer.close();
		}
	}

	// Shutdown hook which can be called from a separate thread
	public void shutdown() {
		closed.set(true);
		consumer.close();
	}
}