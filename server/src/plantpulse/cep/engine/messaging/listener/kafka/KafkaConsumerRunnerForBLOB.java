package plantpulse.cep.engine.messaging.listener.kafka;

import java.time.Duration;
import java.util.Arrays;
import java.util.Properties;
import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;

import plantpulse.cep.engine.CEPEngineManager;
import plantpulse.cep.engine.messaging.listener.MessageListenerStatus;
import plantpulse.client.kafka.KAFKAEventClient;

/**
 * KafkaConsumerRunnerForBLOB
 * 
 * @author lsb
 *
 */
public class KafkaConsumerRunnerForBLOB implements Runnable,KafkaConsumerRunner {

	private static final Log log = LogFactory.getLog(KafkaConsumerRunnerForBLOB.class);

	private final AtomicBoolean closed = new AtomicBoolean(false);

	
	private KafkaConsumer<String, byte[]> consumer;
	private int index;
	private String group;
    private String client_id;
	private String topic;
	private String host;
	private KafkaBLOBProcessor blob_processor = new KafkaBLOBProcessor();

	public KafkaConsumerRunnerForBLOB(int index, String host, final String group, final String client_id, final String topic) {
		this.index = index;
		this.host = host;
		this.topic = topic;
		this.group = group;
	   	this.client_id = client_id;
	}

	public void run() {
		try {

			// 바이트 어레이 처리 (오버라이드)
			Properties props = new Properties();
			props.put("bootstrap.servers", host + ":" + KAFKAEventClient.DEFAULT_PORT);
			
			props.put("group.id", group);
			props.put("client.id", client_id);

			props.put("receive.buffer.bytes",  "655360");
			props.put("request.timeout.ms",    "305000");
			props.put("heartbeat.interval.ms", "3000");

			props.put("auto.offset.reset", "earliest");
			props.put("enable.auto.commit", "false");
			props.put("auto.commit.interval.ms", "1000");
			props.put("session.timeout.ms",      "60000");

			props.put("fetch.min.bytes",   "1024000");
			props.put("fetch.max.bytes",   "52428800");
			props.put("fetch.max.wait.ms", "500");
			

			props.put("max.partition.fetch.bytes", "10485760");
			props.put("max.poll.records", "10");
			props.put("max.poll.interval.ms", Integer.MAX_VALUE + "");

			props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
			props.put("value.deserializer", "org.apache.kafka.common.serialization.ByteArrayDeserializer");

			consumer = new KafkaConsumer<>(props);
			consumer.subscribe(Arrays.asList(topic));
			//

			//
			while (!closed.get()) {
				
				//
		    	if(CEPEngineManager.getInstance().isShutdown()) {
					return;
				};
				
				ConsumerRecords<String, byte[]> records = consumer.poll(Duration.ofMillis(10_000));

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
							MessageListenerStatus.TOTAL_IN_BYTES.addAndGet(record.value().length);
							
							MessageListenerStatus.TOTAL_IN_COUNT_BY_KAFKA.incrementAndGet();
							MessageListenerStatus.TOTAL_IN_COUNT_BY_BLOB.incrementAndGet();
							//
							// BLOB 프로세싱
							blob_processor.process(record.key(), record.value());

						} catch (Exception e) {
							log.error("KafkaMessageListener BLOB type message listen error : " + e.getMessage(), e);
						}
					});
					// AOUTO COMMIT 활성화로 주석처리
					consumer.commitAsync();
				}
			}
		} catch (Exception e) {
			log.error("Kafka BLOB consumer runner starting error : " + e.getMessage(), e);
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