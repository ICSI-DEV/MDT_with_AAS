package plantpulse.cep.engine.messaging.listener.kafka;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.commons.lang.SerializationUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.kafka.clients.consumer.ConsumerRebalanceListener;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;

import plantpulse.cep.engine.CEPEngineManager;
import plantpulse.cep.engine.messaging.unmarshaller.DefaultTagMessageUnmarshaller;

/**
 * KafkaConsumerRunner
 * 
 * https://www.confluent.io/blog/kafka-consumer-multi-threaded-messaging/
 * @author lsb
 *
 */
public class KafkaConsumerRunnerForPointMT implements Runnable, KafkaConsumerRunner {

	private static final Log log = LogFactory.getLog(KafkaConsumerRunnerForPointMT.class);

	private final AtomicBoolean closed = new AtomicBoolean(false);
	
	private static final int MT_CONSUMER_THREAD_COUNT = 8;

	private static final int POLL_TIMEOUT = 1_000;
	
	private static final int COMMIT_MS = 5_000;

	private final ExecutorService executor = Executors.newFixedThreadPool(MT_CONSUMER_THREAD_COUNT);
	
	private final Map<TopicPartition, KafkaConsumerRunnerForPointTask> activeTasks = new HashMap<>();
	private final Map<TopicPartition, OffsetAndMetadata> offsetsToCommit = new HashMap<>();

	private long lastCommitTime = System.currentTimeMillis();

	private KafkaConsumer<String, String> consumer;
	private int index;
	private Properties props;
	private String group;
	private String client_id;
	private String topic;

	private KafkaTypeBaseListener listener = new KafkaTypeBaseListener(CEPEngineManager.getInstance().getData_flow_pipe(), new DefaultTagMessageUnmarshaller());
	
	public KafkaConsumerRunnerForPointMT(int index, final Properties props, final String group, final String client_id,
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
			props.put("group.id", group);
			props.put("client.id", client_id);
			//
			consumer = new KafkaConsumer<>(props);
			consumer.subscribe(Arrays.asList(topic), new ConsumerRebalanceListener() {

				@Override
				public void onPartitionsAssigned(Collection<TopicPartition> partitions) {
					   consumer.resume(partitions);
					
				}

				@Override
				public void onPartitionsRevoked(Collection<TopicPartition> partitions) {
					  // 1. Stop all tasks handling records from revoked partitions
			        Map<TopicPartition, KafkaConsumerRunnerForPointTask> stoppedTask = new HashMap<>();
			        for (TopicPartition partition : partitions) {
			            KafkaConsumerRunnerForPointTask task = activeTasks.remove(partition);
			            if (task != null) {
			                task.stop();
			                stoppedTask.put(partition, task);
			            }
			        }

			        // 2. Wait for stopped tasks to complete processing of current record
			        stoppedTask.forEach((partition, task) -> {
			            long offset = task.waitForCompletion();
			            if (offset > 0)
			                offsetsToCommit.put(partition, new OffsetAndMetadata(offset));
			        });


			        // 3. collect offsets for revoked partitions
			        Map<TopicPartition, OffsetAndMetadata> revokedPartitionOffsets = new HashMap<>();
			        partitions.forEach( partition -> {
			            OffsetAndMetadata offset = offsetsToCommit.remove(partition);
			            if (offset != null)
			                revokedPartitionOffsets.put(partition, offset);
			        });

			        // 4. commit offsets for revoked partitions
			        try {
			            consumer.commitSync(revokedPartitionOffsets);
			        } catch (Exception e) {
			            log.warn("Failed to commit offsets for revoked partitions!");
			        }
				}
				
			});

			//
			while (!closed.get()) {
				
				//
		    	if(CEPEngineManager.getInstance().isShutdown()) {
					return;
				};
				
				final ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(POLL_TIMEOUT));

                handleFetchedRecords(records);
                checkActiveTasks();
                commitOffsets();
                
			};
			
		} catch (Exception e) {
			log.error("Kafka consumer runner starting error : " + e.getMessage(), e);
			// Ignore exception if closing
			if (!closed.get())
				throw e;
		} finally {
			consumer.close();
		}
	};
	
	
    private void handleFetchedRecords(ConsumerRecords<String, String> records) {
        if (records.count() > 0) {
            List<TopicPartition> partitionsToPause = new ArrayList<>();
             records.partitions().forEach(partition -> {
                 List<ConsumerRecord<String, String>> partitionRecords = records.records(partition);
                 KafkaConsumerRunnerForPointTask task = new KafkaConsumerRunnerForPointTask(partitionRecords, listener);
                 partitionsToPause.add(partition);
                 executor.submit(task);
                 activeTasks.put(partition, task);
             });
            consumer.pause(partitionsToPause);
        }
    }

    private void commitOffsets() {
        try {
            long currentTimeMillis = System.currentTimeMillis();
            if (currentTimeMillis - lastCommitTime > COMMIT_MS) {
                if(!offsetsToCommit.isEmpty()) {
                    consumer.commitSync(offsetsToCommit);
                    offsetsToCommit.clear();
                }
                lastCommitTime = currentTimeMillis;
            }
        } catch (Exception e) {
            log.error("Failed to commit offsets!", e);
        }
    }


    private void checkActiveTasks() {
        List<TopicPartition> finishedTasksPartitions = new ArrayList<>();
        activeTasks.forEach((partition, task) -> {
            if (task.isFinished())
                finishedTasksPartitions.add(partition);
            long offset = task.getCurrentOffset();
            if (offset > 0)
                offsetsToCommit.put(partition, new OffsetAndMetadata(offset));
        });
        finishedTasksPartitions.forEach(partition -> activeTasks.remove(partition));
        consumer.resume(finishedTasksPartitions);
    }

	// Shutdown hook which can be called from a separate thread
	public void shutdown() {
		closed.set(true);
		consumer.close();
	}
}