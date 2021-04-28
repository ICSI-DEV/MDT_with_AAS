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
import plantpulse.cep.engine.messaging.impoter.PointImportProcessor;
import plantpulse.cep.engine.messaging.listener.MessageListenerStatus;

/**
 * KafkaConsumerRunnerForImport
 * @author lsb
 *
 */
public class KafkaConsumerRunnerForImport implements Runnable,KafkaConsumerRunner {
	
	private static final Log log = LogFactory.getLog(KafkaConsumerRunnerForPoint.class);
	
    private final AtomicBoolean closed = new AtomicBoolean(false);
    

	private PointImportProcessor processor = new PointImportProcessor();
	
	private static final long LOGGING_IMPORT_COUNT_STEP = 1_000_000;
    
    private KafkaConsumer<String, String> consumer;
    private int index;
    private Properties props;
    private String group;
    private String client_id;
    private String topic;
    
    public KafkaConsumerRunnerForImport(int index, final Properties props, final String group, final String client_id, final String topic){
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
        	//
        	consumer = new KafkaConsumer<>(props);
            consumer.subscribe(Arrays.asList(topic));
            
            //
            while (!closed.get()) {
            	
            	//
            	if(CEPEngineManager.getInstance().isShutdown()) {
        			return;
        		};
        		
                ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(1_000));
                
                int count = records.count();
                if (count == 0) {
                    // 
                	 try {
              		   Thread.sleep(1000L);
              		  } catch (InterruptedException e) {
              	      }
                } else {
                	records.forEach(record -> {
                		try {
                			//
                			MessageListenerStatus.TOTAL_IN_COUNT.incrementAndGet();
                			MessageListenerStatus.TOTAL_IN_BYTES.addAndGet(record.value().getBytes().length);
                    		MessageListenerStatus.TOTAL_IN_COUNT_BY_KAFKA.incrementAndGet();
                    		//
                    		String query = record.value();

                    		//
                    		processor.process(index, query);
							
						} catch (Exception e) {
							log.error("KafkaMessageListener point import message listen error : " + e.getMessage(), e);
						}
                	});
                	
                	// AOUTO COMMIT 활성화로 주석처리
                    consumer.commitAsync();
                	
                	if((processor.getTotalImportedCount() % LOGGING_IMPORT_COUNT_STEP) == 0) { 
                		log.info("Point imported by kafka consumer : topic=[" + topic + "], total_imported_count=[" + processor.getTotalImportedCount() + "]");
                	};
                }
            }
        } catch (Exception e) {
            log.error("Kafka consumer runner starting error : " + e.getMessage(), e);
            if (!closed.get()) throw e;
        } finally {
            consumer.close();
        }
    }

    
    public void shutdown() {
        closed.set(true);
        consumer.close();
    }
}