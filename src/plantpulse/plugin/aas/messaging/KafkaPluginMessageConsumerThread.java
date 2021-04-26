package plantpulse.plugin.aas.messaging;

import java.text.SimpleDateFormat;
import java.time.Duration;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;

import plantpulse.json.JSONObject;
import plantpulse.plugin.aas.server.AASServer;
import plantpulse.plugin.aas.utils.ConstantsJSON;

/**
 * KafkaPluginMessageConsumerThread
 * @author leesa
 *
 */
public class KafkaPluginMessageConsumerThread implements Runnable {

	private static final Log log = LogFactory.getLog(KafkaPluginMessageConsumerThread.class);

    private final AtomicBoolean closed = new AtomicBoolean(false);


    private KafkaConsumer<String, String> consumer;
    private Properties props;
    private String[] topic;
    
    private AASServer server;

    public KafkaPluginMessageConsumerThread(AASServer server, final Properties props, final String[] topic){
    	this.server = server;
    	this.props = props;
   	    this.topic = topic;
   	   
    }

    public void run() {
        try {

        	
        	consumer = new KafkaConsumer<>(props);
            consumer.subscribe(Arrays.asList(topic));
            

            while (!closed.get()) {
                ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(1_000));
                if (records.count() == 0) {

                    // timeout/nothing to read
                } else {
                    // Yes, loop over records
                    for(ConsumerRecord<String, String> record: records) {
                    	String msg =  "";
	   	                 try {
	   	                	msg = record.value();
	   							if(msg != null) {
		   							//
		   							Map<String, String> headers = new HashMap<String, String>();
		   							//
		   							
		   							PluginMessageProcessor processor = new PluginMessageProcessor(server);
		   							processor.process(headers, record.topic(), msg);

	   							}
	   						} catch (Exception e) {
	   							log.error("KafkaMessageListener message listen error : " + e.getMessage() + ", msg=[" + msg +"]", e);
	   						}
                    }

                    //
                    consumer.commitAsync();
                }
            }
        } catch (Exception e) {
            log.error("Kafka consumer runner starting error : " + e.getMessage(), e);
            // Ignore exception if closing
            if (!closed.get()) throw e;
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