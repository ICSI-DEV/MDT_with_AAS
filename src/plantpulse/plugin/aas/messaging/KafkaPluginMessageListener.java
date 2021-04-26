package plantpulse.plugin.aas.messaging;

import java.util.ArrayList;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.kafka.clients.consumer.KafkaConsumer;

import plantpulse.plugin.aas.server.AASServer;
import plantpulse.plugin.aas.utils.ConstantsJSON;
import plantpulse.plugin.aas.utils.PropertiesUtils;


/**
 * KafkaPluginMessageListener
 * @author lsb
 *
 */
public class KafkaPluginMessageListener extends PluginMessageProcessor implements PluginMessageListener {


	private static final Log log = LogFactory.getLog(KafkaPluginMessageListener.class);

	public KafkaPluginMessageListener(AASServer server) {
		super(server);
	}

	//
	private static  int NUM_THREADS = 8;
	
	//
	private ExecutorService executor = null;
	private List<KafkaPluginMessageConsumerThread> consumers = new ArrayList<KafkaPluginMessageConsumerThread>();

	@Override
	public void init() {
	}

	@Override
	public void close() {
		
	}
	

	@Override
	public void startListener() {
		try {

			 String[] topic = new String[]{ConstantsJSON.getConfig().getString("kafka.topic")};
			 String host  = ConstantsJSON.getConfig().getString("kafka.host");
			 String port  = ConstantsJSON.getConfig().getString("kafka.port");
			 int thread = Integer.parseInt(ConstantsJSON.getConfig().getString("kafka.thread"));

			  Properties props = new Properties();
			  props.put("bootstrap.servers", host +":"+ port);
		      props.put("group.id", ConstantsJSON.getConfig().getString("group.id"));
			  props.put("auto.offset.reset", ConstantsJSON.getConfig().getString("auto.offset.reset"));
		      props.put("enable.auto.commit", ConstantsJSON.getConfig().getString("enable.auto.commit"));
		      props.put("auto.commit.interval.ms", ConstantsJSON.getConfig().getString("auto.commit.interval.ms"));
		      props.put("session.timeout.ms", ConstantsJSON.getConfig().getString("session.timeout.ms"));
		      props.put("key.deserializer",    ConstantsJSON.getConfig().getString("key.deserializer"));
		      props.put("value.deserializer",  ConstantsJSON.getConfig().getString("value.deserializer"));

			executor = Executors.newFixedThreadPool(NUM_THREADS);

			for (int i = 0; i < NUM_THREADS; i++) {
				KafkaPluginMessageConsumerThread consumer = new KafkaPluginMessageConsumerThread(getServer(), props, topic);
			    consumers.add(consumer);
			    executor.execute(consumer);
			}

			//
			log.info("Kafka PluginMessageListener is started : destinationName=[" + topic+ "], props=[" + props.toString() + "]");

			Thread.sleep(100);

		} catch (Exception ex) {
			log.error("KafkaMessageListener message receive thread running error : " + ex.getMessage(), ex);
		} finally {
			//
		}
	}

	@Override
	public void stopListener() {
		try {
			if(executor != null){ executor.shutdown(); }
			if(consumers != null ){ 
				consumers = new ArrayList<KafkaPluginMessageConsumerThread>(); 
		   }

		} catch (Exception e) {
			log.error(e);
		}
	}

	

}
