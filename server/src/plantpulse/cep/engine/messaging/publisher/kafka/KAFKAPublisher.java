package plantpulse.cep.engine.messaging.publisher.kafka;

import java.util.Properties;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;

import plantpulse.cep.engine.CEPEngineManager;
import plantpulse.cep.engine.config.ConfigurationManager;
import plantpulse.cep.engine.monitoring.metric.MetricNames;
import plantpulse.cep.engine.monitoring.metric.MonitoringMetrics;


public class KAFKAPublisher  {

	private static final Log log = LogFactory.getLog(KAFKAPublisher.class);

	public static final String TOPIC_BASE_PATH  = "pp-";
	public static final int DEFAULT_PORT        =  9092;

	private Producer<String,String> producer = null;
	private Properties properties = null;
	
	private boolean connected = false;
	
	public void init() {
		connect();
		log.info("KAFKAPublisher initiallized.");
		;
	}

	public void destory() {
		disconnect();
		log.info("KAFKAPublisher destoryed.");
		;
	}

	public void connect() {
		try{

			//
			String host = ConfigurationManager.getInstance().getServer_configuration().getMq_host();
			
			Properties k_props = new Properties();
			k_props.put("bootstrap.servers", host + ":" + DEFAULT_PORT);
			k_props.put("client.id",     "PLANTPULSE-INTERNAL");
			k_props.put("acks",   "all"); //강력한 전송 보증
			k_props.put("batch.size", 16384);
			k_props.put("retries", 3);
		    k_props.put("linger.ms", 0);
		    k_props.put("buffer.memory", 33554432);
		    k_props.put("max.request.size", 10485760); //
		    k_props.put("max.in.flight.requests.per.connection", 10485760); //
		   //k_props.put("compression.type", "lz4"); //
		    
		    k_props.put("key.serializer",     plantpulse.kafka.serialization.KeySerializer.class);
		    k_props.put("value.serializer",   plantpulse.kafka.serialization.ValueSerializer.class);
		   
		   //
		   producer = new KafkaProducer<String, String>(k_props);
		   connected = true;
		   
		   log.info("KAFKA connected : broker=[" + host + ":" + DEFAULT_PORT  + "]");
		   
		   
		} catch (Exception ex) {
			log.error("KAFKA connection failed : properties = " +  properties.toString() + " : " + ex.getMessage(), ex);
		}
	}

	
	public void send(String uri, String key, String mesg) throws Exception {
		try {
			
			//
	    	if(CEPEngineManager.getInstance().isShutdown()) {
				return;
			};
			
			if(!connected){
				log.error("KAFKA Publisher connection closed.");
				return;
			};
			
			String destination = TOPIC_BASE_PATH + uri;
			destination = destination.toLowerCase();

			producer.send(new ProducerRecord<String, String>(destination, "PP-PUB-MSG-KEY-" + System.currentTimeMillis(),  mesg.toString()));
			
			//
			MonitoringMetrics.getMeter(MetricNames.MESSAGE_OUT).mark();
			MonitoringMetrics.getMeter(MetricNames.MESSAGE_OUT_KAFKA).mark();
			
			log.debug("Send KAFKA message : destination=[" + destination + "], message=[" + mesg + "]");

		} catch (Exception e) {
			connected = false;
			log.error("KAFKA Connection failed : " + e.getMessage(), e);
		} 
	}

	public void disconnect() {
		try {
			//TODO producers close
			if(producer != null) producer.close();
			connected = false;
		} catch (Exception e) {
			log.error(e);
		}
	}

	public boolean isConnected() {
		return connected;
	};
	
	

}
