package plantpulse.cep.engine.messaging.listener.kafka;

import java.util.ArrayList;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.StickyAssignor;

import com.google.common.util.concurrent.ThreadFactoryBuilder;

import plantpulse.cep.engine.config.ConfigurationManager;
import plantpulse.cep.engine.logging.EngineLogger;
import plantpulse.cep.engine.messaging.listener.MessageListener;
import plantpulse.client.DefaultDestination;
import plantpulse.client.blob.BLOBDestination;
import plantpulse.client.kafka.KAFKAEventClient;

/**
 * KafkaMessageListener
 * 
 * @author lsb
 *
 */
public class KafkaMessageListener implements MessageListener<List<KafkaConsumerRunner>> {

	private static final Log log = LogFactory.getLog(KafkaMessageListener.class);

	//
	private static final int FOR_P_NUM_THREADS     = 8; //포인트
	private static final int FOR_R_NUM_THREADS     = 2; //로우
	private static final int FOR_P_I_NUM_THREADS   = 1; //임포트
	private static final int FOR_BLOB_NUM_THREADS  = 1; //BLOB
	private static final long THREAD_TIMEOUT_MS = 1000 * 60 * 60; //1시간
	
	//
	private static final boolean USE_MT = false;
	private static final int     MT_FOR_P_NUM_THREADS  = 1; //포인트
	 
	
	//
	private ExecutorService executor = null;
	private List<KafkaConsumerRunner> consumers = new ArrayList<KafkaConsumerRunner>();

	
	public KafkaMessageListener() {
		
	}

	public Object startListener() throws Exception {

		try {

		      String host = ConfigurationManager.getInstance().getServer_configuration().getMq_host();

			  String group_id = "plantpulse-server";
			  //
			  Properties props = new Properties();
			  props.put("bootstrap.servers", host + ":" + KAFKAEventClient.DEFAULT_PORT);
			  
			  //props.put("client.id", "PLANTPULSE-EVENT-CONSUMER");
		      props.put("group.id",  group_id);
		      
		      props.put("receive.buffer.bytes",    "655360");
		      props.put("request.timeout.ms",      "305000");
		      props.put("heartbeat.interval.ms",   "3000");
		      
			  props.put("auto.offset.reset",       "earliest");
		      props.put("enable.auto.commit",      "false");
		      props.put("auto.commit.interval.ms", "10000");
		      props.put("session.timeout.ms",      "60000");
		      
		      
		      props.put("fetch.min.bytes",            "100000");
		      props.put("fetch.max.bytes",            "52428800");
		      props.put("fetch.max.wait.ms",          "200");
		      
		      props.put("max.partition.fetch.bytes",  "10485760");
		      props.put("max.poll.records",           "1000");
		      props.put("max.poll.interval.ms",        Integer.MAX_VALUE + "");
		      
		      props.put("partition.assignment.strategy",  StickyAssignor.class.getName());
		      
		      props.put("key.deserializer",    plantpulse.kafka.serialization.KeyDeserializer.class);
		      props.put("value.deserializer",  plantpulse.kafka.serialization.ValueDeserializer.class);
		      
		      
		 
			  //
			  ThreadFactory thread_factory = new ThreadFactoryBuilder()
		        		.setNameFormat("PP-KAFKA-CONSUMER-POOL-%d")
		        		.setDaemon(false)
		        		.setPriority(Thread.MAX_PRIORITY)
		        		.build();
			 
			  //
			  executor = new ThreadPoolExecutor(
					  FOR_P_NUM_THREADS + FOR_R_NUM_THREADS + FOR_BLOB_NUM_THREADS + FOR_P_I_NUM_THREADS, 
					  FOR_P_NUM_THREADS + FOR_R_NUM_THREADS + FOR_BLOB_NUM_THREADS + FOR_P_I_NUM_THREADS, 
					  THREAD_TIMEOUT_MS, 
					  TimeUnit.MILLISECONDS, 
					  new LinkedBlockingQueue<Runnable>(),
					  thread_factory);
			  
			//포인트
			if(USE_MT) { //컨슈머 자체 멀티 스레딩 사용 여부
				for (int i = 0; i < MT_FOR_P_NUM_THREADS; i++) {
					KafkaConsumerRunnerForPointMT consumer_point_type = new KafkaConsumerRunnerForPointMT(i
							, props, 
							"PP", 
							"PP-KAFKA-POINT-MT-" + i, 
							DefaultDestination.KAFKA);
				    consumers.add(consumer_point_type);
				    executor.execute(consumer_point_type);
				}
			}else {
				for (int i = 0; i < FOR_P_NUM_THREADS; i++) {
					KafkaConsumerRunnerForPoint consumer_point_type = new KafkaConsumerRunnerForPoint(i
							, props, 
							"PP", 
							"PP-KAFKA-POINT-" + i, 
							DefaultDestination.KAFKA);
				    consumers.add(consumer_point_type);
				    executor.execute(consumer_point_type);
				}
			};
			
			Thread.sleep(100);
			
			//포인트 로우 컨슈머
			for (int i = 0; i < FOR_R_NUM_THREADS; i++) {
				KafkaConsumerRunnerForRow consumer_row_type = new KafkaConsumerRunnerForRow(
						i, 
						props, 
						"PP", 
						"PP-KAFKA-ROW-" + i, 
						DefaultDestination.KAFKA + "-" + "row");
				consumers.add(consumer_row_type);
			    executor.execute(consumer_row_type);
			}
			Thread.sleep(100);
			
			//포인트 임포트 컨슈머
			for (int i = 0; i < FOR_P_I_NUM_THREADS; i++) {
				KafkaConsumerRunnerForImport consumer_import_type = new KafkaConsumerRunnerForImport(
						i, 
						props, 
						"PP", 
						"PP-KAFKA-POINT-IMPORT-" + i, 
						DefaultDestination.KAFKA + "-" + "import");
				consumers.add(consumer_import_type);
			    executor.execute(consumer_import_type);
			};
			Thread.sleep(100);
			
			//BLOB 컨슈머
			for (int i = 0; i < FOR_BLOB_NUM_THREADS; i++) {
				KafkaConsumerRunnerForBLOB consumer_blob_type = new KafkaConsumerRunnerForBLOB(
						i, 
						host, 
						"PP-BLOB", 
						"PP-KAFKA-BLOB-" + i, 
						BLOBDestination.KAFKA);
				consumers.add(consumer_blob_type);
			    executor.execute(consumer_blob_type);
			};
			Thread.sleep(10);
			
			//
			log.info("KafkaMessageListener is started : destinationName=[" + DefaultDestination.KAFKA + "," + DefaultDestination.KAFKA + "-" + "row" + "," + DefaultDestination.KAFKA + "-" + "import"  +","+  BLOBDestination.KAFKA + "], props=[" + props.toString() + "]");

			Thread.sleep(100);

		} catch (Exception ex) {
			log.error("KafkaMessageListener message receive thread running error : " + ex.getMessage(), ex);
			EngineLogger.error("KAFKA 메세지 리스너 시작에 실패하였습니다.");
			throw ex;
		} finally {
			//
		}
		return consumers;
	}

	public void stopListener() {
		try {
			
			for(int i=0; i < consumers.size(); i++){
				if(consumers.get(i) != null) consumers.get(i).shutdown();
			};
			
			if(executor != null){ 
				 executor.shutdown();
				 if (executor.awaitTermination(10, TimeUnit.SECONDS)) {
			     } else {
			            executor.shutdownNow();
			     }
			};
			if(consumers != null ){ consumers = new ArrayList<KafkaConsumerRunner>(); }
		} catch (Exception e) {
			log.error(e);
		}
	}

	public List<KafkaConsumerRunner> getConnection() {
		return consumers;
	}

	public void resetConnection() {
		try {
			for(int i=0; i < consumers.size(); i++){

				if (consumers.get(i) != null)
					consumers.get(i).shutdown();
				}
			
		} catch (Exception e) {
			log.error(e);
		}
		consumers = null;
	}

	
	 
}
