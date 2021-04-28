package plantpulse.cep.engine.storage.timeseries.kairosdb;

import java.math.BigDecimal;
import java.util.Iterator;
import java.util.List;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.http.HttpHost;
import org.apache.http.client.config.RequestConfig;
import org.apache.http.config.SocketConfig;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClientBuilder;
import org.apache.http.impl.client.HttpClients;
import org.apache.http.impl.conn.PoolingHttpClientConnectionManager;
import org.json.JSONObject;

import plantpulse.buffer.db.DBDataPoint;
import plantpulse.cep.engine.config.ConfigurationManager;
import plantpulse.cep.engine.monitoring.jmx.PlantPulse;
import plantpulse.cep.engine.storage.timeseries.TimeSeriesDatabase;
import plantpulse.cep.service.client.StorageClient;
import plantpulse.event.opc.Alarm;
import plantpulse.timeseries.db.client.TSDBClient;
import plantpulse.timeseries.db.client.builder.MetricBuilder;


/**
 * KairosDBTimeSeriesDatabase
 * 
 * @author leesa
 *
 */
public class KairosDBTimeSeriesDatabase implements TimeSeriesDatabase {

	private static final Log log = LogFactory.getLog(KairosDBTimeSeriesDatabase.class);

	private TSDBClient client = null;
	
	private static final String _HOST_TAG_NAME    = "_HOST";
	private static final String _SYSTEM_TAG_NAME  = "_SYSTEM";

	private static final String HOST = ConfigurationManager.getInstance().getServer_configuration().getStorage_host();
	private static final int PORT = 7800;
	private static final String _SYSTEM_TAG_VALUE = ConfigurationManager.getInstance().getServer_configuration().getStorage_keyspace();
	

	//
	private static final int TIMEOUT = 60; //타임아웃 1분
	
	
	/**
	 * 
	 */
	public void connect() {
		try {
			
			
			  //
			  SocketConfig sc = SocketConfig.custom()
					    .setSoTimeout(TIMEOUT * 1000)
					    .setSoKeepAlive(true)
					    .setTcpNoDelay(true)
					    .setSoReuseAddress(true)
					    .build();
			
			  //
		      PoolingHttpClientConnectionManager phcc = new PoolingHttpClientConnectionManager();
		      phcc.setMaxTotal(256);
		      phcc.setDefaultMaxPerRoute(100);
		      phcc.setSocketConfig(new HttpHost(HOST, PORT), sc);
		    
		      HttpClientBuilder cb = HttpClients.custom().setConnectionManager(phcc);
		      
		      
			  //
			  RequestConfig config = RequestConfig.custom()
					  .setConnectTimeout(TIMEOUT * 1000)
					  .setConnectionRequestTimeout(TIMEOUT * 1000)
					  .setSocketTimeout(TIMEOUT * 1000).build();
					
			  
		      cb.setDefaultRequestConfig(config);
		      
		      //
		      CloseableHttpClient hc = cb.build();
		      //
			client = new TSDBClient(hc, "http://" + HOST + ":" + PORT);
			log.info("Timeseries database client connected : url=[http://" + HOST + ":" + PORT + "]");

		} catch (Exception ex) {
			log.error("Timeseries database connection error : " + ex.getMessage(), ex);
		}
	}

	/**
	 * 
	 * @param db_data_list
	 */
	public void syncPoint(List<DBDataPoint> db_data_list) {
		
	
	
		if (db_data_list != null && db_data_list.size() > 0) {

			try {
				
				
				MetricBuilder builder = MetricBuilder.getInstance();
				//
				for (int i = 0; i < db_data_list.size(); i++) {
					DBDataPoint data = db_data_list.get(i);

					// 데이터유형에 따른 변환 처리
					if (data.getPoint().getType().equals("double") || data.getPoint().getType().equals("float")
							|| data.getPoint().getType().equals("long") || data.getPoint().getType().equals("int")
							|| data.getPoint().getType().equals("short") || data.getPoint().getType().equals("character")) {
						
						//
						String value = data.getPoint().getValue();
						BigDecimal de = new BigDecimal(value);
						if(de.scale() == 0 && value.length() > 17) {
							de = new BigDecimal(value + ".0");
						};
						
						//
						builder.addMetric(data.getPoint().getTag_name(), "double")
								.addDataPoint(data.getPoint().getTimestamp(), de.doubleValue())
								.addTag(_HOST_TAG_NAME, HOST)
								.addTag("MM:SITE_ID", data.getPoint().getSite_id())
								.addTag("MM:OPC_ID",  data.getPoint().getOpc_id())
								;
					
					} else if (data.getPoint().getType().equals("date")) { //DATE 는 LONG TIMESTAMP 값으로 처리
						builder.addMetric(data.getPoint().getTag_name(), "long")
								.addDataPoint(data.getPoint().getTimestamp(), Long.parseLong(data.getPoint().getValue()))
								.addTag(_HOST_TAG_NAME, HOST)
								.addTag("MM:SITE_ID", data.getPoint().getSite_id())
								.addTag("MM:OPC_ID",  data.getPoint().getOpc_id())
								;
						
					} else if (data.getPoint().getType().equals("string")) { //TEXT
						builder.addMetric(data.getPoint().getTag_name(), "string")
								.addDataPoint(data.getPoint().getTimestamp(), data.getPoint().getValue())
								.addTag(_HOST_TAG_NAME, HOST)
								.addTag("MM:SITE_ID", data.getPoint().getSite_id())
								.addTag("MM:OPC_ID",  data.getPoint().getOpc_id())
								;
						
					} else if (data.getPoint().getType().equals("boolean")) { //불린값은 0/1로 처리
						builder.addMetric(data.getPoint().getTag_name(), "long")
								.addDataPoint(data.getPoint().getTimestamp(), data.getPoint().getValue().equals("true") ? new Long(1) : new Long(0))
								.addTag(_HOST_TAG_NAME, HOST)
								.addTag("MM:SITE_ID", data.getPoint().getSite_id())
								.addTag("MM:OPC_ID",  data.getPoint().getOpc_id())
								;
					} else {
						//
						log.warn("Unsupported timeseries data types = [" + data.getPoint().getType() + "]");
					}

				}
				;
				client.pushMetrics(builder);

				log.debug("Timesries database point inserted : count=[" + db_data_list.size() + "]");

			} catch (Exception ex) {
				log.error("Timeseries database point sync error : " + ex.getMessage(), ex);
			}finally {
			    //
			}

		}
	}
	
	
	public void syncAlarm(Alarm alarm, JSONObject extend) {
			try {
				
				//
				MetricBuilder builder = MetricBuilder.getInstance();
				//
				builder.addMetric("ALARM.SITE."+ alarm.getSite_id())
				.addDataPoint(alarm.getTimestamp(), 1)
				.addTag(_HOST_TAG_NAME, HOST)
				.addTag("MM:ALARM_SEQ",          extend.getString("alarm_seq"))
				.addTag("MM:ALARM_CONFIG_ID",    alarm.getConfig_id())
				.addTag("MM:PRIORITY",     alarm.getPriority())
				.addTag("MM:SITE_ID",      alarm.getSite_id())
				.addTag("MM:OPC_ID",       alarm.getOpc_id())
				.addTag("MM:TAG_ID",       alarm.getTag_id())
				.addTag("MM:DESCRIPTION",  alarm.getDescription())
				;
				//
				builder.addMetric("ALARM.OPC."+ alarm.getOpc_id())
				.addDataPoint(alarm.getTimestamp(), 1)
				.addTag(_HOST_TAG_NAME, HOST)
				.addTag("MM:ALARM_SEQ",          extend.getString("alarm_seq"))
				.addTag("MM:ALARM_CONFIG_ID",    alarm.getConfig_id())
				.addTag("MM:PRIORITY",     alarm.getPriority())
				.addTag("MM:SITE_ID",      alarm.getSite_id())
				.addTag("MM:OPC_ID",       alarm.getOpc_id())
				.addTag("MM:TAG_ID",       alarm.getTag_id())
				.addTag("MM:DESCRIPTION",  alarm.getDescription())
				;
				
				
				//
				if(extend.containsKey("asset_id")) {
					builder.addMetric("ALARM.ASSET."+ extend.getString("asset_id"))
					.addDataPoint(alarm.getTimestamp(), 1)
					.addTag(_HOST_TAG_NAME, HOST)
					.addTag("MM:ALARM_SEQ",          extend.getString("alarm_seq"))
					.addTag("MM:ALARM_CONFIG_ID",    alarm.getConfig_id())
					.addTag("MM:PRIORITY",     alarm.getPriority())
					.addTag("MM:SITE_ID",      alarm.getSite_id())
					.addTag("MM:OPC_ID",       alarm.getOpc_id())
					.addTag("MM:TAG_ID",       alarm.getTag_id())
					.addTag("MM:DESCRIPTION",  alarm.getDescription())
					
					//
					.addTag("MM:ASSET_ID",     extend.getString("asset_id"))
					.addTag("MM:ALIAS_NAME",   extend.getString("alias_name"))
					;
					
				};
				
				
				//
				builder.addMetric("ALARM.TAG."+ alarm.getTag_id())
				.addDataPoint(alarm.getTimestamp(), 1)
				.addTag(_HOST_TAG_NAME, HOST)
				.addTag("MM:ALARM_SEQ",          extend.getString("alarm_seq"))
				.addTag("MM:ALARM_CONFIG_ID",    alarm.getConfig_id())
				.addTag("MM:PRIORITY",     alarm.getPriority())
				.addTag("MM:SITE_ID",      alarm.getSite_id())
				.addTag("MM:OPC_ID",       alarm.getOpc_id())
				.addTag("MM:TAG_ID",       alarm.getTag_id())
				.addTag("MM:DESCRIPTION",  alarm.getDescription())
				;
				
				client.pushMetrics(builder);

				log.debug("Timesries database alarm inserted. ");

			} catch (Exception ex) {
				log.error("Timeseries database alarm sync error : " + ex.getMessage(), ex);
			}finally {
				//
			}
	};
	
	
	public void syncJMX(PlantPulse pp) {
		
		    long timestamp = System.currentTimeMillis();
		
			try {
				
				
				MetricBuilder builder = MetricBuilder.getInstance();
				
				builder.addMetric("PLANTPULSE.JMX.PRODUCT_NAME").addDataPoint(timestamp, pp.getPRODUCT_NAME()).addTag(_SYSTEM_TAG_NAME, _SYSTEM_TAG_VALUE).addTag(_HOST_TAG_NAME, HOST);
				builder.addMetric("PLANTPULSE.JMX.PRODUCT_VERSION").addDataPoint(timestamp, pp.getPRODUCT_VERSION()).addTag(_SYSTEM_TAG_NAME, _SYSTEM_TAG_VALUE).addTag(_HOST_TAG_NAME, HOST);
				
				//
				builder.addMetric("PLANTPULSE.JMX.SERVER_START_TIMESTAMP").addDataPoint(timestamp, pp.getSERVER_START_TIMESTAMP()).addTag(_SYSTEM_TAG_NAME, _SYSTEM_TAG_VALUE).addTag(_HOST_TAG_NAME, HOST);
				builder.addMetric("PLANTPULSE.JMX.SERVER_START_DURATION").addDataPoint(timestamp,  pp.getSERVER_START_DURATION()).addTag(_SYSTEM_TAG_NAME, _SYSTEM_TAG_VALUE).addTag(_HOST_TAG_NAME, HOST);
				builder.addMetric("PLANTPULSE.JMX.SERVER_START_DATE_TEXT").addDataPoint(timestamp, pp.getSERVER_START_DATE_TEXT()).addTag(_SYSTEM_TAG_NAME, _SYSTEM_TAG_VALUE).addTag(_HOST_TAG_NAME, HOST);
				
				//
				builder.addMetric("PLANTPULSE.JMX.SYSTEM_CPU_USED_PERCENT").addDataPoint(timestamp, pp.getSYSTEM_CPU_USED_PERCENT()).addTag(_SYSTEM_TAG_NAME, _SYSTEM_TAG_VALUE).addTag(_HOST_TAG_NAME, HOST);
				builder.addMetric("PLANTPULSE.JMX.SYSTEM_MEMORY_USED_PERCENT").addDataPoint(timestamp, pp.getSYSTEM_MEMORY_USED_PERCENT()).addTag(_SYSTEM_TAG_NAME, _SYSTEM_TAG_VALUE).addTag(_HOST_TAG_NAME, HOST);
				
				 //자바
				builder.addMetric("PLANTPULSE.JMX.JAVA_HEAP_MEMORY_FREE_SIZE").addDataPoint(timestamp, pp.getJAVA_HEAP_MEMORY_FREE_SIZE()).addTag(_SYSTEM_TAG_NAME, _SYSTEM_TAG_VALUE).addTag(_HOST_TAG_NAME, HOST);
				builder.addMetric("PLANTPULSE.JMX.JAVA_HEAP_MEMORY_TOTAL_SIZE").addDataPoint(timestamp, pp.getJAVA_HEAP_MEMORY_TOTAL_SIZE()).addTag(_SYSTEM_TAG_NAME, _SYSTEM_TAG_VALUE).addTag(_HOST_TAG_NAME, HOST);
				builder.addMetric("PLANTPULSE.JMX.JAVA_HEAP_MEMORY_USED_SIZE").addDataPoint(timestamp, pp.getJAVA_HEAP_MEMORY_USED_SIZE()).addTag(_SYSTEM_TAG_NAME, _SYSTEM_TAG_VALUE).addTag(_HOST_TAG_NAME, HOST);
				
				//
				builder.addMetric("PLANTPULSE.JMX.STREAM_TIMEOVER_MS").addDataPoint(timestamp, pp.getSTREAM_TIMEOVER_MS()).addTag(_SYSTEM_TAG_NAME, _SYSTEM_TAG_VALUE).addTag(_HOST_TAG_NAME, HOST);
				builder.addMetric("PLANTPULSE.JMX.STREAM_TIMEOVER_COUNT").addDataPoint(timestamp, pp.getSTREAM_TIMEOVER_COUNT()).addTag(_SYSTEM_TAG_NAME, _SYSTEM_TAG_VALUE).addTag(_HOST_TAG_NAME, HOST);
				builder.addMetric("PLANTPULSE.JMX.STREAM_PROCESSED_COUNT").addDataPoint(timestamp, pp.getSTREAM_PROCESSED_COUNT()).addTag(_SYSTEM_TAG_NAME, _SYSTEM_TAG_VALUE).addTag(_HOST_TAG_NAME, HOST);
				builder.addMetric("PLANTPULSE.JMX.STREAM_PROCESSED_TIME").addDataPoint(timestamp, pp.getSTREAM_PROCESSED_TIME()).addTag(_SYSTEM_TAG_NAME, _SYSTEM_TAG_VALUE).addTag(_HOST_TAG_NAME, HOST);
				builder.addMetric("PLANTPULSE.JMX.STREAM_PROCESSED_COUNT_BY_SEC").addDataPoint(timestamp, pp.getSTREAM_PROCESSED_COUNT_BY_SEC()).addTag(_SYSTEM_TAG_NAME, _SYSTEM_TAG_VALUE).addTag(_HOST_TAG_NAME, HOST);
				builder.addMetric("PLANTPULSE.JMX.STREAM_PENDING_QUEUE_COUNT").addDataPoint(timestamp, pp.getSTREAM_PENDING_QUEUE_COUNT()).addTag(_SYSTEM_TAG_NAME, _SYSTEM_TAG_VALUE).addTag(_HOST_TAG_NAME, HOST);
				builder.addMetric("PLANTPULSE.JMX.STREAM_RUNNING_THREAD").addDataPoint(timestamp, pp.getSTREAM_RUNNING_THREAD()).addTag(_SYSTEM_TAG_NAME, _SYSTEM_TAG_VALUE).addTag(_HOST_TAG_NAME, HOST);
				builder.addMetric("PLANTPULSE.JMX.STREAM_NETWORK_LATENCY_AVG").addDataPoint(timestamp, pp.getSTREAM_NETWORK_LATENCY_AVG()).addTag(_SYSTEM_TAG_NAME, _SYSTEM_TAG_VALUE).addTag(_HOST_TAG_NAME, HOST);
				builder.addMetric("PLANTPULSE.JMX.STREAM_NETWORK_LATENCY_MIN").addDataPoint(timestamp, pp.getSTREAM_NETWORK_LATENCY_MIN()).addTag(_SYSTEM_TAG_NAME, _SYSTEM_TAG_VALUE).addTag(_HOST_TAG_NAME, HOST);
				builder.addMetric("PLANTPULSE.JMX.STREAM_NETWORK_LATENCY_MAX").addDataPoint(timestamp, pp.getSTREAM_NETWORK_LATENCY_MAX()).addTag(_SYSTEM_TAG_NAME, _SYSTEM_TAG_VALUE).addTag(_HOST_TAG_NAME, HOST);
				
				//
				builder.addMetric("PLANTPULSE.JMX.STORAGE_ACTIVE_TIMER").addDataPoint(timestamp, pp.getSTORAGE_ACTIVE_TIMER()).addTag(_SYSTEM_TAG_NAME, _SYSTEM_TAG_VALUE).addTag(_HOST_TAG_NAME, HOST);
				builder.addMetric("PLANTPULSE.JMX.STORAGE_ACTIVE_BATCH").addDataPoint(timestamp, pp.getSTORAGE_ACTIVE_BATCH()).addTag(_SYSTEM_TAG_NAME, _SYSTEM_TAG_VALUE).addTag(_HOST_TAG_NAME, HOST);
				builder.addMetric("PLANTPULSE.JMX.STORAGE_PROCESS_CURRENT_DATA").addDataPoint(timestamp, pp.getSTORAGE_PROCESS_CURRENT_DATA()).addTag(_SYSTEM_TAG_NAME, _SYSTEM_TAG_VALUE).addTag(_HOST_TAG_NAME, HOST);
				builder.addMetric("PLANTPULSE.JMX.STORAGE_PROCESS_TOTAL_DATA").addDataPoint(timestamp, pp.getSTORAGE_PROCESS_TOTAL_DATA()).addTag(_SYSTEM_TAG_NAME, _SYSTEM_TAG_VALUE).addTag(_HOST_TAG_NAME, HOST);
				builder.addMetric("PLANTPULSE.JMX.STORAGE_TOTAL_SAVED_TAG_DATA_COUNT").addDataPoint(timestamp, pp.getSTORAGE_TOTAL_SAVED_TAG_DATA_COUNT()).addTag(_SYSTEM_TAG_NAME, _SYSTEM_TAG_VALUE).addTag(_HOST_TAG_NAME, HOST);
				builder.addMetric("PLANTPULSE.JMX.STORAGE_WRITE_PER_SEC_THREAD").addDataPoint(timestamp, pp.getSTORAGE_WRITE_PER_SEC_THREAD()).addTag(_SYSTEM_TAG_NAME, _SYSTEM_TAG_VALUE).addTag(_HOST_TAG_NAME, HOST);
				builder.addMetric("PLANTPULSE.JMX.STORAGE_PENDING_DATA_SIZE_IN_BUFFER").addDataPoint(timestamp, pp.getSTORAGE_PENDING_DATA_SIZE_IN_BUFFER()).addTag(_SYSTEM_TAG_NAME, _SYSTEM_TAG_VALUE).addTag(_HOST_TAG_NAME, HOST);
				
				//
				builder.addMetric("PLANTPULSE.JMX.ASYNC_EXEC_POOL_SIZE").addDataPoint(timestamp, pp.getASYNC_EXEC_POOL_SIZE()).addTag(_SYSTEM_TAG_NAME, _SYSTEM_TAG_VALUE).addTag(_HOST_TAG_NAME, HOST);
				builder.addMetric("PLANTPULSE.JMX.ASYNC_EXEC_CORE_POOL_SIZE").addDataPoint(timestamp, pp.getASYNC_EXEC_CORE_POOL_SIZE()).addTag(_SYSTEM_TAG_NAME, _SYSTEM_TAG_VALUE).addTag(_HOST_TAG_NAME, HOST);
				builder.addMetric("PLANTPULSE.JMX.ASYNC_EXEC_MAXIMUM_POOL_SIZE").addDataPoint(timestamp, pp.getASYNC_EXEC_MAXIMUM_POOL_SIZE()).addTag(_SYSTEM_TAG_NAME, _SYSTEM_TAG_VALUE).addTag(_HOST_TAG_NAME, HOST);
				builder.addMetric("PLANTPULSE.JMX.ASYNC_EXEC_ACTIVE_COUNT").addDataPoint(timestamp, pp.getASYNC_EXEC_ACTIVE_COUNT()).addTag(_SYSTEM_TAG_NAME, _SYSTEM_TAG_VALUE).addTag(_HOST_TAG_NAME, HOST);
				builder.addMetric("PLANTPULSE.JMX.ASYNC_EXEC_COMPLETED_TASK_COUNT").addDataPoint(timestamp, pp.getASYNC_EXEC_COMPLETED_TASK_COUNT()).addTag(_SYSTEM_TAG_NAME, _SYSTEM_TAG_VALUE).addTag(_HOST_TAG_NAME, HOST);
				builder.addMetric("PLANTPULSE.JMX.ASYNC_EXEC_TASK_COUNT").addDataPoint(timestamp, pp.getASYNC_EXEC_TASK_COUNT()).addTag(_SYSTEM_TAG_NAME, _SYSTEM_TAG_VALUE).addTag(_HOST_TAG_NAME, HOST);
				builder.addMetric("PLANTPULSE.JMX.ASYNC_EXEC_PENDING_TASK_SIZE").addDataPoint(timestamp, pp.getASYNC_EXEC_PENDING_TASK_SIZE()).addTag(_SYSTEM_TAG_NAME, _SYSTEM_TAG_VALUE).addTag(_HOST_TAG_NAME, HOST);
				
				builder.addMetric("PLANTPULSE.JMX.MESSAGING_TOTAL_IN_COUNT").addDataPoint(timestamp, pp.getMESSAGING_TOTAL_IN_COUNT()).addTag(_SYSTEM_TAG_NAME, _SYSTEM_TAG_VALUE).addTag(_HOST_TAG_NAME, HOST);
				builder.addMetric("PLANTPULSE.JMX.MESSAGING_TOTAL_IN_BYTES").addDataPoint(timestamp, pp.getMESSAGING_TOTAL_IN_BYTES()).addTag(_SYSTEM_TAG_NAME, _SYSTEM_TAG_VALUE).addTag(_HOST_TAG_NAME, HOST);
				
				builder.addMetric("PLANTPULSE.JMX.MESSAGING_TOTAL_IN_COUNT_BY_KAFKA").addDataPoint(timestamp, pp.getMESSAGING_TOTAL_IN_COUNT_BY_KAFKA()).addTag(_SYSTEM_TAG_NAME, _SYSTEM_TAG_VALUE).addTag(_HOST_TAG_NAME, HOST);
				builder.addMetric("PLANTPULSE.JMX.MESSAGING_TOTAL_IN_COUNT_BY_MQTT").addDataPoint(timestamp, pp.getMESSAGING_TOTAL_IN_COUNT_BY_MQTT()).addTag(_SYSTEM_TAG_NAME, _SYSTEM_TAG_VALUE).addTag(_HOST_TAG_NAME, HOST);
				builder.addMetric("PLANTPULSE.JMX.MESSAGING_TOTAL_IN_COUNT_BY_STOMP").addDataPoint(timestamp, pp.getMESSAGING_TOTAL_IN_COUNT_BY_STOMP()).addTag(_SYSTEM_TAG_NAME, _SYSTEM_TAG_VALUE).addTag(_HOST_TAG_NAME, HOST);
				builder.addMetric("PLANTPULSE.JMX.MESSAGING_TOTAL_IN_COUNT_BY_HTTP").addDataPoint(timestamp, pp.getMESSAGING_TOTAL_IN_COUNT_BY_HTTP()).addTag(_SYSTEM_TAG_NAME, _SYSTEM_TAG_VALUE).addTag(_HOST_TAG_NAME, HOST);
				builder.addMetric("PLANTPULSE.JMX.MESSAGING_BACKUPED_TOTAL_COUNT").addDataPoint(timestamp, pp.getMESSAGING_BACKUPED_TOTAL_COUNT()).addTag(_SYSTEM_TAG_NAME, _SYSTEM_TAG_VALUE).addTag(_HOST_TAG_NAME, HOST);
				builder.addMetric("PLANTPULSE.JMX.MESSAGING_BACKUPED_REPROCESS_COUNT").addDataPoint(timestamp, pp.getMESSAGING_BACKUPED_REPROCESS_COUNT()).addTag(_SYSTEM_TAG_NAME, _SYSTEM_TAG_VALUE).addTag(_HOST_TAG_NAME, HOST);
				builder.addMetric("PLANTPULSE.JMX.MESSAGING_BACKUPED_REMAINING_COUNT").addDataPoint(timestamp, pp.getMESSAGING_BACKUPED_REMAINING_COUNT()).addTag(_SYSTEM_TAG_NAME, _SYSTEM_TAG_VALUE).addTag(_HOST_TAG_NAME, HOST);
				builder.addMetric("PLANTPULSE.JMX.MESSAGING_TOTAL_IN_RATE_1_SECOND").addDataPoint(timestamp, pp.getMESSAGING_TOTAL_IN_RATE_1_SECOND()).addTag(_SYSTEM_TAG_NAME, _SYSTEM_TAG_VALUE).addTag(_HOST_TAG_NAME, HOST);
				builder.addMetric("PLANTPULSE.JMX.MESSAGING_TOTAL_IN_RATE_1_MINUTE").addDataPoint(timestamp, pp.getMESSAGING_TOTAL_IN_RATE_1_MINUTE()).addTag(_SYSTEM_TAG_NAME, _SYSTEM_TAG_VALUE).addTag(_HOST_TAG_NAME, HOST);
				
				
				client.pushMetrics(builder);
				
				//
				//
				//
				cassandraMetricsSync();

				log.debug("Timesries database JMX inserted. ");

			} catch (Exception ex) {
				log.error("Timeseries database JMX sync error : " + ex.getMessage(), ex);
			} finally {
				//
			}
	};
	
	 /**
	  * cassandraMetricsSync
	  */
	 private void cassandraMetricsSync() {
		
	    long timestamp = System.currentTimeMillis();
	
		try {
		
			//
			StorageClient storage = new StorageClient();
			org.json.JSONObject json = storage.forSelect().selectClusterMetricsLast();
			if(json == null) {
				log.warn("JMX Cluster metrics data not found in [TM_MONITOR_CLUSTER_METRICS]. please check [plantpulse-moinitor] component.");
				return ;
			}
			
			org.json.JSONObject metrics =  org.json.JSONObject.fromObject(json.getString("json"));
			//
			if(json != null) {
				MetricBuilder builder = MetricBuilder.getInstance();
				Iterator<String> keysItr = metrics.keys();
			    while (keysItr.hasNext()) {
			    	String key = keysItr.next();
					Object value = metrics.get( key);
					builder.addMetric("PLANTPULSE.JMX.CASSANDRA." + key.toUpperCase())
					.addDataPoint(timestamp, value)
					.addTag(_SYSTEM_TAG_NAME, _SYSTEM_TAG_VALUE)
					.addTag(_HOST_TAG_NAME, HOST);
			    };
				client.pushMetrics(builder);
			};

			log.debug("Cassandra JMX metrics inserted. ");

		} catch (Exception ex) {
			log.error("Cassandra JMX metrics insert error : " + ex.getMessage(), ex);
		}finally{
			//
		}
     };

	/**
	 * 
	 */
	public void close() {
		try {
			if (client != null)
				client.close();
			log.info("Timeseries database client closed.");
		} catch (Exception ex) {
			log.error("Timeseries database close error : " + ex.getMessage(), ex);
		} finally {
			//
		}
	}
}
