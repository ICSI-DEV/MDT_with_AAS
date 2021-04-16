package plantpulse.cep.engine.messaging.listener.kafka;

import java.time.Duration;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.commons.lang.SerializationUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.json.JSONArray;
import org.json.JSONObject;

import plantpulse.cep.engine.CEPEngineManager;
import plantpulse.cep.engine.messaging.listener.MessageListenerStatus;
import plantpulse.cep.engine.messaging.unmarshaller.DefaultTagMessageUnmarshaller;
import plantpulse.cep.engine.utils.TagUtils;
import plantpulse.cep.service.support.tag.TagCacheManager;
import plantpulse.domain.Tag;
import plantpulse.event.opc.Point;

/**
 * KafkaConsumerRunner
 * @author lsb
 *
 */
public class KafkaConsumerRunnerForRow implements Runnable,KafkaConsumerRunner {
	
	private static final Log log = LogFactory.getLog(KafkaConsumerRunnerForPoint.class);
	
    private final AtomicBoolean closed = new AtomicBoolean(false);
    
    private KafkaConsumer<String, String> consumer;
    private int index;
    private Properties props;
    private String group;
    private String client_id;
    private String topic;
    
    public KafkaConsumerRunnerForRow(int index, final Properties props, final String group, final String client_id, final String topic){
    	this.index = index;
    	this.props = (Properties) SerializationUtils.clone(props);
   	    this.topic = topic;
   	    this.group = group;
	    this.client_id = client_id;
    }

    public void run() {
        try {
        	
        	//
        	KafkaTypeBaseListener listener = new KafkaTypeBaseListener(CEPEngineManager.getInstance().getData_flow_pipe(), new DefaultTagMessageUnmarshaller());
        	
        	//
        	props.put("group.id", group);
			props.put("client.id", client_id);
			//
        	consumer = new KafkaConsumer<>(props);
        	
            consumer.subscribe(Arrays.asList(topic));
            //
            
            TagCacheManager tag_cache_manager = new TagCacheManager();
            
            while (!closed.get()) {
            	//
    			if(CEPEngineManager.getInstance().isShutdown()) {
    				return;
    			};
    			
                ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(1_000));
                
                if (records.count() == 0) {
                    // timeout/nothing to read
                	 try {
                		   Thread.sleep(1000L);
                		  } catch (InterruptedException e) {
                	      }
                } else {
                    // Yes, loop over records
                	records.forEach(record -> {
                		//2. 이벤트 전달
						
    						
									try {
										//1.포인트 리스트 생성
										JSONObject json = JSONObject.fromObject(record.value());
										String _event_type = json.getString("_event_type");
										String _event_class = json.getString("_event_class");
										String _api_key = json.getString("_api_key");
										
										JSONArray points = json.getJSONObject("_event_data").getJSONArray("points");
										for(int i=0; i < points.size(); i++) {
											
											MessageListenerStatus.TOTAL_IN_COUNT.incrementAndGet();
											MessageListenerStatus.TOTAL_IN_BYTES.addAndGet(points.getJSONObject(i).toString().getBytes().length);
					                		MessageListenerStatus.TOTAL_IN_COUNT_BY_KAFKA.incrementAndGet();
					                		
											//포인트 분리
											JSONObject item = points.getJSONObject(i);
											String tag_id  = item.getString("tag_id");
											long timestamp    = item.getLong("timestamp");
											String value   = item.getString("value");
											int quality    = item.getInt("quality");
											int error_code = item.getInt("error_code");
											Map<String,String> attribute = new HashMap<String,String>(item.getJSONObject("attribute"));
											
											//메타 데이터 맵핑
											Tag tag = tag_cache_manager.getTag(tag_id);
											if(tag != null) {
												Point point = new Point();
												point.setTag_id(tag_id);
												point.setTag_name(tag.getTag_name());
												point.setTimestamp(timestamp);
												point.setValue(value);
												point.setQuality(quality);
												point.setError_code(error_code);
												point.setGroup_name(tag.getGroup_name());
												point.setSite_id(tag.getSite_id());
												point.setOpc_id(tag.getOpc_id());
												point.setType(TagUtils.fromTagTypeToPointType(tag.getJava_type()));
												point.setAttribute(attribute);
												
												//1. 이벤트 생성
												String event_data = JSONObject.fromObject(point).toString();
												JSONObject event = new JSONObject();
												event.put("_event_type",   _event_type);
												event.put("_event_class",  _event_class);
												event.put("_event_data",   event_data);
												event.put("_api_key",      _api_key);
												
												//2. 이벤트 전달
												String in = event.toString();
												listener.onEvent(in);
											
										
											}else {
												log.warn("Invalid TAG_ID or Not found tag in cache : tag_id=[" + tag_id + "]");
											}
										}
										
										
									} catch (Exception e) {
										log.error("KafkaMessageListener row type message listen error : " + e.getMessage(), e);
									}
							});
                
                    // AOUTO COMMIT 활성화로 주석처리
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