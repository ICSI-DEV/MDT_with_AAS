package plantpulse.cep.engine.monitoring.statistics;

import java.util.concurrent.atomic.AtomicLong;

import org.json.JSONObject;


/**
 * DataFlowErrorCount
 * @author lsb
 *
 */
public class DataFlowErrorCount {
	
	//메세지 수신
	public static AtomicLong ERROR_KAFKA_SUBSCRIBE = new AtomicLong();
	public static AtomicLong ERROR_MQTT_SUBSCRIBE  = new AtomicLong();
	public static AtomicLong ERROR_STOMP_SUBSCRIBE = new AtomicLong();
	
	//메세지 발행
	public static AtomicLong ERROR_KAFKA_PUBLISH  = new AtomicLong();
	public static AtomicLong ERROR_MQTT_PUBLISH   = new AtomicLong();
	public static AtomicLong ERROR_STOMP_PUBLISH  = new AtomicLong();
	
	//데이터 저장
	public static AtomicLong ERROR_STORE_POINT_INSERT   = new AtomicLong();
	public static AtomicLong ERROR_STORE_POINT_MAP_INSERT   = new AtomicLong();
	public static AtomicLong ERROR_STORE_BLOB_INSERT   = new AtomicLong();
	public static AtomicLong ERROR_STORE_SAMPLING_INSERT   = new AtomicLong();
	public static AtomicLong ERROR_STORE_AGGREGATION_INSERT   = new AtomicLong();
	public static AtomicLong ERROR_STORE_SNAPSHOT_INSERT   = new AtomicLong();
	public static AtomicLong ERROR_STORE_TRIGGER_INSERT   = new AtomicLong();
	public static AtomicLong ERROR_STORE_ASSET_INSERT   = new AtomicLong();
	public static AtomicLong ERROR_STORE_ASSET_HEALTH_INSERT   = new AtomicLong();
	public static AtomicLong ERROR_STORE_ASSET_EVENT_INSERT   = new AtomicLong();
	public static AtomicLong ERROR_STORE_ASSET_AGGREGATION_INSERT   = new AtomicLong();
	public static AtomicLong ERROR_STORE_ALARM_INSERT   = new AtomicLong();
	
	public static JSONObject toJSON(){
		//
		JSONObject json = new JSONObject();
		json.put("ERROR_KAFKA_SUBSCRIBE", ERROR_KAFKA_SUBSCRIBE.get());
		json.put("ERROR_MQTT_SUBSCRIBE", ERROR_MQTT_SUBSCRIBE.get());
		json.put("ERROR_STOMP_SUBSCRIBE", ERROR_STOMP_SUBSCRIBE.get());
		//
		json.put("ERROR_MQTT_PUBLISH", ERROR_MQTT_PUBLISH.get());
		json.put("ERROR_STOMP_PUBLISH", ERROR_STOMP_PUBLISH.get());
		//
		json.put("ERROR_STORE_POINT_INSERT", ERROR_STORE_POINT_INSERT.get());
		json.put("ERROR_STORE_POINT_MAP_INSERT", ERROR_STORE_POINT_MAP_INSERT.get());
		
		json.put("ERROR_STORE_BLOB_INSERT", ERROR_STORE_BLOB_INSERT.get());
		json.put("ERROR_STORE_SAMPLING_INSERT", ERROR_STORE_SAMPLING_INSERT.get());
		json.put("ERROR_STORE_AGGREGATION_INSERT", ERROR_STORE_AGGREGATION_INSERT.get());
		json.put("ERROR_STORE_SNAPSHOT_INSERT", ERROR_STORE_SNAPSHOT_INSERT.get());
		json.put("ERROR_STORE_TRIGGER_INSERT", ERROR_STORE_TRIGGER_INSERT.get());
		json.put("ERROR_STORE_ASSET_INSERT", ERROR_STORE_ASSET_INSERT.get());
		json.put("ERROR_STORE_ASSET_HEALTH_INSERT", ERROR_STORE_ASSET_HEALTH_INSERT.get());
		json.put("ERROR_STORE_ASSET_EVENT_INSERT", ERROR_STORE_ASSET_EVENT_INSERT.get());
		json.put("ERROR_STORE_ASSET_AGGREGATION_INSERT", ERROR_STORE_ASSET_AGGREGATION_INSERT.get());
		json.put("ERROR_STORE_ALARM_INSERT", ERROR_STORE_ALARM_INSERT.get());
		
		return json;
	}
	
	
	public static void rest(){
		ERROR_KAFKA_SUBSCRIBE.set(0);
		ERROR_MQTT_SUBSCRIBE.set(0);
		ERROR_STOMP_SUBSCRIBE.set(0);
		//
		ERROR_MQTT_PUBLISH.set(0);
		ERROR_STOMP_PUBLISH.set(0);
		//
		ERROR_STORE_POINT_INSERT.set(0);
		ERROR_STORE_POINT_MAP_INSERT.set(0);
		ERROR_STORE_BLOB_INSERT.set(0);
		ERROR_STORE_AGGREGATION_INSERT.set(0);
		ERROR_STORE_SNAPSHOT_INSERT.set(0);
		ERROR_STORE_TRIGGER_INSERT.set(0);
		ERROR_STORE_ASSET_INSERT.set(0);
		ERROR_STORE_ASSET_HEALTH_INSERT.set(0);
		ERROR_STORE_ASSET_EVENT_INSERT.set(0);
		ERROR_STORE_ASSET_AGGREGATION_INSERT.set(0);
		ERROR_STORE_ALARM_INSERT.set(0);
		//
	}
	
}
