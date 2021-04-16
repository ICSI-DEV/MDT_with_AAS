package plantpulse.cep.engine.monitoring.metric;


/**
 * MetricNames
 * 
 * @author leesa
 *
 */
public class MetricNames {
	
	//
	public static final String NETWORK_LATENCY = "NETWORK_LATENCY";
	//
	public static final String MEESSAGE_LATENCY = "MEESSAGE_LATENCY";
	public static final String MESSAGE_IN = "MESSAGE_IN";
	public static final String MESSAGE_IN_KAFKA = "MESSAGE_IN_KAFKA";
	public static final String MESSAGE_IN_MQTT  = "MESSAGE_IN_MQTT";
	public static final String MESSAGE_IN_STOMP = "MESSAGE_IN_STOMP";
	public static final String MESSAGE_OUT = "MESSAGE_OUT";
	public static final String MESSAGE_OUT_KAFKA = "MESSAGE_OUT_KAFKA";
	public static final String MESSAGE_OUT_MQTT  = "MESSAGE_OUT_MQTT";
	public static final String MESSAGE_OUT_STOMP = "MESSAGE_OUT_STOMP";
	public static final String MESSAGE_TIMEOUT = "MESSAGE_TIMEOUT";
	public static final String MESSAGE_TIMEOUT_RE_PROCESSED = "MESSAGE_TIMEOUT_RE_PROCESSED";
	public static final String MESSAGE_TIMEOUT_RE_PROCESSED_TIME = "MESSAGE_TIMEOUT_RE_PROCESSED_TIME";
	
	//
	public static final String DATAFLOW_WORKER_THREAD   = "DATAFLOW_WORKER_THREAD";
	public static final String DATAFLOW_VALIDATE_FAILED = "DATAFLOW_VALIDATE_FAILED";
	
	//
	public static final String STREAM_PROCESSED = "STREAM_PROCESSED";
	public static final String STREAM_PROCESSED_TIME = "STREAM_PROCESSED_TIME";
	public static final String STREAM_TIMEOVER = "STREAM_TIMEOVER";
	
	//
	public static final String STORAGE_DB_BUFFER = "STORAGE_DB_BUFFER";
	public static final String STORAGE_DB_BUFFER_QUEUE_PEDDING = "STORAGE_DB_BUFFER_QUEUE_PEDDING";
	
	//
	public static final String DDS_PROCESSED = "DDS_PROCESSED";
	public static final String DDS_PROCESSED_TYPE_TAG_POINT     = "DDS_PROCESSED_TYPE_TAG_POINT";
	public static final String DDS_PROCESSED_TYPE_TAG_ALARM     = "DDS_PROCESSED_TYPE_TAG_ALARM";
	public static final String DDS_PROCESSED_TYPE_ASSET_DATA    = "DDS_PROCESSED_TYPE_ASSET_DATA";
	public static final String DDS_PROCESSED_TYPE_ASSET_ALARM   = "DDS_PROCESSED_TYPE_ASSET_ALARM";
	public static final String DDS_PROCESSED_TYPE_ASSET_EVENT   = "DDS_PROCESSED_TYPE_ASSET_EVENT";
	public static final String DDS_PROCESSED_TYPE_ASSET_CONTEXT = "DDS_PROCESSED_TYPE_ASSET_CONTEXT";
	public static final String DDS_PROCESSED_TYPE_ASSET_AGGREGATION = "DDS_PROCESSED_TYPE_ASSET_AGGREGATION";
	
}
