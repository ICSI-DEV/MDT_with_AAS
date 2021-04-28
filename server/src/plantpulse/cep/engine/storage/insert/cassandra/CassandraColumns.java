package plantpulse.cep.engine.storage.insert.cassandra;

/**
 * 카산드라 숏 컬럼명 (데이터 용량 축소를 위함.)
 * 
 * @author lsb
 *
 */
public class CassandraColumns {

	public static final String SITE_ID = "site_id";
	public static final String OPC_ID = "opc_id";
	// 공통
	public static final String TAG_ID = "tag_id";
	public static final String TIMESTAMP = "timestamp";
	

	// 태그
	/*
	public static final String STRING_VALUE = "string_value";
	public static final String INT_VALUE = "int_value";
	public static final String LONG_VALUE = "long_value";
	public static final String FLOAT_VALUE = "float_value";
	public static final String DOUBLE_VALUE = "double_value";
	public static final String BOOLEAN_VALUE = "boolean_value";
	public static final String TIMESTAMP_VALUE = "timestamp_value";
	*/
	public static final String BUCKET = "bucket";
	
	public static final String DATE = "date";
	
	public static final String VALUE = "value";
	public static final String TYPE = "type";
	
	public static final String QUALITY = "quality";
	public static final String ERROR_CODE = "error_code";
	public static final String ATTRIBUTE = "attribute";

	// 맵
	public static final String MAP_ID = "map_id";
		
	// 스냅샷
	public static final String MAP = "map";
	
	public static final String DATA_MAP = "data_map";
	public static final String TIMESTAMP_MAP = "timestamp_map";
	public static final String ALARM_MAP = "alarm_map";
	
	public static final String POINT_MAP = "point_map";

	// 집계
	public static final String COUNT = "count";
	public static final String FIRST = "first";
	public static final String LAST = "last";
	public static final String MIN = "min";
	public static final String MAX = "max";
	public static final String AVG = "avg";
	public static final String SUM = "sum";
	public static final String STDDEV = "stddev";
	public static final String MEDIAN = "median";

	// 스냅샷
	public static final String SNAPSHOT_ID = "snapshot_id";

	// 트리거
	public static final String TRIGGER_ID = "trigger_id";
	public static final String TIME_UUID = "tid";

	// 알람
	public static final String PRIORITY = "priority";
	public static final String DESCRIPTION = "description";
	public static final String ALARM_SEQ   = "alarm_seq";
	public static final String ALARM_CONFIG_ID = "alarm_config_id";
	
	// 에셋 헬쓰
	public static final String ASSET_ID = "asset_id";
	public static final String ASSET_TYPE = "asset_type";
	public static final String FROM = "from_timestamp";
	public static final String TO   = "to_timestamp";
	public static final String STATUS   = "status";
	public static final String ALARM_INFO_COUNT    = "alarm_info_count";
	public static final String ALARM_WARN_COUNT    = "alarm_warn_count";
	public static final String ALARM_ERROR_COUNT   = "alarm_error_count";
	
	//에셋 타임라인
	public static final String TIMELINE_TYPE   = "timeline_type";
	
	//에셋 이벤트
	public static final String EVENT   = "event";
	public static final String COLOR   = "color";
	public static final String NOTE    = "note";
	
	//에셋 집계
	public static final String AGGREGATION    = "aggregation";
	
	//에셋 상황
	public static final String CONTEXT    = "context";
	public static final String KEY   = "key";
	
	
	// 사이트 요약
	public static final String YEAR = "year";
	public static final String MONTH = "month";
	public static final String DAY = "day";
	public static final String HOUR = "hour";
	public static final String MINUTE = "minute";
	public static final String SECOND = "second";

	public static final String DATA_COUNT = "data_count";
	public static final String ALARM_COUNT = "alarm_count";

	public static final String ALARM_COUNT_BY_INFO = "alarm_count_by_info";
	public static final String ALARM_COUNT_BY_WARN = "alarm_count_by_warn";
	public static final String ALARM_COUNT_BY_ERROR = "alarm_count_by_error";
	
	
	public static final String APP_NAME = "app_name";

	//
	public static final String SITE_NAME = "site_name";
	public static final String LAT = "lat";
	public static final String LNG = "lng";
	public static final String INSERT_DATE = "insert_date";
	public static final String UPDATE_DATE = "update_date";
	
	public static final String OPC_NAME = "opc_name";
	public static final String OPC_TYPE = "opc_type";
	public static final String OPC_SERVER_IP = "opc_server_ip";
	

	public static final String TAG_NAME = "tag_name";
	public static final String TAG_SOURCE = "tag_source";
	public static final String JAVA_TYPE = "java_type";
	
	public static final String ALIAS_NAME = "alias_name";
	public static final String IMPORTANCE = "importance";
	public static final String INTERVAL = "interval";
	public static final String UNIT = "unit";
	public static final String TRIP_HI = "trip_hi";
	public static final String HI = "hi";
	public static final String HI_HI = "hi_hi";
	public static final String LO = "lo";
	public static final String LO_LO = "lo_lo";
	public static final String TRIP_LO = "trip_lo";
	public static final String MIN_VALUE = "min_value";
	public static final String MAX_VALUE = "max_value";
	public static final String DISPLAY_FORMAT = "display_format";
	public static final String LINKED_ASSET_ID = "linked_asset_id";

	
	public static final String ALARM_CONFIG_NAME = "alarm_config_name";
	public static final String ALARM_CONFIG_PRIORITY = "alarm_config_priority";
	public static final String ALARM_CONFIG_DESC = "alarm_config_desc";
	public static final String ALARM_TYPE = "alarm_type";
	public static final String EPL = "epl";
	public static final String CONDITION = "condition";
	public static final String MESSAGE = "message";
	public static final String SEND_EMAIL = "send_email";
	public static final String SEND_SMS = "send_sms";
	public static final String DUPLICATE_CHECK = "duplicate_check";
	public static final String DUPLICATE_CHECK_TIME = "duplicate_check_time";
	

	
	public static final String PARENT_ASSET_ID = "parent_asset_id";
	public static final String ASSET_NAME = "asset_name";
	public static final String ASSET_ORDER = "asset_order";
	public static final String ASSET_SVG_IMG = "asset_svg_img";
	public static final String TABLE_TYPE = "table_type";
	
	
	public static final String USER_ID = "user_id";
	public static final String SECURITY_ID = "security_id";
	public static final String PASSWORD = "password";
	public static final String ROLE = "role";
	public static final String NAME = "name";
	public static final String EMAIL = "email";
	public static final String PHONE = "phone";
	
	public static final String SECURITY_DESC = "security_desc";
	public static final String OBJECT_PERMISSION_ARRAY = "object_permission_array";
	
	public static final String RECIEVE_ME = "recieve_me";
	public static final String RECIEVE_OTHERS = "recieve_others";
	
	public static final String PATH = "path";
	public static final String PAYLOAD  = "payload";
	
	public static final String DATA   = "data";
	
	public static final String LAST_UPDATE   = "last_update";
	
	
	public static final String URL    = "url";
	public static final String JSON   = "json";
	
	public static final String DOMAIN_TYPE    = "domain_type";
	public static final String OBJECT_ID      = "object_id";
	public static final String OBJECT         = "object";
	
	//v6 추가
	public static final String STATEMENT_NAME             = "statement_name";
	public static final String STATEMENT_DESCRIPTION      = "statement_description";
	
	

	//BLOB 추가
	public static final String BLOB           = "blob";
	public static final String FILE_PATH      = "file_path";
	public static final String FILE_SIZE      = "file_size";
	public static final String MIME_TYPE      = "mime_type";
	
	//
	public static final String PROPERTY       = "property";
	public static final String CONTENT        = "content";
	public static final String RUN_DURATION   = "run_duration";
	
	public static final String YYYY        = "yyyy";
	public static final String MM          = "mm";
	public static final String DD          = "dd";
	
	
	
}
