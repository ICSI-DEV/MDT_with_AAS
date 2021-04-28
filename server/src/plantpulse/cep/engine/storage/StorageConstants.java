package plantpulse.cep.engine.storage;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * StorageConstants
 * @author lsb
 *
 */
public class StorageConstants {
	
	

	//MM
	public static final String SITE_TABLE           = "mm_site"; // 
	public static final String OPC_TABLE            = "mm_opc"; // 
	public static final String TAG_TABLE            = "mm_tag"; // 
	public static final String ALARM_TABLE          = "mm_alarm"; // 
	public static final String ASSET_TABLE          = "mm_asset"; // 
	public static final String USER_TABLE           = "mm_user"; // 
	public static final String SECURITY_TABLE       = "mm_security"; //
	public static final String METADATA_TABLE       = "mm_metadata"; //
	public static final String JSON_MODEL_TABLE     = "mm_json_model"; //
	
	//TM 
	public static final String SITE_SUMMARY_TABLE   = "tm_site_summary"; // 사이트 요약
	
	public static final String OPC_POINT_LAST_UPDATE_TABLE = "tm_opc_point_last_update";//
	
	public static final String TAG_POINT_TABLE         = "tm_tag_point"; // 태그 포인트 테이터 저장 테이블명
	public static final String TAG_POINT_BUCKET_TABLE  = "tm_tag_point_bucket"; // 태그 포인트 테이터 저장 테이블명
	public static final String TAG_POINT_RAW_TABLE     = "tm_tag_point_raw"; // 태그 포인트 테이터 저장 테이블명
	public static final String TAG_POINT_MATRIX_TABLE  = "tm_tag_point_matrix"; // 태그 포인트 테이터 저장 테이블명
	
	public static final String TAG_POINT_LIMIT_1_DAYS_TABLE  = "tm_tag_point_limit_1_days";  // 태그 포인트 데이터 제한 테이블명
	public static final String TAG_POINT_LIMIT_7_DAYS_TABLE  = "tm_tag_point_limit_7_days";  // 
	public static final String TAG_POINT_LIMIT_15_DAYS_TABLE = "tm_tag_point_limit_15_days"; // 
	public static final String TAG_POINT_LIMIT_31_DAYS_TABLE = "tm_tag_point_limit_31_days"; // 
	
	public static final String TAG_BLOB_TABLE        = "tm_tag_blob";
	public static final String TAG_BLOB_OBJECT_TABLE        = "tm_tag_blob_object";
	
	public static final String TAG_POINT_MAP_TABLE = "tm_tag_point_map"; // 태그 포인트 테이터 맵 저장 테이블명
	public static final String TAG_POINT_MAP_BINARY_TABLE = "tm_tag_point_map_binary"; // 태그 포인트 테이터 맵 바이너리 저장 테이블명
	public static final String TAG_POINT_COUNT_TABLE = "tm_tag_point_count"; // 태그 카운트 저장
	public static final String TAG_POINT_COUNT_BY_HOUR_TABLE = "tag_point_count_by_hour_table"; // 태그 카운트 시간별 저장
	
	public static final String TAG_POINT_COUNT_BY_SITE_TABLE = "tm_tag_point_count_by_site"; //
	public static final String TAG_POINT_COUNT_BY_OPC_TABLE = "tm_tag_point_count_by_opc";//
	public static final String TAG_POINT_SAMPLING_TABLE = "tm_tag_point_sampling";
	public static final String TAG_POINT_AGGREGATION_TABLE = "tm_tag_point_aggregation";
	public static final String TAG_POINT_SNAPSHOT_TABLE = "tm_tag_point_snapshot";
	public static final String TAG_POINT_ARCHIVE_TABLE = "tm_tag_point_archive";
	
	public static final String TAG_ARLAM_TABLE   = "tm_tag_alarm";
	public static final String TAG_ARLAM_ON_TABLE   = "tm_tag_alarm_on";
	public static final String TAG_ARLAM_COUNT_TABLE   = "tm_tag_alarm_count";
	
	
	public static final String ASSET_TIMELINE_TABLE ="tm_asset_timeline";
	public static final String ASSET_DATA_TABLE ="tm_asset_data";
	public static final String ASSET_DATA_BY_DATE_TABLE ="tm_asset_data_by_date";
	public static final String ASSET_TEMPLATE_DATA_TABLE_PREFIX ="tm_asset_template_data_";
	public static final String ASSET_ALARM_TABLE   = "tm_asset_alarm";
	public static final String ASSET_HEALTH_STATUS_TABLE = "tm_asset_health_status";
	public static final String ASSET_CONNECTION_STATUS_TABLE = "tm_asset_connection_status";
	public static final String ASSET_EVENT_TABLE = "tm_asset_event";
	public static final String ASSET_AGGREGATION_TABLE = "tm_asset_aggregation";
	public static final String ASSET_CONTEXT_TABLE = "tm_asset_context";
	
	public static final String DOMAIN_CHANGE_HISTORY_TABLE = "tm_domain_change_history";
	
	public static final String OPTION_LISTENER_PUSH_CACHE_TABLE = "tm_option_listener_push_cache";

	public static final String OPTION_SYSTEM_START_DATE         = "tm_system_start_date";
	public static final String OPTION_SYSTEM_PROPERTIES         = "tm_system_properties";

	public static boolean  SAMPLING_ENABLED = true; 
	public static boolean  AGGREGATION_ENABLED = true;  ///
	
	public static String[] SAMPLING_TERMS_ARRAY    = new String[] { "10 SECONDS", "30 SECONDS", "1 MINUTES", "5 MINUTES", "10 MINUTES", "30 MINUTES", "1 HOURS" }; //
	public static String[] AGGREGATION_TERMS_ARRAY = new String[] { "1 MINUTES", "5 MINUTES", "10 MINUTES", "30 MINUTES", "1 HOURS" }; //
	public static String[] SNAPSHOT_TERMS_ARRAY    = new String[] { "1 MINUTES", "5 MINUTES", "10 MINUTES", "30 MINUTES", "1 HOURS", "3 HOURS", "6 HOURS", "12 HOURS", "1 DAYS" }; //
	//public static String[] ARCHIVE_TERMS_ARRAY    = new String[] { "60_SECONDS_8_DAYS", "1_HOURS_30_DAYS", "1_DAYS_1_YEARS" }; //
	
	public static String   ASSET_HEALTH_TERM      =  "10 MINUTES";
	public static String   ASSET_CONNECTION_TERM  =  "10 MINUTES";
	public static String   SYSTEM_HEALTH_TERM     =  "1 MINUTES";
	
	//PP_SYSTEM
	
	public static final String SYSTEM_KEYSPACE            = "pp_system";
	public static final String SYSTEM_LOG_TABLE           = "tm_system_log";
	public static final String SYSTEM_HEALTH_STATUS_TABLE = "tm_system_health_status";
	
//
	public static String       SYSTEM_APP_NAME            =  "SERVER";
	
	public static List<Map<String, String>> getSamplingOptions() {
		List<Map<String, String>> list = new ArrayList<Map<String, String>>();
		for (int i = 0; i < SAMPLING_TERMS_ARRAY.length; i++) {
			Map<String, String> map = new HashMap<String, String>();
			map.put("key", SAMPLING_TERMS_ARRAY[i].replaceAll(" ", "_"));
			map.put("value", replaceName((SAMPLING_TERMS_ARRAY[i])));
			list.add(map);
		}
		return list;
	}
	
	public static List<Map<String, String>> getAggregationOptions() {
		List<Map<String, String>> list = new ArrayList<Map<String, String>>();
		for (int i = 0; i < AGGREGATION_TERMS_ARRAY.length; i++) {
			Map<String, String> map = new HashMap<String, String>();
			map.put("key", AGGREGATION_TERMS_ARRAY[i].replaceAll(" ", "_"));
			map.put("value", replaceName((AGGREGATION_TERMS_ARRAY[i])));
			list.add(map);
		}
		return list;
	}

	public static List<Map<String, String>> getSnapshotOptions() {
		List<Map<String, String>> list = new ArrayList<Map<String, String>>();
		for (int i = 0; i < SNAPSHOT_TERMS_ARRAY.length; i++) {
			Map<String, String> map = new HashMap<String, String>();
			map.put("key", SNAPSHOT_TERMS_ARRAY[i].replaceAll(" ", "_"));
			map.put("value", replaceName((SNAPSHOT_TERMS_ARRAY[i])));
			list.add(map);
		}
		return list;
	}

	public static String replaceName(String key) {
		if (key.indexOf("SECONDS") > -1) {
			return key.replaceAll("SECONDS", "초");
		} else if (key.indexOf("MINUTES") > -1) {
			return key.replaceAll("MINUTES", "분");
		} else if (key.indexOf("HOURS") > -1) {
			return key.replaceAll("HOURS", "시간");
		} else if (key.indexOf("DAYS") > -1) {
			return key.replaceAll("DAYS", "일");
		} else {
			return "";
		}
	}
	
	
	public static int convertMinutes(String key) {
		int min = 0;
		String[] spl = key.split("");
		int a1 = Integer.parseInt(spl[0]);
		int a2 = spl[1].startsWith("MIN") ? 1 : 60;
		min = a1 * a2;
		return min;
	}

}
