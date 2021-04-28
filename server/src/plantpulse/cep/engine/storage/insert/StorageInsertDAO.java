package plantpulse.cep.engine.storage.insert;

import java.util.List;
import java.util.Map;

import org.json.JSONObject;

import plantpulse.cep.domain.AssetStatement;
import plantpulse.cep.domain.BLOB;
import plantpulse.cep.domain.Trigger;
import plantpulse.domain.Asset;
import plantpulse.domain.MetaModel;
import plantpulse.domain.Tag;
import plantpulse.event.opc.Alarm;
import plantpulse.event.opc.Point;

/**
 * insertAlarm
 * 
 * @author lsb
 *
 */
public interface StorageInsertDAO {

	/**
	 * 키스페이스를 생성한다.
	 * 
	 * @param site
	 * @throws Exception
	 */
	public void createKeyspace() throws Exception;

	/**
	 * 함수 및 집계를 생성한다.
	 * @throws Exception
	 */
	public void createFunctionAndAggregate() throws Exception ;
	
	
	/**
	 * 메타데이터(태그, 에셋, 유저, 권한, 알람 설정 등) 테이블을 생성한다. 
	 * @throws Exception
	 */
	public void createTableForMetadata() throws Exception;
	
	/**
	 * 데이터  포인트 및 카운트, 알람 테이블을 생성한다.
	 * 
	 * @throws Exception
	 */
	public void createTableForPointAndAlarm() throws Exception;

	/**
	 * BLOB 저장 테이블을 생성한다.
	 * @throws Exception
	 */
	public void createTableForBLOB() throws Exception;
	
	/**
	 * 샘플링 테이블을 생성한다.
	 * 
	 * @throws Exception
	 */
	public void createTableForSampling() throws Exception;
	
	/**
	 * 집계 테이블을 생성한다.
	 * 
	 * @throws Exception
	 */
	public void createTableForAggregation() throws Exception;

	/**
	 * 스냅샷 테이블을 생성한다.
	 * 
	 * @throws Exception
	 */
	public void createTableForSnapshot() throws Exception;
	
	
	/**
	 * 아카이브 테이블을 생성한다.
	 * @throws Exception
	 */
	public void createTableForArchive() throws Exception;
	
	
	
	/**
	 * 시스템 상태 테이블을 생성한다.
	 * @throws Exception
	 */
	public void createTableForSystemHealthStatus() throws Exception;
	
	
	/**
	 * 트리거 테이블을 생성한다.
	 * 
	 * @throws Exception
	 */
	public void createTableForTrigger(Trigger trigger) throws Exception;
	
	
	/**
	 * 에셋 타임라인 테이블을 생성한다.
	 * @throws Exception
	 */
	public void createTableForAssetTimeline() throws Exception;
	
	
	/**
	 * 에셋 테이블 생성한다.
	 * @throws Exception
	 */
	public void createTableForAssetData() throws Exception;
	
	
	/**
	 * 에셋 알람 테이블 생성
	 * @throws Exception
	 */
	public void createTableForAssetAlarm() throws Exception;
	
	
	/**
	 * 에셋 템플릿 테이블 생성한다.
	 * @param asset
	 * @param tag_map
	 * @throws Exception
	 */
	public void createTableForAssetTemplateData(Asset asset, Map<String, Tag> tag_map) throws Exception;

	
	/**
	 * 에셋 헬스 상태 테이블을 생성한다.
	 * @throws Exception
	 */
	public void createTableForAssetHealthStatus() throws Exception;
	
	/**
	 * 에셋 커넥션 상태 테이블을 생성한다.
	 * @throws Exception
	 */
	public void createTableForAssetConnectionStatus() throws Exception;
	
	 
	/**
	 * 에셋 이벤트 테이블을 생성한다.
	 * @throws Exception
	 */
	public void createTableForAssetEvent() throws Exception;
	
	/**
	 * 에셋 컨텍스트 테이블을 생성한다.
	 * @throws Exception
	 */
	public void createTableForAssetContext() throws Exception;
	
	/**
	 * 에셋 집계 테이블을 생성한다.
	 * @throws Exception
	 */
	public void createTableForAssetAggregation() throws Exception;
	

   /**
    * 도매인 변경 히스토리 테이블을 생성한다.
    * @throws Exception
    */
	public void createTableForDomainChangeHistory() throws Exception;
	
	
	/**
	 * 옵션 테이블을 생성한다. (캐쉬)
	 * @throws Exception
	 */
	public void createTableForOptions() throws Exception;
	
	/**
	 * 뷰 를 생성한다.
	 * 
	 * @throws Exception
	 */
	public void createMaterializedView() throws Exception;
	
	
	/**
	 * 테이블 컴팩션 전략 설정
	 * @throws Exception
	 */
	public void setupTableCompactionStrategy() throws Exception;
	
	
	/**
	 * 테이블 압축 설정
	 * @throws Exception
	 */
	public void setupTableComression() throws Exception;
	
	/**
	 * 테이블 캐시 및 TTL 옵션 설정
	 * @throws Exception
	 */
	public void setupTableCacheAndTTL() throws Exception;
	
	
	/**
	 * 복제 팩터를 변경한다.
	 * @param factor
	 * @throws Exception
	 */
	public void setupReplicationFactor(int factor) throws Exception;

	
	/**
	 * 메터데이터 테이블을 업데이트 한다.
	 * 
	 * @param meta
	 * @throws Exception
	 */
	public void updateMetadata(MetaModel meta) throws Exception;
	
	/**
	 * 시스템 러닝 듀레이션을 업데이트한다.
	 * @throws Exception
	 */
	public void updateSystemRunDuration() throws Exception;
	
	/**
	 * 트리거의 수신 데이터를 저장한다.
	 * 
	 * @param trigger
	 * @param timestamp
	 * @param data
	 * @throws Exception
	 */
	public void insertTrigger(Trigger trigger, long timestamp, JSONObject data) throws Exception;
	
	/**
	 * BLOB를 저장한다.
	 * @param blob
	 * @throws Exception
	 */
	public void insertBLOB(BLOB blob) throws Exception;

	/**
	 * 샘플링 정보를 저장한다.
	 * 
	 * @param term
	 * @param timestamp
	 * @param data
	 * @throws Exception
	 */
	public void insertPointSampling(String term, long timestamp, List<Point> point_list) throws Exception;
	

	/**
	 * 스냅샷 정보를 저장한다.
	 * 
	 * @param site_id
	 * @param term
	 * @param timestamp
	 * @param data_map
	 * @throws Exception
	 */
	public void insertPointSnapshot(String site_id, String term, long timestamp, JSONObject data_map, JSONObject timestamp_map, JSONObject alarm_map) throws Exception;
	
	
	/**
	 * 집계정보를 저장한다.
	 * 
	 * @param term
	 * @param timestamp
	 * @param data
	 * @throws Exception
	 */
	public void insertPointAggregation(Tag tag, String term, long timestamp, JSONObject json) throws Exception;

	
	/**
	 * 시간별 아카이브를 저장한다.
	 * @param tag
	 * @param date
	 * @param hour
	 * @param point_count
	 * @param map
	 * @throws Exception
	 */
	public void insertPointArchive(Tag tag, int date, int hour, int point_count, Map<String, List<String>> map) throws Exception;
	
	
	/**
	 * 포인트 ROW 데이터를 입력한다.
	 * 
	 * @param map
	 * @throws Exception
	 */
	public void insertPointMap(long timestamp, Map<String, Point> map) throws Exception;

	
	
	/**
	 * 포인트 메트릭스를 저장한다.
	 * @param timestamp
	 * @param from
	 * @param to
	 * @param map
	 * @throws Exception
	 */
	public void insertPointMatrix(int date, long timestamp, long from, long to, Map<String, List<String>> map) throws Exception;

	
	/**
	 * 알람을 저장한다.
	 * 
	 * @param data
	 * @throws Exception
	 */
	public void insertAlarm(Alarm alarm, JSONObject data) throws Exception;
	
	
	
	/**
	 * 에셋 타임라인을 저장한다.
	 * @param timestamp
	 * @param asset
	 * @param column_def
	 * @param map
	 * @throws Exception
	 */
	public void insertAssetTimeline(AssetStatement stmt, long timestamp, Asset asset, String timeline_type, String payload) throws Exception;
	
	/**
	 * 에셋 데이터를 저장한다.
	 * @param timestamp
	 * @param asset
	 * @param column_def
	 * @param map
	 * @throws Exception
	 */
	public void insertAssetData(long timestamp, Asset asset, Map<String, Tag> tag_map, Map<String, Point> point_map) throws Exception;
	
	
	/**
	 * 에셋 데이터 샘플링을 저장한다.
	 * @param term
	 * @param timestamp
	 * @param asset
	 * @param tag_map
	 * @param point_map
	 * @throws Exception
	 */
	public void insertAssetDataSampling(String term, long timestamp, Asset asset, Map<String, Tag> tag_map, Map<String, Point> point_map) throws Exception;
	
	
	
	/**
	 * 에셋 헬스 상태를 저장한다.
	 * @param current_timestamp
	 * @param model
	 * @param status
	 * @param info_count
	 * @param warn_count
	 * @param error_count
	 */
	public void insertAssetHealthStatus(long current_timestamp, Asset model, long from_timestamp, long to_timestamp, String status, int info_count, int warn_count, int error_count)  throws Exception ;
	
	
	/**
	 * 에셋 연결 상태를 저장한다.
	 * @param current_timestamp
	 * @param model
	 * @param status
	 * @param info_count
	 * @param warn_count
	 * @param error_count
	 */
	public void insertAssetConnectionStatus(long current_timestamp, Asset model, long from_timestamp, long to_timestamp, String status)  throws Exception ;
	
	
	
	/**
	 * 에셋 이벤트를 저장한다.
	 * @param current_timestamp
	 * @param model
	 * @param status
	 * @param info_count
	 * @param warn_count
	 * @param error_count
	 */
	public void insertAssetEvent(AssetStatement stmt, long current_timestamp, Asset model, String event, long from_timestamp, long to_timestamp, String color, Map<String, String> data, String note)  throws Exception ;


	/**
	 * 에셋 컨텍스트를 저장한다.
	 * @param current_timestamp
	 * @param model
	 * @param status
	 * @param info_count
	 * @param warn_count
	 * @param error_count
	 */
	public void insertAssetContext(AssetStatement stmt, long current_timestamp, Asset model, String context, String key, String value)  throws Exception ;


	/**
	 * 에셋 집계를 저장한다.
	 * @param current_timestamp
	 * @param model
	 * @param status
	 * @param info_count
	 * @param warn_count
	 * @param error_count
	 */
	public void insertAssetAggregation(AssetStatement stmt, long current_timestamp, Asset model, String aggregation, String key, String value)  throws Exception ;

	
	/**
	 * 시스템 상태를 저장한다.
	 * @param current_timestamp
	 * @param model
	 * @param status
	 * @param info_count
	 * @param warn_count
	 * @param error_count
	 */
	public void insertSystemHealthStatus(long current_timestamp, long from_timestamp, long to_timestamp, String status, int info_count, int warn_count, int error_count) throws Exception ;

	
	/**
	 * 도매인 변경 히스토리를 저장한다.
	 * 
	 * @param current_timestamp
	 * @param domain_type
	 * @param object_id
	 * @param json
	 * @throws Exception
	 */
	public void insertDomainChangeHistory(long current_timestamp, String domain_type, String object_id, String json) throws Exception;

	/**
	 * 푸시 데이터 캐시를 저장한다.
	 * @param current_timestamp
	 * @param url
	 * @param json
	 * @throws Exception
	 */
	public void insertListenerPushCache(long current_timestamp, String url, String json) throws Exception;
}
