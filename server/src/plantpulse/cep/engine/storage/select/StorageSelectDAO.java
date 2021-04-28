package plantpulse.cep.engine.storage.select;

import java.util.List;
import java.util.Map;
import java.util.UUID;

import org.json.JSONArray;
import org.json.JSONObject;

import plantpulse.domain.Asset;
import plantpulse.domain.OPC;
import plantpulse.domain.Site;
import plantpulse.domain.Tag;

/**
 * 
 * @author lsb
 *
 */
public interface StorageSelectDAO {

	/**
	 * 사이트 요약을 조회한다 조회한다.
	 * 
	 * @param tag
	 * @param from
	 * @param to
	 * @param aggregation
	 * @param term
	 * @return
	 * @throws Exception
	 */
	public JSONObject selectSiteSummary(Site site, int year, int month, int day) throws Exception;
	
	
	/**
	 * OPC(연결)의 포인트 마지막 업데이트 시간을 조회한다.
	 * 
	 * @param tag
	 * @param from
	 * @param to
	 * @param aggregation
	 * @param term
	 * @return
	 * @throws Exception
	 */
	public JSONObject selectOPCPointLastUpdate(OPC opc) throws Exception;

	
	
	/**
	 *  태그 데이터 목록을 조회한다.
	 * @param tag
	 * @param from
	 * @param to
	 * @param limit
	 * @return
	 * @throws Exception
	 */
	public List<Map<String, Object>> selectPointList(Tag tag, String from, String to) throws Exception;
	

	/**
	 *  태그 데이터 목록을 조회한다.
	 * @param tag
	 * @param from
	 * @param to
	 * @param limit
	 * @return
	 * @throws Exception
	 */
	public List<Map<String, Object>> selectPointList(Tag tag, String from, String to, int limit) throws Exception;
	
	/**
	 * 태그 데이터 목록을 조회한다.
	 * 
	 * @param tag
	 * @param from
	 * @param to
	 * @param limit
	 * @param condition
	 * @return
	 * @throws Exception
	 */
	public List<Map<String, Object>> selectPointList(Tag tag, String from, String to, int limit, String condition) throws Exception;
	
	/**
	 * selectLastPoint
	 * @param tag
	 * @param from
	 * @param to
	 * @param limit
	 * @param condition
	 * @return
	 * @throws Exception
	 */
	public Map<String, Object> selectPointLast(Tag tag, String from, String to, String condition) throws Exception;
	
	
	/**
	 * selectLastPoint
	 * @param tag
	 * @param from
	 * @param to
	 * @param limit
	 * @param condition
	 * @return
	 * @throws Exception
	 */
	public Map<String, Object> selectPointLast(Tag tag) throws Exception;
	
	
	/**
	 * 설정된 갯수만큼의 마지막 포인트 리스트를 반환한다.
	 * @param tag
	 * @param limit
	 * @return
	 * @throws Exception
	 */
	public List<Map<String, Object>> selectPointLast(Tag tag, int limit) throws Exception;

	
	/**
	 * selectLastPoint
	 * @param tag
	 * @param from
	 * @param to
	 * @param limit
	 * @param condition
	 * @return
	 * @throws Exception
	 */
	public Map<String, Object> selectPointLast(Tag tag, String to) throws Exception;
	
	
	/**
	 * 타임스탬프 이후의  포인트 데이터를 반환한다.
	 * @param tag
	 * @param timestamp
	 * @return
	 * @throws Exception
	 */
	public  List<Map<String, Object>>  selectPointAfterTimestamp(Tag tag, long timestamp) throws Exception;

	
	/**
	 * 포인트 목록을 조회한다.
	 * 
	 * @param site
	 * @param tag
	 * @param from
	 * @param to
	 * @param aggregation
	 * @return
	 * @throws Exception
	 */
	public JSONArray selectPointValueList(Tag tag, String from, String to, String sampling, String term) throws Exception;
	
	
	/**
	 * 포인트 목록을 조회한다.
	 * 
	 * @param tag
	 * @param from
	 * @param to
	 * @param sampling
	 * @param term
	 * @param limit
	 * @return
	 * @throws Exception
	 */
	public JSONArray selectPointValueList(Tag tag, String from, String to, String sampling, String term, int limit) throws Exception;
	
	
	/**
	 * 포인트 목록을 조회한다.
	 * 
	 * @param site
	 * @param tag
	 * @param from
	 * @param to
	 * @param aggregation
	 * @return
	 * @throws Exception
	 */
	public JSONArray selectPointValueList(Tag tag, int limit) throws Exception;
	
	/**
	 * 태그 요약 집계(COUNT/AVG/MIN/MAX/SUM)을 조회한다.
	 * 
	 * @param tag
	 * @param from
	 * @param to
	 * @param aggregation
	 * @param term
	 * @return
	 * @throws Exception
	 */
	public JSONObject selectPointDataForAggregatation(Tag tag, long current_timestamp, String from, String to) throws Exception;
	
	
	/**
	 * 태그 요약 집계(COUNT/AVG/MIN/MAX/SUM)을 조회한다.
	 * 
	 * @param tag
	 * @param from
	 * @param to
	 * @param aggregation
	 * @param term
	 * @return
	 * @throws Exception
	 */
	public JSONArray selectPointDataForAggregatationByType(String type, long current_timestamp, String from, String to) throws Exception;
	
	
	/**
	 * 태그 요약 집계(COUNT/AVG/MIN/MAX/SUM)을 조회한다.
	 * 
	 * @param tag
	 * @param from
	 * @param to
	 * @param aggregation
	 * @param term
	 * @return
	 * @throws Exception
	 */
	public JSONObject selectPointDataForSnapshot(Tag tag, long current_timestamp, String from, String to) throws Exception;
	
	
	
	/**
	 * BLOB 데이터 목록을 조회한다.
	 * @param tag
	 * @param from
	 * @param to
	 * @param limit
	 * @return
	 * @throws Exception
	 */
	public List<Map<String, Object>> selectBLOBList(String tag_id, String from, String to, int limt) throws Exception;
	
	/**
	 * BLOB 데이터를 조회한다.
	 * @param object_id
	 * @return
	 * @throws Exception
	 */
	public Map<String, Object> selectBLOBByObjectId(UUID object_id) throws Exception;
	
	/**
	 * BLOB 데이터 목록을 조회한다.
	 * @param tag
	 * @param from
	 * @param to
	 * @param limit
	 * @return
	 * @throws Exception
	 */
	public Map<String, Object> selectBLOBObject(UUID object_id) throws Exception;
	
	
	/**
	 * 
	 * @param tag
	 * @param current_timestamp
	 * @param from
	 * @param to
	 * @return
	 * @throws Exception
	 */
	public JSONObject selectAlarmCount(Tag tag, long current_timestamp, String from, String to) throws Exception;
	
	
	/**
	 * 
	 * @param asset
	 * @param current_timestamp
	 * @param from
	 * @param to
	 * @return
	 * @throws Exception
	 */
	public JSONObject selectAlarmCountByAssetId(Asset asset, long current_timestamp, String from, String to) throws Exception;
	
	
	/**
	 * 
	 * @param tag
	 * @param year
	 * @param month
	 * @param day
	 * @return
	 * @throws Exception
	 */
	public JSONObject selectAlarmCountByDate(Tag tag, int year, int month, int day) throws Exception;
	
	/**
	 * 
	 * @param tag
	 * @param current_timestamp
	 * @param from
	 * @param to
	 * @return
	 * @throws Exception
	 */
	public JSONObject selectAlarmErrorCountForSnapshot(Tag tag, long current_timestamp, String from, String to) throws Exception;
	

	/**
	 * 태그 요약 집계(COUNT/AVG/MIN/MAX/SUM)을 조회한다.
	 * 
	 * @param tag
	 * @param from
	 * @param to
	 * @param aggregation
	 * @param term
	 * @return
	 * @throws Exception
	 */
	public JSONObject selectPointAggregatation(Tag tag, String from, String to, String sampling, String term) throws Exception;

	/**
	 * 태그 요약 집계(COUNT/AVG/MIN/MAX/SUM)을 조회한다.
	 * 
	 * @param tag
	 * @param from
	 * @param to
	 * @param aggregation
	 * @param term
	 * @return
	 * @throws Exception
	 */
	public JSONArray selectPointSnapshot(String site_id, String from, String to, String term, int limit) throws Exception;
	
	
	/**
	 * 에셋 타임라인를 조회한다.
	 * 
	 * @param asset
	 * @param from
	 * @param to
	 * @param limit
	 * @return
	 * @throws Exception
	 */
	public JSONArray  selectAssetTimeline(Asset asset, String from, String to, int limit) throws Exception;
	
	/**
	 * 에셋 데이터를 조회한다.
	 * 
	 * @param tag
	 * @param from
	 * @param to
	 * @param aggregation
	 * @param term
	 * @return
	 * @throws Exception
	 */
	public JSONArray selectAssetData(Asset asset, int date, String from, String to, int limit) throws Exception;
	
	
	/**
	 * 에셋 데이터를 조회한다.
	 * 
	 * @param tag
	 * @param from
	 * @param to
	 * @param aggregation
	 * @param term
	 * @return
	 * @throws Exception
	 */
	public JSONArray selectAssetAlarm(Asset asset, String from, String to, int limit) throws Exception;
	
	
	/**
	 * 에셋 알람 카운트를 조회한다.
	 * 
	 * @param asset
	 * @param from
	 * @param to
	 * @param priority
	 * @param limit
	 * @return
	 * @throws Exception
	 */
	public JSONObject selectAssetAlarmCountByPriority(Asset asset, String from, String to, String priority, int limit) throws Exception;
	
	
	
	/**
	 * 에셋 이벤트를 조회한다.
	 * 
	 * @param tag
	 * @param from
	 * @param to
	 * @param aggregation
	 * @param term
	 * @return
	 * @throws Exception
	 */
	public JSONArray selectAssetEvent(Asset asset, String from, String to, int limit) throws Exception;
	
	/**
	 * 에셋 이벤트를 조회한다.
	 * @param asset
	 * @param limit
	 * @return
	 * @throws Exception
	 */
	public JSONArray selectAssetEvent(Asset asset, int limit) throws Exception;
	
	/**
	 * 에셋 집계를 조회한다.
	 * 
	 * @param tag
	 * @param from
	 * @param to
	 * @param aggregation
	 * @param term
	 * @return
	 * @throws Exception
	 */
	public JSONArray selectAssetAggregation(Asset asset, String from, String to, int limit) throws Exception;
	
	/**
	 * 에셋 상황를 조회한다.
	 * 
	 * @param tag
	 * @param from
	 * @param to
	 * @param aggregation
	 * @param term
	 * @return
	 * @throws Exception
	 */
	public JSONArray selectAssetContext(Asset asset, String from, String to, int limit) throws Exception;
	
	/**
	 * 에셋 헬스 상태를 조회한다.
	 * 
	 * @param tag
	 * @param from
	 * @param to
	 * @param aggregation
	 * @param term
	 * @return
	 * @throws Exception
	 */
	public JSONArray selectAssetHealthStatus(Asset asset, String from, String to) throws Exception;
	
	
	/**
	 * 에셋 연결 상태를 조회한다.
	 * 
	 * @param asset
	 * @param from
	 * @param to
	 * @return
	 * @throws Exception
	 */
	public JSONArray selectAssetConnectionStatus(Asset asset, String from, String to) throws Exception;
	
	
	/**
	 * 시스템 상태를 조회한다.
	 * 
	 * @param tag
	 * @param from
	 * @param to
	 * @param aggregation
	 * @param term
	 * @return
	 * @throws Exception
	 */
	public JSONArray selectSystemHealthStatus(String from, String to) throws Exception;

	
	/**
	 * 시스템 로그 메세지를 조회한다.
	 * @param from
	 * @param to
	 * @param app_name
	 * @param level
	 * @param message
	 * @oaram limit
	 * @return
	 * @throws Exception
	 */
	public JSONArray selectSystemLogList(String from, String to, String app_name, String level, String message, int limit) throws Exception;
	
	
	/**
	 * 푸시 캐쉬 목록을 조회한다.
	 * @param url
	 * @param limit
	 * @return
	 * @throws Exception
	 */
	public JSONArray selectListenerPushCacheList(String url, int limit) throws Exception ;
	
	

	/**
	 * 저장된 전체 태그 카운트를 반환한다.
	 * 
	 * @param site
	 * @return
	 * @throws Exception
	 */
	public long countPointTotal() throws Exception;

	/**
	 * 저장된 전체 사이트 카운트를 반환한다.
	 * 
	 * @param site
	 * @return
	 * @throws Exception
	 */
	public long countPointTotalBySite(Site site) throws Exception;

	
	/**
	 * 저장된 전체 OPC 카운트를 반환한다.
	 * 
	 * @param site
	 * @return
	 * @throws Exception
	 */
	public long countPointTotalByOPC(OPC opc) throws Exception;

	/**
	 * 태그별 저장된 카운트를 반환한다.
	 * 
	 * @param tag
	 * @return
	 * @throws Exception
	 */
	public long countPoint(Tag tag) throws Exception;


	/**
	 * 마지막 클러스터 상태 목록을 반환한다.
	 * 
	 * @return
	 * @throws Exception
	 */
	public JSONObject selectClusterStatusLast() throws Exception ;
	
	
	/**
	 * 마지막 클러스터 매트릭 목록을 반환한다.
	 * @return
	 * @throws Exception
	 */
	public JSONObject selectClusterMetricsLast() throws Exception;


	
	

}
