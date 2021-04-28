package plantpulse.cep.engine.storage.insert.cassandra;

import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.json.JSONArray;
import org.json.JSONObject;
import org.realdisplay.framework.util.StringUtils;

import com.datastax.driver.core.BatchStatement;
import com.datastax.driver.core.BatchStatement.Type;
import com.datastax.driver.core.BoundStatement;
import com.datastax.driver.core.PreparedStatement;
import com.datastax.driver.core.Session;
import com.datastax.driver.core.Statement;

import plantpulse.buffer.db.DBDataPoint;
import plantpulse.cep.engine.storage.buffer.DBDataPointBuffer;
import plantpulse.cep.engine.storage.buffer.DBDataPointBufferFactory;
import plantpulse.cep.engine.storage.insert.BatchFailover;
import plantpulse.cep.engine.storage.insert.BatchTimerStastics;
import plantpulse.cep.engine.storage.session.CassandraSessionFixer;
import plantpulse.cep.engine.storage.timeseries.TimeSeriesDatabase;
import plantpulse.cep.engine.storage.utils.StorageUtils;
import plantpulse.event.opc.Point;

/**
 * Cassandra30BatchWriteWorkerThread
 * @author lsb
 *
 */
public class CassandraBatchWriteWorker extends CassandraInsertDAO implements Runnable {

	private static Log log = LogFactory.getLog(CassandraBatchWriteWorker.class);
	
	
	// 파티션 그룹화
	// 파티션 그룹화을 사용하지 않을 경우에는 cassandra.ymal 의 (see batch_size_fail_threshold_in_kb) 를 2MB 이상으로 증가시킨다.
	private boolean IS_PATITION_GROUPING = true; //
	
	//비동기 스테이트먼트 사용 여부 (기본은 배치모드)
	private boolean USE_ASYNC_STATEMENT = true; //
	
	
	private int timer_number = 0;
	private int thread_number = 0;
	
	private  TimeSeriesDatabase timeseries_db = null;

	public CassandraBatchWriteWorker(int timer_number, int thread_number, boolean patition_grouping, boolean use_async_statement, TimeSeriesDatabase timeseries_db) {
		this.timer_number = timer_number;
		this.thread_number = thread_number;
		this.timeseries_db = timeseries_db;
		this.IS_PATITION_GROUPING = patition_grouping;
		this.USE_ASYNC_STATEMENT = use_async_statement;
	}

	@Override
	public void run() {

		//
		Thread.currentThread().setName("PP_STORAGE_BATCH" + "_" + timer_number + "_" + thread_number);
			
		//	
		List<DBDataPoint> db_data_list = null;
		
		try {

			BatchTimerStastics.getInstance().getBATCH_STARTED().incrementAndGetAsync();
			
			//
			DBDataPointBuffer buffer = DBDataPointBufferFactory.getDBDataBuffer();
			if (buffer.size() < 1) {
				log.debug("Cassandra batch data list size zero from db-data buffer: thread_number=[" + timer_number + "-" + thread_number + "]");
				return;
			}
			;

			db_data_list = buffer.getDBDataList();
			if (db_data_list.size() == 0) {
				return;
			}
			;

			
			try {

				BatchTimerStastics.getInstance().getBATCH_ACTIVE().incrementAndGetAsync();

				log.debug("Cassandra batch starting...");
				//
				long start = System.currentTimeMillis();

				//
				Session session = CassandraSessionFixer.getSession();
				String append_columns =CassandraSessionFixer.getAppendColumns();
				
				
				long batch_total_size = db_data_list.size();
				//태그별 배치 스테이트먼트 맵 / LIST
				Map<String, List<Statement>> point_token_batch_statement_map = new HashMap<String, List<Statement>>();
				//Map<String, List<Statement>> point_raw_token_batch_statement_map = new HashMap<String, List<Statement>>();
				
				List<Statement> point_batch_statement_list = new ArrayList<Statement>();
				//List<Statement> point_raw_batch_statement_list = new ArrayList<Statement>();
				
				//카테고리별 카운트 맵
				Map<String, Long> tag_count_map = new HashMap<String, Long>();
				Map<String, Long> opc_count_map = new HashMap<String, Long>();
				Map<String, Long> site_count_map = new HashMap<String, Long>();
				//
				PreparedStatement ps_tag = CassandraBatchStatementBuilder.getInstance().getTAG_DATA_INSERT_STATEMENT();
			
				//----------------------------------------------------------------------------------------------------------
				// 1. 스테이트먼트 생성
				//----------------------------------------------------------------------------------------------------------
				for (int i = 0; i < db_data_list.size(); i++) {
					//
					Point data = db_data_list.get(i).getPoint();
					//

					// 1.태그 포인트 데이터 등록
					BoundStatement p_query = new BoundStatement(ps_tag);
					p_query.setIdempotent(true);
					
					p_query.setString(CassandraColumns.TAG_ID, data.getTag_id());
					p_query.setTimestamp(CassandraColumns.TIMESTAMP, new Date(data.getTimestamp()));
					p_query.setString(CassandraColumns.VALUE, data.getValue());
					p_query.setString(CassandraColumns.TYPE, data.getType());
					p_query.setInt(CassandraColumns.QUALITY, data.getQuality());
					p_query.setInt(CassandraColumns.ERROR_CODE, data.getError_code());
					p_query.setMap(CassandraColumns.ATTRIBUTE, data.getAttribute());
					p_query.setLong(7, data.getTimestamp());
					
					// 1.태그 포인트 데이터 원본 등록
					/*
					BoundStatement p_raw_query = new BoundStatement(ps_tag_raw);
					p_raw_query.setIdempotent(true);
					//
					LocalDate ld = LocalDate.now();
					p_raw_query.setInt(CassandraColumns.YYYY, ld.getYear());
					p_raw_query.setInt(CassandraColumns.MM,   ld.getMonthValue());
					//
					p_raw_query.setString(CassandraColumns.TAG_ID, data.getTag_id());
					p_raw_query.setTimestamp(CassandraColumns.TIMESTAMP, new Date(data.getTimestamp()));
					p_raw_query.setString(CassandraColumns.VALUE, data.getValue());
					p_raw_query.setString(CassandraColumns.TYPE, data.getType());
					p_raw_query.setInt(CassandraColumns.QUALITY, data.getQuality());
					p_raw_query.setInt(CassandraColumns.ERROR_CODE, data.getError_code());
					p_raw_query.setMap(CassandraColumns.ATTRIBUTE, data.getAttribute());
					*/
					
					//p_query.setDefaultTimestamp(data.getTimestamp());
					
					//
					JSONArray columns = JSONArray.fromObject(append_columns);
					for(int x=0; columns != null && x < columns.size(); x++){
						JSONObject column = columns.getJSONObject(x);
						Map<String,String> attribute = data.getAttribute();
						if(attribute != null && attribute.containsKey(column.getString("column"))){
							String value = attribute.get(columns.getJSONObject(x).getString("column"));
							if(value != null && StringUtils.isNotEmpty(value)){
								//
								String type = column.getString("type");
								if(type.equals("int")){
									p_query.setInt(column.getString("column"), Integer.parseInt(attribute.get(column.getString("column"))));
								}else if(type.equals("bingint")){
									p_query.setLong(column.getString("column"), Long.parseLong(attribute.get(column.getString("column"))));
								}else if(type.equals("float")){
									p_query.setFloat(column.getString("column"), Float.parseFloat(attribute.get(column.getString("column"))));
								}else if(type.equals("double")){
									p_query.setDouble(column.getString("column"), Double.parseDouble(attribute.get(column.getString("column"))));
								}else if(type.equals("varchar")){
									p_query.setString(column.getString("column"), attribute.get(column.getString("column")));
								}else if(type.equals("boolean")){
									p_query.setBool(column.getString("column"), Boolean.parseBoolean(attribute.get(column.getString("column"))));
								}else{
									log.warn("Unsupport custom column type : " + type);
								}
							}else{
								p_query.setToNull(column.getString("column"));
							}
						}else{
							p_query.setToNull(column.getString("column"));
						}
					};
				
					// 2.포인트 버켓 데이터 등록
					
					
					//3.태그 포인트 스테이트먼트 맵  생성 (토큰 배치)
					if(IS_PATITION_GROUPING){
						if(point_token_batch_statement_map.containsKey(data.getTag_id())){
							point_token_batch_statement_map.get(data.getTag_id()).add(p_query);
							//point_raw_token_batch_statement_map.get(data.getTag_id()).add(p_raw_query);
							
						}else{
							point_token_batch_statement_map.put(data.getTag_id(), new ArrayList<Statement>()); //신규 스테이먼트 리스트 생성
							point_token_batch_statement_map.get(data.getTag_id()).add(p_query);
							
							//point_raw_token_batch_statement_map.put(data.getTag_id(), new ArrayList<Statement>()); //신규 스테이먼트 리스트 생성
							//point_raw_token_batch_statement_map.get(data.getTag_id()).add(p_raw_query);
							
						};
					}else{ 
						//일반 배치
						point_batch_statement_list.add(p_query);
						//point_raw_batch_statement_list.add(p_raw_query);
						
					};

					// 2.태그 카운트 맵 생성
					if (tag_count_map.containsKey(data.getTag_id())) {
						tag_count_map.put(data.getTag_id(), tag_count_map.get(data.getTag_id()) + 1);
					} else {
						tag_count_map.put(data.getTag_id(), new Long(1));
					}
					;

					// 2.OPC 카운트 맵 생성
					if (opc_count_map.containsKey(data.getOpc_id())) {
						opc_count_map.put(data.getOpc_id(), opc_count_map.get(data.getOpc_id()) + 1);
					} else {
						opc_count_map.put(data.getOpc_id(), new Long(1));
					}
					;

					// 3.사이트 카운트 맵 생성
					if (site_count_map.containsKey(data.getSite_id())) {
						site_count_map.put(data.getSite_id(), site_count_map.get(data.getSite_id()) + 1);
					} else {
						site_count_map.put(data.getSite_id(), new Long(1));
					}
					;
					
				}; // tag point list for end
				
				
				//----------------------------------------------------------------------------------------------------------
				// 2. 포인트 배치
				//----------------------------------------------------------------------------------------------------------
				if(IS_PATITION_GROUPING){
					// 파티션 배치 (태그 데이터 배치 입력 실행 by 태그 ID 파티션)
					if (point_token_batch_statement_map.size() > 0) {
						for( String key : point_token_batch_statement_map.keySet() ){
							executeBatch(session, point_token_batch_statement_map.get(key));
							//executeBatch(session, point_raw_token_batch_statement_map.get(key));
						};
				    }
				}else{
					//일반 배치
				    if (point_batch_statement_list.size() > 0) {
				    	if(USE_ASYNC_STATEMENT) {
				    		for(int i=0; i < point_batch_statement_list.size(); i++) {
				    			session.executeAsync(point_batch_statement_list.get(i));
				    		}
				    	}else {
				    		executeBatch(session, point_batch_statement_list);	
				    	}		
				    }
				};

				
				//----------------------------------------------------------------------------------------------------------
				// 3. OPC 포인트 마지막 업데이트
				//----------------------------------------------------------------------------------------------------------
				List<Statement> opc_value_update_list = new ArrayList<>();
				PreparedStatement ps_opc_last_update = CassandraBatchStatementBuilder.getInstance().getOPC_LAST_POINT_UPDATE_STATEMENT();
				if (opc_count_map.size() > 0) {
					for( String key : opc_count_map.keySet() ){
						BoundStatement query_opc_last_update = new BoundStatement(ps_opc_last_update);
						//query_opc_last_update.setIdempotent(true);
						query_opc_last_update.setTimestamp(CassandraColumns.LAST_UPDATE, new Date());
						query_opc_last_update.setString(CassandraColumns.OPC_ID, key);
						opc_value_update_list.add(query_opc_last_update);
					}
				}
				executeBatch(session, opc_value_update_list);	
				
				//----------------------------------------------------------------------------------------------------------
				// 4. 태그 및 OPC 카운트 배치
				//----------------------------------------------------------------------------------------------------------
				List<Statement> count_query_list = null;
				// 태그 카운트 업데이트
				count_query_list = new ArrayList<>();
				PreparedStatement ps_count = CassandraBatchStatementBuilder.getInstance().getTAG_COUNT_UPDATE_STATEMENT();
				if (tag_count_map.size() > 0) {
					for( String key : tag_count_map.keySet() ){
						BoundStatement query_count = new BoundStatement(ps_count);
						//query_count.setIdempotent(true);
						query_count.setString(CassandraColumns.TAG_ID, key);
						query_count.setLong(CassandraColumns.COUNT, tag_count_map.get(key));
						count_query_list.add(query_count);
					}
				}
				executeBatchForCounter(session, count_query_list);

				// OPC 카운트 업데이트
				count_query_list = new ArrayList<>();
				PreparedStatement ps_count_by_opc = CassandraBatchStatementBuilder.getInstance().getTAG_COUNT_BY_OPC_UPDATE_STATEMENT();
				if (opc_count_map.size() > 0) {
					for( String key : opc_count_map.keySet() ){
						BoundStatement query_count_by_opc = new BoundStatement(ps_count_by_opc);
						//query_count_by_opc.setIdempotent(true);
						query_count_by_opc.setString(CassandraColumns.OPC_ID, key);
						query_count_by_opc.setLong(CassandraColumns.COUNT, opc_count_map.get(key));
						count_query_list.add(query_count_by_opc);
					}
				}
				executeBatchForCounter(session, count_query_list);

				//------------------------------------------------------------------------------------------
				// 4. 사이트 요약 카운트 업데이트
				//------------------------------------------------------------------------------------------
				PreparedStatement ps_count_by_site = CassandraBatchStatementBuilder.getInstance().getTAG_COUNT_BY_SITE_UPDATE_STATEMENT();
				PreparedStatement ps_site_summary = CassandraBatchStatementBuilder.getInstance().getSITE_SUMMARY_UPDATE_STATEMENT();
				
				if (site_count_map.size() > 0) {
					for( String key : site_count_map.keySet() ){
						
						BoundStatement query_count_by_site = new BoundStatement(ps_count_by_site);
						//query_count_by_site.setIdempotent(true);
						query_count_by_site.setString(CassandraColumns.SITE_ID, key);
						query_count_by_site.setLong(CassandraColumns.COUNT, site_count_map.get(key));
						session.executeAsync(query_count_by_site);
						
						BoundStatement query_site_summary = new BoundStatement(ps_site_summary);
						//query_site_summary.setIdempotent(true);
						query_site_summary.setString(CassandraColumns.SITE_ID,  key);
						query_site_summary.setLong(CassandraColumns.DATA_COUNT, site_count_map.get(key));
						query_site_summary.setInt(CassandraColumns.YEAR, StorageUtils.getLocalDateTime().getYear());
						query_site_summary.setInt(CassandraColumns.MONTH, StorageUtils.getLocalDateTime().getMonthValue());
						query_site_summary.setInt(CassandraColumns.DAY, StorageUtils.getLocalDateTime().getDayOfMonth());
						query_site_summary.setInt(CassandraColumns.HOUR, StorageUtils.getLocalDateTime().getHour());
						query_site_summary.setInt(CassandraColumns.MINUTE, StorageUtils.getLocalDateTime().getMinute());
						session.executeAsync(query_site_summary);
						
					}
				}
				
				//------------------------------------------------------------------------------------------
				// 4. 시계열 데이터베이스 동기화
				//------------------------------------------------------------------------------------------
				timeseries_db.syncPoint(db_data_list);
				
				//
				db_data_list = null;
				long end = System.currentTimeMillis() - start;

				BatchTimerStastics.getInstance().getTOTAL_BATCH_PROCESS_COUNT().getAndAddAsync(batch_total_size);
				BatchTimerStastics.getInstance().getTOTAL_BATCH_PROCESS_TIME_MS().getAndAddAsync(end);

				log.debug("Cassandra batch insert completed : thread_number=[" + timer_number + "-" + thread_number + "], batch_total_size=[" + batch_total_size + "], time=[" + end + "]");

				//

			} catch (Exception ex) {
				log.error("Cassandra batch insert error : " + ex.getMessage(), ex);
				throw ex;

			} finally {
				//
				BatchTimerStastics.getInstance().getBATCH_ACTIVE().decrementAndGetAsync();
			}

			//
			
		} catch (Exception e) {
			try {
				log.error("DB batch insert error : " + e.getMessage(), e);
				if (db_data_list != null && db_data_list.size() > 0) {
					//메모리에 백업
					//DBDataPointBufferLocal.getInstance().addList(db_data_list); TODO 카산드라 저장 실패시 메모리로 복구하지 말것.
					//log.error("DB batch data list re-added cause error : " + e.getMessage() + " : size=[" + db_data_list.size() + "]");

					//파일에 백업
					BatchFailover failover = new BatchFailover();
					failover.backup(db_data_list);
					
				}
			} catch (Exception e1) {
				log.error("DB batch data list re-add failed : " + e.getMessage(), e);
			}
			log.error("Cassandra batch worker thread error : " + e.getMessage(), e);
		} finally {
			BatchTimerStastics.getInstance().getBATCH_COMPLETED().incrementAndGetAsync();
		}
       
		
	}; // Thread run close

	/**
	 * 
	 * @return
	 */
	public String getName() {
		return this.getClass().getName() + "[" + timer_number + "-" + thread_number + "]";
	}
	
	/**
	 * executeBatch
	 * @param session
	 * @param list
	 */
	private void executeBatch(Session session, List<Statement> list) {
		BatchStatement batch = new BatchStatement(Type.UNLOGGED);
        batch.addAll(list);  
        session.executeAsync(batch);
	};
	
	/**
	 * executeBatchForCounter
	 * @param session
	 * @param list
	 */
	private void executeBatchForCounter(Session session, List<Statement> list) {
		BatchStatement batch = new BatchStatement(Type.COUNTER);
        batch.addAll(list);  
        session.executeAsync(batch);
	};
	

}
