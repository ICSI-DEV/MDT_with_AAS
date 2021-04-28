package plantpulse.cep.engine.storage.select.cassandra;

import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Date;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.UUID;

import org.apache.commons.lang.StringUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.json.JSONArray;
import org.json.JSONObject;
import org.springframework.stereotype.Repository;

import com.datastax.driver.core.BoundStatement;
import com.datastax.driver.core.ConsistencyLevel;
import com.datastax.driver.core.PreparedStatement;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.ResultSetFuture;
import com.datastax.driver.core.Row;
import com.datastax.driver.core.Session;
import com.datastax.driver.core.utils.Bytes;

import plantpulse.cep.engine.logging.LoggingAppender;
import plantpulse.cep.engine.properties.PropertiesLoader;
import plantpulse.cep.engine.storage.DAOHelper;
import plantpulse.cep.engine.storage.StorageConstants;
import plantpulse.cep.engine.storage.insert.cassandra.CassandraColumns;
import plantpulse.cep.engine.storage.select.StorageSelectDAO;
import plantpulse.cep.engine.storage.session.CassandraSessionFixer;
import plantpulse.cep.engine.storage.statement.PreparedStatementCacheFactory;
import plantpulse.cep.engine.utils.DateUtils;
import plantpulse.domain.Asset;
import plantpulse.domain.OPC;
import plantpulse.domain.Site;
import plantpulse.domain.Tag;

/**
 * Table
 * 
 * @author lsb
 * 
 */
@Repository
public class CassandraSelectDAO extends DAOHelper implements StorageSelectDAO {

	private static final Log log = LogFactory.getLog(CassandraSelectDAO.class);
	
	private static final int QUERY_POINT_FETCH_SIZE     = 5_000;
	private static final int QUERY_ASSET_FETCH_SIZE     = 200;
	private static final int QUERY_SNAPSHOT_FETCH_SIZE  = 10;
	
	private static  int POINT_LIMIT_SIZE               = 1_000_000; //기본 포인트 제한 사이즈
	
	
	public CassandraSelectDAO() {
		POINT_LIMIT_SIZE = Integer.parseInt(PropertiesLoader.getStorage_properties().getProperty("storage.tag.point.select.limit.size", POINT_LIMIT_SIZE + ""));
	};
	
	
	@Override
	public JSONObject selectSiteSummary(Site site, int year, int month, int day) throws Exception {
		//

		JSONObject result = null;

		Session session = null;

		String table = "";
		StringBuffer cql = null;
		//
		try {

			//
			long start = System.currentTimeMillis();

			//
			session = CassandraSessionFixer.getSession();
			String keyspace = CassandraSessionFixer.getKeyspace();

			//
			cql = new StringBuffer();

			cql.append(" SELECT SUM(" + CassandraColumns.DATA_COUNT + ") as data_count,  ");
			cql.append(" SUM(" + CassandraColumns.ALARM_COUNT + ") as alarm_count,  ");
			cql.append(" SUM(" + CassandraColumns.ALARM_COUNT_BY_INFO + ") as alarm_count_by_info,  ");
			cql.append(" SUM(" + CassandraColumns.ALARM_COUNT_BY_WARN + ") as alarm_count_by_warn, ");
			cql.append(" SUM(" + CassandraColumns.ALARM_COUNT_BY_ERROR + ") as alarm_count_by_error ");
			cql.append(" FROM " + keyspace + "." + StorageConstants.SITE_SUMMARY_TABLE + " ");
			cql.append(" WHERE " + CassandraColumns.SITE_ID + " = ? ");
			cql.append(" AND " + CassandraColumns.YEAR + "  = ?  ");
			cql.append(" AND " + CassandraColumns.MONTH + " = ? ");
			cql.append(" AND " + CassandraColumns.DAY + "   = ? ;  ");


			PreparedStatement statement =  PreparedStatementCacheFactory.getInstance().get(session, cql.toString(), ConsistencyLevel.ONE);
			
			BoundStatement query = new BoundStatement(statement);
			query.setString(0, site.getSite_id());
			query.setInt(1, year);
			query.setInt(2, month);
			query.setInt(3, day);

			
			ResultSetFuture rsf = session.executeAsync(query);
			ResultSet rs = rsf.getUninterruptibly();
			List<Row> list  = rs.all();

			result = new JSONObject();
			for (int i = 0; list != null && i < list.size(); i++) {
				Row row = list.get(i);
				result.put("site_id", site.getSite_id());
				result.put("data_count", row.getLong("data_count"));
				result.put("alarm_count", row.getLong("alarm_count"));
				result.put("alarm_count_by_info", row.getLong("alarm_count_by_info"));
				result.put("alarm_count_by_warn", row.getLong("alarm_count_by_warn"));
				result.put("alarm_count_by_error", row.getLong("alarm_count_by_error"));
			}

			//
			long end = System.currentTimeMillis() - start;

			//
			log.debug("Cassandra select site summary : exec_time=[" + end + "]");
			//
			return result;

		} catch (Exception ex) {
			log.error("Table[" + table + "] select site summary error : CQL=[" + cql.toString() + "], " + ex.getMessage(), ex);
			throw ex;
		} finally {
			//
			// if (session != null && !session.isClosed())
			// session.close();
		}
	};
	
	
	public JSONObject selectOPCPointLastUpdate(OPC opc) throws Exception{
		//

		JSONObject object = null;

		Session session = null;

		String table = "";
		StringBuffer cql = null;
		//
		try {

			//
			long start = System.currentTimeMillis();

			//
			session = CassandraSessionFixer.getSession();
			String keyspace = CassandraSessionFixer.getKeyspace();

			//
			cql = new StringBuffer();
            cql.append(" SELECT * FROM " + keyspace + "." + StorageConstants.OPC_POINT_LAST_UPDATE_TABLE + " WHERE " + CassandraColumns.OPC_ID + " = ? ");

				
			PreparedStatement statement =  PreparedStatementCacheFactory.getInstance().get(session, cql.toString(), ConsistencyLevel.ONE);
			BoundStatement query = new BoundStatement(statement);
			query.setString(0, opc.getOpc_id());
		
			ResultSetFuture rsf = session.executeAsync(query);
			ResultSet rs = rsf.getUninterruptibly();
			Row row  = rs.one();

			object = new JSONObject();
			if(row != null){
				object.put("opc_id", opc.getOpc_id());
				object.put("last_update_timestamp", row.getTimestamp(CassandraColumns.LAST_UPDATE).getTime());
				object.put("last_update_date", row.getTimestamp(CassandraColumns.LAST_UPDATE));
			}else{
				object.put("opc_id", opc.getOpc_id());
			}
				
			//
			long end = System.currentTimeMillis() - start;

			//
			log.debug("Cassandra select opc point last update : exec_time=[" + end + "]");
			//
			return object;

		} catch (Exception ex) {
			log.error("Table[" + table + "] opc point last update error : CQL=[" + cql.toString() + "], " + ex.getMessage(), ex);
			throw ex;
		} finally {
			//
			// if (session != null && !session.isClosed())
			// session.close();
		}
	}
	
	/*
	 * (non-Javadoc)
	 * @see plantpulse.cep.engine.storage.select.StorageSelectDAO#selectPointList(plantpulse.domain.Tag, java.lang.String, java.lang.String)
	 */
	@Override
	public List<Map<String, Object>> selectPointList(Tag tag, String from, String to) throws Exception {
		return selectPointList(tag, from, to, POINT_LIMIT_SIZE, null);
	}
	
	
	/*
	 * (non-Javadoc)
	 * @see plantpulse.cep.engine.storage.select.StorageSelectDAO#selectPointList(plantpulse.domain.Tag, java.lang.String, java.lang.String, int)
	 */
	@Override
	public List<Map<String, Object>> selectPointList(Tag tag, String from, String to, int limit) throws Exception {
		return selectPointList(tag, from, to, limit, null);
	}

	
	/*
	 * (non-Javadoc)
	 * @see plantpulse.cep.engine.storage.select.StorageSelectDAO#selectPointList(plantpulse.domain.Tag, java.lang.String, java.lang.String, int, java.lang.String)
	 */
	@Override
	public List<Map<String, Object>> selectPointList(Tag tag, String from, String to, int limit, String condition) throws Exception {
		//

		List<Map<String, Object>> result = null;

		Session session = null;

		String table = "";
		StringBuffer cql = null;
		//
		try {

			//
			long start = System.currentTimeMillis();

			//
			session = CassandraSessionFixer.getSession();
			String keyspace = CassandraSessionFixer.getKeyspace();

			//
			result = new ArrayList<Map<String, Object>>();

			//
			cql = new StringBuffer();

			String java_type = tag.getJava_type();
			//
			cql.append(" SELECT * FROM " + keyspace + "." + StorageConstants.TAG_POINT_TABLE + " WHERE " + CassandraColumns.TAG_ID + " = ?" + " AND " + CassandraColumns.TIMESTAMP + " >= ? AND "
					+ CassandraColumns.TIMESTAMP + " <= ?  ");
			if(StringUtils.isNotEmpty(condition)){
				String converter_condition = condition.replaceAll("VALUE", super.getFieldNameWithCondition(java_type));
				cql.append(" AND " + converter_condition + "  ");
			}
			cql.append("  LIMIT ? ");
			if(StringUtils.isNotEmpty(condition)){
				cql.append("  ALLOW FILTERING ");
			};
			
			
			//log.info("Point Data Search CQL = " + cql.toString());

			PreparedStatement statement =  PreparedStatementCacheFactory.getInstance().get(session, cql.toString(), ConsistencyLevel.ONE);
			
			BoundStatement query = new BoundStatement(statement);
			query.setString(0,    tag.getTag_id());
			query.setTimestamp(1, DateUtils.isoToTimestamp(from, true));
			query.setTimestamp(2, DateUtils.isoToTimestamp(to, true));
			query.setInt(3,       limit);
			
			
			query.setFetchSize(QUERY_POINT_FETCH_SIZE);

			ResultSetFuture rsf = session.executeAsync(query);
			ResultSet rs = rsf.getUninterruptibly();
			
		    Iterator<Row> iter = rs.iterator();
		    while (iter.hasNext()) {
		        if (rs.getAvailableWithoutFetching() == QUERY_POINT_FETCH_SIZE && !rs.isFullyFetched()) {
		           rs.fetchMoreResults();
		        };
		        Row row = iter.next();
		        Map<String, Object> map = new HashMap<String, Object>();
				map.put("timestamp",  new Date(row.getTimestamp(CassandraColumns.TIMESTAMP).getTime()));
				map.put("quality",    row.getInt(CassandraColumns.QUALITY));
				map.put("error_code", row.getInt(CassandraColumns.ERROR_CODE));
				map.put("attribute",  row.getMap(CassandraColumns.ATTRIBUTE, String.class, String.class));
				map.put("type",       row.getString(CassandraColumns.TYPE));
				super.addMapValueFromRow(row, map, java_type);

				//
				result.add(map);
		    };
		    
			/*   
			List<Row> list = rs.all();
			result = new ArrayList<Map<String, Object>>();
			for (int i = 0; list != null && i < list.size(); i++) {
				Row row = list.get(i);

				Map<String, Object> map = new HashMap<String, Object>();
				map.put("timestamp", new Date(row.getTimestamp(CassandraColumns.TIMESTAMP).getTime()));
				map.put("quality",    row.getInt(CassandraColumns.QUALITY));
				map.put("error_code", row.getInt(CassandraColumns.ERROR_CODE));
				map.put("attribute",  row.getMap(CassandraColumns.ATTRIBUTE, String.class, String.class));
				map.put("type",       row.getString(CassandraColumns.TYPE));
				super.addMapValueFromRow(row, map, java_type);

				//
				result.add(map);
			}
			*/

			//
			long end = System.currentTimeMillis() - start;

			//
			log.debug("Cassandra select tag data list : exec_time=[" + end + "]");
			//
			return result;

		} catch (Exception ex) {
			log.error("Table[" + table + "] select tag data list error : CQL=[" + cql.toString() + "], " + ex.getMessage(), ex);
			throw ex;
		} finally {
			//
			// if (session != null && !session.isClosed())
			// session.close();
		}
	}
	
	
	@Override
	public Map<String, Object> selectPointLast(Tag tag, String from, String to, String condition) throws Exception {
		//

		Map<String, Object> map = null;

		Session session = null;

		String table = "";
		StringBuffer cql = null;
		//
		try {

			//
			long start = System.currentTimeMillis();

			//
			session = CassandraSessionFixer.getSession();
			String keyspace = CassandraSessionFixer.getKeyspace();

			//
			// table = "TD_" + tag.getTag_id();

			//
			cql = new StringBuffer();

			String java_type = tag.getJava_type();
			//
			cql.append(" SELECT * FROM " + keyspace + "." + StorageConstants.TAG_POINT_TABLE + " WHERE " + CassandraColumns.TAG_ID + " = ?" + " AND " + CassandraColumns.TIMESTAMP + " >= ? AND "
					+ CassandraColumns.TIMESTAMP + " <= ? " );
			if(StringUtils.isNotEmpty(condition)){
				String converter_condition = condition.replaceAll("VALUE", super.getFieldNameWithCondition(java_type));
				cql.append(" AND " + converter_condition + "  ");
			}
			cql.append("  LIMIT 1 ");
			cql.append(" ALLOW FILTERING ");
			
			//log.info("Point Data Search CQL = " + cql.toString());

			PreparedStatement statement =  PreparedStatementCacheFactory.getInstance().get(session, cql.toString(), ConsistencyLevel.ONE);
			
			BoundStatement query = new BoundStatement(statement);
			query.setString(0, tag.getTag_id());
			query.setTimestamp(1, DateUtils.isoToTimestamp(from, true));
			query.setTimestamp(2, DateUtils.isoToTimestamp(to, true));
			

			ResultSetFuture rsf = session.executeAsync(query);
			ResultSet rs = rsf.getUninterruptibly();
			
				Row row = rs.one();
				if(row != null){
				    map = new HashMap<String, Object>();
					map.put("timestamp", new Date(row.getTimestamp(CassandraColumns.TIMESTAMP).getTime()));
					map.put("quality", row.getInt(CassandraColumns.QUALITY));
					map.put("error_code", row.getInt(CassandraColumns.ERROR_CODE));
					map.put("attribute",  row.getMap(CassandraColumns.ATTRIBUTE, String.class, String.class));
					map.put("type",       row.getString(CassandraColumns.TYPE));
					super.addMapValueFromRow(row, map, java_type);
				}

				//


			//
			long end = System.currentTimeMillis() - start;

			//
			log.debug("Cassandra select tag data list : exec_time=[" + end + "]");
			//
			return map;

		} catch (Exception ex) {
			log.error("Table[" + table + "] select tag data list error : CQL=[" + cql.toString() + "], " + ex.getMessage(), ex);
			throw ex;
		} finally {
			//
			// if (session != null && !session.isClosed())
			// session.close();
		}
	}
	

	@Override
	public Map<String, Object> selectPointLast(Tag tag, String to) throws Exception {
		//

		Map<String, Object> map = null;

		Session session = null;

		String table = "";
		StringBuffer cql = null;
		//
		try {

			//
			long start = System.currentTimeMillis();

			//
			session = CassandraSessionFixer.getSession();
			String keyspace = CassandraSessionFixer.getKeyspace();

			//
			// table = "TD_" + tag.getTag_id();

			//
			cql = new StringBuffer();

			String java_type = tag.getJava_type();
			//
			cql.append(" SELECT * FROM " + keyspace + "." + StorageConstants.TAG_POINT_TABLE + " WHERE " + CassandraColumns.TAG_ID + " = ? AND  " + CassandraColumns.TIMESTAMP + " <= ? " + "" );
			cql.append("  LIMIT 1 ALLOW FILTERING ");
			

			PreparedStatement statement =  PreparedStatementCacheFactory.getInstance().get(session, cql.toString(), ConsistencyLevel.ONE);
			
			BoundStatement query = new BoundStatement(statement);
			query.setString(0, tag.getTag_id());
			query.setTimestamp(1, DateUtils.isoToTimestamp(to, true));
			
			
			ResultSetFuture rsf = session.executeAsync(query);
			ResultSet rs = rsf.getUninterruptibly();

				Row row = rs.one();
				if(row != null){
				    map = new HashMap<String, Object>();
					map.put("timestamp", new Date(row.getTimestamp(CassandraColumns.TIMESTAMP).getTime()));
					map.put("quality", row.getInt(CassandraColumns.QUALITY));
					map.put("error_code", row.getInt(CassandraColumns.ERROR_CODE));
					map.put("attribute",  row.getMap(CassandraColumns.ATTRIBUTE, String.class, String.class));
					map.put("type",       row.getString(CassandraColumns.TYPE));
					super.addMapValueFromRow(row, map, java_type);
				}

			//
			long end = System.currentTimeMillis() - start;

			//
			log.debug("Cassandra select tag data last : exec_time=[" + end + "]");
			//
			return map;

		} catch (Exception ex) {
			log.error("Table[" + table + "] select tag data list error : CQL=[" + cql.toString() + "], " + ex.getMessage(), ex);
			throw ex;
		} finally {
			//
			// if (session != null && !session.isClosed())
			// session.close();
		}
	};
	
	@Override
	public  List<Map<String, Object>>  selectPointLast(Tag tag, int limit) throws Exception {
		//

		 List<Map<String, Object>> result = null;

		Session session = null;

		String table = "";
		StringBuffer cql = null;
		//
		try {

			//
			long start = System.currentTimeMillis();

			//
			session = CassandraSessionFixer.getSession();
			String keyspace = CassandraSessionFixer.getKeyspace();

			
			//
			cql = new StringBuffer();

			String java_type = tag.getJava_type();
			//
			cql.append(" SELECT * FROM " + keyspace + "." + StorageConstants.TAG_POINT_TABLE  + " "  );
			cql.append(" WHERE " + CassandraColumns.TAG_ID + " = ? ");
			cql.append("  LIMIT ? ");
			cql.append("  ALLOW FILTERING ");
			
			PreparedStatement statement =  PreparedStatementCacheFactory.getInstance().get(session, cql.toString(), ConsistencyLevel.ONE);
			BoundStatement query = new BoundStatement(statement);
			
			query.setString(0, tag.getTag_id());
			query.setInt(1,  limit);

			
			ResultSetFuture rsf = session.executeAsync(query);
			ResultSet rs = rsf.getUninterruptibly();
			
			
			List<Row> all = rs.all();
			if(all != null) {
				result = new ArrayList<Map<String, Object>>();
				for(int i=0; i < all.size(); i++) {
					   Row row = all.get(i);
						Map<String,Object> map = new HashMap<>();
					    map = new HashMap<String, Object>();
						map.put("timestamp",  new Date(row.getTimestamp(CassandraColumns.TIMESTAMP).getTime()));
						map.put("quality",    row.getInt(CassandraColumns.QUALITY));
						map.put("error_code", row.getInt(CassandraColumns.ERROR_CODE));
						map.put("attribute",  row.getMap(CassandraColumns.ATTRIBUTE, String.class, String.class));
						map.put("type",       row.getString(CassandraColumns.TYPE));
						super.addMapValueFromRow(row, map, java_type);
						result.add(map);
				};
			}
			//
			long end = System.currentTimeMillis() - start;
			log.debug("Cassandra select tag point limit : exec_time=[" + end + "]");
			//
			return result;

		} catch (Exception ex) {
			log.error("Table[" + table + "] select tag point limit error : CQL=[" + cql.toString() + "], " + ex.getMessage(), ex);
			throw ex;
		} finally {
			//
			// if (session != null && !session.isClosed())
			// session.close();
		}
	};
	
	@Override
	public Map<String, Object> selectPointLast(Tag tag) throws Exception {
		//

		Map<String, Object> map = null;

		Session session = null;

		String table = "";
		StringBuffer cql = null;
		//
		try {

			//
			long start = System.currentTimeMillis();

			//
			session = CassandraSessionFixer.getSession();
			String keyspace = CassandraSessionFixer.getKeyspace();

			//
			// table = "TD_" + tag.getTag_id();

			//
			cql = new StringBuffer();

			String java_type = tag.getJava_type();
			//
			cql.append(" SELECT * FROM " + keyspace + "." + StorageConstants.TAG_POINT_TABLE + " " );
			cql.append(" WHERE " + CassandraColumns.TAG_ID + " = ? ");
			cql.append("  LIMIT 1 ");
			cql.append("  ALLOW FILTERING ");
			
			//log.info("Point Data Search CQL = " + cql.toString());

			PreparedStatement statement =  PreparedStatementCacheFactory.getInstance().get(session, cql.toString(), ConsistencyLevel.ONE);
			
			BoundStatement query = new BoundStatement(statement);
			query.setString(0, tag.getTag_id());
			
			query.setFetchSize(QUERY_POINT_FETCH_SIZE);

			ResultSetFuture rsf = session.executeAsync(query);
			ResultSet rs = rsf.getUninterruptibly();

				Row row = rs.one();
				if(row != null){
				    map = new HashMap<String, Object>();
					map.put("timestamp", new Date(row.getTimestamp(CassandraColumns.TIMESTAMP).getTime()));
					map.put("quality", row.getInt(CassandraColumns.QUALITY));
					map.put("error_code", row.getInt(CassandraColumns.ERROR_CODE));
					map.put("attribute",  row.getMap(CassandraColumns.ATTRIBUTE, String.class, String.class));
					map.put("type",       row.getString(CassandraColumns.TYPE));
					super.addMapValueFromRow(row, map, java_type);
				}

			//
			//
			long end = System.currentTimeMillis() - start;

			//
			log.debug("Cassandra select tag data last : exec_time=[" + end + "]");
			//
			return map;

		} catch (Exception ex) {
			log.error("Table[" + table + "] select tag data list error : CQL=[" + cql.toString() + "], " + ex.getMessage(), ex);
			throw ex;
		} finally {
			//
			// if (session != null && !session.isClosed())
			// session.close();
		}
	}
	
	
	@Override
	public  List<Map<String, Object>>  selectPointAfterTimestamp(Tag tag, long timestamp) throws Exception {
		//

		 List<Map<String, Object>> result = null;

		Session session = null;

		String table = "";
		StringBuffer cql = null;
		//
		try {

			//
			long start = System.currentTimeMillis();

			//
			session = CassandraSessionFixer.getSession();
			String keyspace = CassandraSessionFixer.getKeyspace();

			
			//
			cql = new StringBuffer();

			String java_type = tag.getJava_type();
			//
			cql.append(" SELECT * FROM " + keyspace + "." + StorageConstants.TAG_POINT_TABLE + " WHERE " + CassandraColumns.TAG_ID + " = ? AND "
					+ " " + CassandraColumns.TIMESTAMP + " >= ?  " );
			cql.append(" ALLOW FILTERING  ");
			
			PreparedStatement statement =  PreparedStatementCacheFactory.getInstance().get(session, cql.toString(), ConsistencyLevel.ONE);
			BoundStatement query = new BoundStatement(statement);
			query.setString(0, tag.getTag_id());
			query.setTimestamp(1, new Date(timestamp));
			
			

			ResultSetFuture rsf = session.executeAsync(query);
			ResultSet rs = rsf.getUninterruptibly();
			
			List<Row> all = rs.all();
			if(all != null) {
				result = new ArrayList<Map<String, Object>>();
				for(int i=0; i < all.size(); i++) {
					   Row row = all.get(i);
						Map<String,Object> map = new HashMap<>();
					    map = new HashMap<String, Object>();
						map.put("timestamp",  new Date(row.getTimestamp(CassandraColumns.TIMESTAMP).getTime()));
						map.put("quality",    row.getInt(CassandraColumns.QUALITY));
						map.put("error_code", row.getInt(CassandraColumns.ERROR_CODE));
						map.put("attribute",  row.getMap(CassandraColumns.ATTRIBUTE, String.class, String.class));
						map.put("type",       row.getString(CassandraColumns.TYPE));
						super.addMapValueFromRow(row, map, java_type);
						result.add(map);
				};
			}
			//
			long end = System.currentTimeMillis() - start;
			log.debug("Cassandra select tag point after : exec_time=[" + end + "]");
			//
			return result;

		} catch (Exception ex) {
			log.error("Table[" + table + "] select tag point after error : CQL=[" + cql.toString() + "], " + ex.getMessage(), ex);
			throw ex;
		} finally {
			//
			// if (session != null && !session.isClosed())
			// session.close();
		}
	};
	
	
	
	
	/**
	 * 태그 데이터를 조회한다.
	 * 
	 * @param site
	 * @param tag
	 * @param from
	 * @param to
	 * @param aggregation
	 * @return
	 * @throws Exception
	 */
	@Override
	public JSONArray selectPointValueList(Tag tag, String from, String to, String sampling, String term) throws Exception {
		return selectPointValueList( tag,  from,  to,  sampling,  term, POINT_LIMIT_SIZE);
	}

	/**
	 * 태그 데이터를 조회한다.
	 * 
	 * @param site
	 * @param tag
	 * @param from
	 * @param to
	 * @param aggregation
	 * @return
	 * @throws Exception
	 */
	@Override
	public JSONArray selectPointValueList(Tag tag, String from, String to, String sampling, String term, int limit) throws Exception {
		//

		JSONArray array = null;

		Session session = null;

		String table = "";
		StringBuffer cql = null;
		//
		try {

			//
			long start = System.currentTimeMillis();

			array = new JSONArray();
			
			//
			session = CassandraSessionFixer.getSession();
			String keyspace = CassandraSessionFixer.getKeyspace();

			//
			// table = "TD_" + tag.getTag_id();

			//
			cql = new StringBuffer();
			
			//샘플링 데이터 필터링일 경우
			if (StringUtils.isNotEmpty(sampling) && StringUtils.isNotEmpty(term)) {
				
				String java_type = tag.getJava_type();
				String function_name = super.getSamplingFunctionNameForData();

				cql.append(" SELECT " + CassandraColumns.TIMESTAMP + ", " + function_name + " as value FROM " + keyspace + "." + StorageConstants.TAG_POINT_SAMPLING_TABLE + "_" + term + " WHERE "
						+ CassandraColumns.TAG_ID + " = ? AND " + CassandraColumns.TIMESTAMP + " >= ? AND " + CassandraColumns.TIMESTAMP + " <= ?  " 
						+ " LIMIT ? ALLOW FILTERING");

				PreparedStatement statement =  PreparedStatementCacheFactory.getInstance().get(session, cql.toString(), ConsistencyLevel.ONE);
				
				BoundStatement query = new BoundStatement(statement);
				query.setString(0, tag.getTag_id());
				query.setTimestamp(1, DateUtils.isoToTimestamp(from, true));
				query.setTimestamp(2, DateUtils.isoToTimestamp(to, true));
				limit = (limit > 0 ) ? limit : POINT_LIMIT_SIZE;
				query.setInt(3, limit);
				
				query.setFetchSize(QUERY_POINT_FETCH_SIZE);

				ResultSetFuture rsf = session.executeAsync(query);
				ResultSet rs = rsf.getUninterruptibly();
				
			    Iterator<Row> iter = rs.iterator();
			    while (iter.hasNext()) {
			        if (rs.getAvailableWithoutFetching() == QUERY_POINT_FETCH_SIZE && !rs.isFullyFetched()) {
			           rs.fetchMoreResults();
			        }
			        Row row = iter.next();
			        //
					JSONArray json = new JSONArray();
					long timestamp = row.getTimestamp(CassandraColumns.TIMESTAMP).getTime();
					json.add(timestamp);
					json.add(row.getDouble("value"));
					//
					array.add(json);
			      
			    };
				    

		    //원본 데이터 조회 조회
			} else {

				String java_type = tag.getJava_type();
				cql.append(" SELECT " + CassandraColumns.TIMESTAMP + ", " + CassandraColumns.VALUE + "  FROM " + keyspace + "." + StorageConstants.TAG_POINT_TABLE + 
						" WHERE " + CassandraColumns.TAG_ID + " = ?" + " AND " + CassandraColumns.TIMESTAMP + " >= ? AND "
						+ CassandraColumns.TIMESTAMP + " <= ? " + 
								"  ALLOW FILTERING  ");

				PreparedStatement statement =  PreparedStatementCacheFactory.getInstance().get(session, cql.toString(), ConsistencyLevel.ONE);
				
				BoundStatement query = new BoundStatement(statement);
				query.setString(0, tag.getTag_id());
				query.setTimestamp(1, DateUtils.isoToTimestamp(from, true));
				query.setTimestamp(2, DateUtils.isoToTimestamp(to, true));
				
				query.setFetchSize(QUERY_POINT_FETCH_SIZE);

				ResultSetFuture rsf = session.executeAsync(query);
				ResultSet rs = rsf.getUninterruptibly();
				
				Iterator<Row> iter = rs.iterator();
			    while (iter.hasNext()) {
			        if (rs.getAvailableWithoutFetching() == 100 && !rs.isFullyFetched()) {
			           rs.fetchMoreResults();
			        }
			        Row row = iter.next();
			        
			        //
			        JSONArray json = new JSONArray();
					long timestamp = row.getTimestamp(CassandraColumns.TIMESTAMP).getTime();
					json.add(timestamp);

					//
					super.addJSONValueFromRow(row, json, java_type);
					
					//
					array.add(json);
			      
			    };
				    
				
			};
			
			Collections.reverse(array);

			
			//
			long end = System.currentTimeMillis() - start;

			//
			log.debug("Cassandra select tag value list : exec_time=[" + end + "]");
			//
			return array;

		} catch (Exception ex) {
			log.error("Table[" + table + "] select value list error : CQL=[" + cql.toString() + "], " + ex.getMessage(), ex);
			throw ex;
		} finally {
			//
			// if (session != null && !session.isClosed())
			// session.close();
		}
	};
	
	
	/**
	 * 태그 데이터를 조회한다.
	 * 
	 * @param site
	 * @param tag
	 * @param from
	 * @param to
	 * @param aggregation
	 * @return
	 * @throws Exception
	 */
	@Override
	public JSONArray selectPointValueList(Tag tag, int limit) throws Exception {
		//

		JSONArray array = null;

		Session session = null;

		String table = "";
		StringBuffer cql = null;
		//
		try {

			//
			long start = System.currentTimeMillis();

			//
			session = CassandraSessionFixer.getSession();
			String keyspace = CassandraSessionFixer.getKeyspace();

			//
			// table = "TD_" + tag.getTag_id();

			//
			cql = new StringBuffer();
			
			

				String java_type = tag.getJava_type();
				cql.append(" SELECT " + CassandraColumns.TIMESTAMP + ", " + CassandraColumns.VALUE + "  FROM " + keyspace + "." + StorageConstants.TAG_POINT_TABLE  + " ");
				cql.append(" WHERE " + CassandraColumns.TAG_ID + " = ? ");
				cql.append(" LIMIT ?  ALLOW FILTERING ");

				PreparedStatement statement =  PreparedStatementCacheFactory.getInstance().get(session, cql.toString(), ConsistencyLevel.ONE);
				
				BoundStatement query = new BoundStatement(statement);
				query.setString(0, tag.getTag_id());
				query.setInt(1,  limit);
				
				query.setFetchSize(QUERY_POINT_FETCH_SIZE);
				

				ResultSetFuture rsf = session.executeAsync(query);
				ResultSet rs = rsf.getUninterruptibly();
				
				List<Row> list = rs.all();

				Collections.reverse(list);

				array = new JSONArray();
				for (int i = 0; list != null && i < list.size(); i++) {
					Row row = list.get(i);

					JSONArray json = new JSONArray();
					long timestamp = row.getTimestamp(CassandraColumns.TIMESTAMP).getTime();
					json.add(timestamp);

					//
					super.addJSONValueFromRow(row, json, java_type);
					
					//
					array.add(json);
				}

			//
			long end = System.currentTimeMillis() - start;

			//
			log.debug("Cassandra select tag value list : exec_time=[" + end + "]");
			//
			return array;

		} catch (Exception ex) {
			log.error("Table[" + table + "] select value list error : CQL=[" + cql.toString() + "], " + ex.getMessage(), ex);
			throw ex;
		} finally {
			//
			// if (session != null && !session.isClosed())
			// session.close();
		}
	}

	@Override
	public JSONObject selectPointDataForAggregatation(Tag tag, long current_timestamp, String from, String to) throws Exception {
		//

		JSONObject object = null;

		Session session = null;

		String table = "";
		StringBuffer cql = null;
		//
		try {

			//
			long start = System.currentTimeMillis();

			//
			session = CassandraSessionFixer.getSession();
			String keyspace = CassandraSessionFixer.getKeyspace();

			//
			cql = new StringBuffer();
			
			String column_name = super.getFieldNameWithCondition(tag.getJava_type());


			cql.append(" SELECT JSON " + CassandraColumns.TAG_ID + ", "
					+ "MIN(" + column_name + ") as min,"
							+ " MAX(" + column_name + ") as max, "
									+ "AVG(" + column_name + ") as avg, "
											+ "SUM(" + column_name + ") as sum, "
										//			+ "STDDEV(CAST(" + column_name + " as double)) as stddev, "
															+ "COUNT(" + column_name + ") as count  "
																	+ "FROM " + keyspace
							+ "." + StorageConstants.TAG_POINT_TABLE  + " WHERE " + CassandraColumns.TAG_ID + " = ? AND " + CassandraColumns.TIMESTAMP + " >= ? AND "
							+ CassandraColumns.TIMESTAMP + " <= ? " 
							+ " ALLOW FILTERING  ");

					PreparedStatement statement =  PreparedStatementCacheFactory.getInstance().get(session, cql.toString(), ConsistencyLevel.ONE);
					
					BoundStatement query = new BoundStatement(statement);
					query.setString(0, tag.getTag_id());
					query.setTimestamp(1, DateUtils.isoToTimestamp(from, true));
					query.setTimestamp(2, DateUtils.isoToTimestamp(to, true));
				
					
					ResultSetFuture rsf = session.executeAsync(query);
					ResultSet rs = rsf.getUninterruptibly();
					
					List<Row> list = rs.all();
					if(list != null && list.size() > 0){
						Row row = list.get(0);
						String json_str = row.getString(0);
						json_str = json_str.replaceAll("NaN", "null"); //
						object = JSONObject.fromObject(json_str);
						object.put("timestamp", current_timestamp);
					}
 
			//
			long end = System.currentTimeMillis() - start;

			//
			log.debug("Cassandra select tag data aggregate : exec_time=[" + end + "]");
			//
			return object;

		} catch (Exception ex) {
			log.error("Table[" + table + "] select tag data for aggregation  error : CQL=[" + cql.toString() + "], TAG_ID=[" + tag.getTag_id() + "] " + ex.getMessage(), ex);
			throw ex;
		} finally {
			//
			// if (session != null && !session.isClosed())
			// session.close();
		}
	}
	

	@Override
	public JSONArray selectPointDataForAggregatationByType(String type, long current_timestamp, String from, String to) throws Exception {
		//

		JSONArray object = null;

		Session session = null;

		String table = "";
		StringBuffer cql = null;
		//
		try {

			//
			long start = System.currentTimeMillis();

			//
			session = CassandraSessionFixer.getSession();
			String keyspace = CassandraSessionFixer.getKeyspace();

			//
			cql = new StringBuffer();
			
			String column_name = super.getFieldNameWithCondition("Double");

			cql.append(" SELECT JSON " + CassandraColumns.TAG_ID + ", "
					+ "MIN(" + column_name + ") as min,"
							+ " MAX(" + column_name + ") as max, "
									+ "AVG(" + column_name + ") as avg, "
											+ "SUM(" + column_name + ") as sum, "
													+ "STDDEV(" + column_name + ") as stddev, "
															+ "COUNT(" + column_name + ") as count  "
																	+ "FROM " + keyspace
							+ "." + StorageConstants.TAG_POINT_TABLE  + " WHERE " + CassandraColumns.TYPE + " = ? "
									+ " AND " + CassandraColumns.TIMESTAMP + " >= ? "
											+ " AND " + CassandraColumns.TIMESTAMP + " <= ? "
											+ " GROUP BY TAG_ID ALLOW FILTERING " );

					PreparedStatement statement =  PreparedStatementCacheFactory.getInstance().get(session, cql.toString(), ConsistencyLevel.ONE);
					
					BoundStatement query = new BoundStatement(statement);
					query.setString(0, type);
					query.setTimestamp(1, DateUtils.isoToTimestamp(from, true));
					query.setTimestamp(2, DateUtils.isoToTimestamp(to, true));
					
					
					ResultSetFuture rsf = session.executeAsync(query);
					ResultSet rs = rsf.getUninterruptibly();
					
					List<Row> list = rs.all();
					if(list != null && list.size() > 0){
						object = new JSONArray();
						for(int i=0; i < list.size(); i++){
						Row row = list.get(i);
						String json_str = row.getString(0);
						json_str = json_str.replaceAll("NaN", "null"); //
						JSONObject one = JSONObject.fromObject(json_str);
						object.add(one);
						}
					}
 
			//
			long end = System.currentTimeMillis() - start;

			//
			log.debug("Cassandra select tag data aggregate by type : type=[" + type + "], exec_time=[" + end + "]");
			//
			return object;

		} catch (Exception ex) {
			log.error("Table[" + table + "] select tag data for aggregation by type error : CQL=[" + cql.toString() + "] " + ex.getMessage(), ex);
			throw ex;
		} finally {
			//
			// if (session != null && !session.isClosed())
			// session.close();
		}
	}
	

	@Override
	public JSONObject selectPointDataForSnapshot(Tag tag, long current_timestamp, String from, String to) throws Exception {
		//

		JSONObject object = null;

		Session session = null;

		String table = "";
		StringBuffer cql = null;
		//
		try {

			//
			long start = System.currentTimeMillis();

			//
			session = CassandraSessionFixer.getSession();
			String keyspace = CassandraSessionFixer.getKeyspace();

			//
			cql = new StringBuffer();
			
			String column_name = super.getFieldNameWithCondition(tag.getJava_type());


			cql.append(" SELECT JSON " + CassandraColumns.TAG_ID + ", " + CassandraColumns.TIMESTAMP + ", " + column_name + " AS value FROM " + keyspace
							+ "." + StorageConstants.TAG_POINT_TABLE  + " WHERE " + CassandraColumns.TAG_ID + " = ? AND " + CassandraColumns.TIMESTAMP + " >= ? AND " 
					+ CassandraColumns.TIMESTAMP + " <= ? " + " LIMIT  1 ALLOW FILTERING ");

			PreparedStatement statement =  PreparedStatementCacheFactory.getInstance().get(session, cql.toString(), ConsistencyLevel.ONE);
			
					BoundStatement query = new BoundStatement(statement);
					query.setString(0, tag.getTag_id());
					query.setTimestamp(1, DateUtils.isoToTimestamp(from, true));
					query.setTimestamp(2, DateUtils.isoToTimestamp(to, true));


					ResultSetFuture rsf = session.executeAsync(query);
					ResultSet rs = rsf.getUninterruptibly();
					
					List<Row> list = rs.all();
					if(list != null && list.size() > 0){
						Row row = list.get(0);
						object = JSONObject.fromObject(row.getString(0));
					}
			//
			long end = System.currentTimeMillis() - start;

			//
			log.debug("Cassandra select tag data snapshot : exec_time=[" + end + "]");
			//
			return object;

		} catch (Exception ex) {
			log.error("Table[" + table + "] select tag data for snapshot error : CQL=[" + cql.toString() + "], " + ex.getMessage(), ex);
			throw ex;
		} finally {
			//
			// if (session != null && !session.isClosed())
			// session.close();
		}
	}
	
	
	@Override
	public List<Map<String, Object>> selectBLOBList(String tag_id, String from, String to, int limit) throws Exception {
		//

		List<Map<String, Object>> result = null;

		Session session = null;

		String table = "";
		StringBuffer cql = null;
		//
		try {

			//
			long start = System.currentTimeMillis();

			//
			session = CassandraSessionFixer.getSession();
			String keyspace = CassandraSessionFixer.getKeyspace();

			//
			result = new ArrayList<Map<String, Object>>();

			//
			cql = new StringBuffer();
			cql.append(" SELECT * FROM " + keyspace + "." + StorageConstants.TAG_BLOB_TABLE + " WHERE " + CassandraColumns.TAG_ID + " = ? " + " AND " + CassandraColumns.TIMESTAMP + " >= ? AND "
					+ CassandraColumns.TIMESTAMP + " <= ?  ");
			cql.append("  LIMIT ? ");
			

			PreparedStatement statement =  PreparedStatementCacheFactory.getInstance().get(session, cql.toString(), ConsistencyLevel.ONE);
			
			BoundStatement query = new BoundStatement(statement);
			query.setString(0,    tag_id);
			query.setTimestamp(1, DateUtils.isoToTimestamp(from, true));
			query.setTimestamp(2, DateUtils.isoToTimestamp(to, true));
			query.setInt(3,       limit);
			
			
			query.setFetchSize(QUERY_POINT_FETCH_SIZE);

			ResultSetFuture rsf = session.executeAsync(query);
			ResultSet rs = rsf.getUninterruptibly();
			
		    Iterator<Row> iter = rs.iterator();
		    while (iter.hasNext()) {
		        if (rs.getAvailableWithoutFetching() == 100 && !rs.isFullyFetched()) {
		           rs.fetchMoreResults();
		        }
		        Row row = iter.next();
		        Map<String, Object> map = new HashMap<String, Object>();
		        map.put("tag_id",     row.getString(CassandraColumns.TAG_ID));
		        map.put("timestamp",  new Date(row.getTimestamp(CassandraColumns.TIMESTAMP).getTime()));
		        map.put("object_id",  row.getUUID(CassandraColumns.OBJECT_ID));
		        map.put("object_id_text",  row.getUUID(CassandraColumns.OBJECT_ID).toString());
		        map.put("file_path",  row.getString(CassandraColumns.FILE_PATH));
		        map.put("file_size",  row.getLong(CassandraColumns.FILE_SIZE));
		        map.put("mime_type",  row.getString(CassandraColumns.MIME_TYPE));
				map.put("attribute",  row.getMap(CassandraColumns.ATTRIBUTE, String.class, String.class));
				//
				result.add(map);
		      
		    };
		    
	
			//
			long end = System.currentTimeMillis() - start;

			//
			log.debug("Cassandra select BLOB data list : exec_time=[" + end + "]");
			//
			return result;

		} catch (Exception ex) {
			log.error("Table[" + table + "] select BLOB data list error : CQL=[" + cql.toString() + "], " + ex.getMessage(), ex);
			throw ex;
		} finally {
			
		}
	};
	
	
	@Override
	public Map<String, Object> selectBLOBByObjectId(UUID object_id) throws Exception {
		//

		Map<String, Object> result = null;

		Session session = null;

		String table = "";
		StringBuffer cql = null;
		//
		try {

			//
			long start = System.currentTimeMillis();

			//
			session = CassandraSessionFixer.getSession();
			String keyspace = CassandraSessionFixer.getKeyspace();

			//
			result = new HashMap<String, Object>();

			//
			cql = new StringBuffer();
			cql.append(" SELECT * FROM " + keyspace + "." + StorageConstants.TAG_BLOB_TABLE + " WHERE " + CassandraColumns.OBJECT_ID + " = ? " );
			

			PreparedStatement statement =  PreparedStatementCacheFactory.getInstance().get(session, cql.toString(), ConsistencyLevel.ONE);
			
			BoundStatement query = new BoundStatement(statement);
			query.setUUID(0,    object_id);
			

			ResultSetFuture rsf = session.executeAsync(query);
			ResultSet rs = rsf.getUninterruptibly();
			
		    Iterator<Row> iter = rs.iterator();
		    while (iter.hasNext()) {
		        if (rs.getAvailableWithoutFetching() == 100 && !rs.isFullyFetched()) {
		           rs.fetchMoreResults();
		        }
		        Row row = iter.next();
		        Map<String, Object> map = new HashMap<String, Object>();
		        map.put("tag_id",     row.getString(CassandraColumns.TAG_ID));
		        map.put("timestamp",  new Date(row.getTimestamp(CassandraColumns.TIMESTAMP).getTime()));
		        map.put("object_id",  row.getUUID(CassandraColumns.OBJECT_ID));
		        map.put("object_id_text",  row.getUUID(CassandraColumns.OBJECT_ID).toString());
		        map.put("file_path",  row.getString(CassandraColumns.FILE_PATH));
		        map.put("file_size",  row.getLong(CassandraColumns.FILE_SIZE));
		        map.put("mime_type",  row.getString(CassandraColumns.MIME_TYPE));
				map.put("attribute",  row.getMap(CassandraColumns.ATTRIBUTE, String.class, String.class));
				//
				result.putAll(map);
		      
		    };
		    
	
			//
			long end = System.currentTimeMillis() - start;

			//
			log.debug("Cassandra select BLOB data by object_id : exec_time=[" + end + "]");
			//
			return result;

		} catch (Exception ex) {
			log.error("Table[" + table + "] select BLOB data by object_id error : CQL=[" + cql.toString() + "], " + ex.getMessage(), ex);
			throw ex;
		} finally {
			
		}
	}

	@Override
	public Map<String, Object> selectBLOBObject(UUID object_id) throws Exception {
		//

		Map<String, Object>  result = null;

		Session session = null;

		String table = "";
		StringBuffer cql = null;
		//
		try {

			//
			long start = System.currentTimeMillis();

			//
			session = CassandraSessionFixer.getSession();
			String keyspace = CassandraSessionFixer.getKeyspace();

			//
			result = new HashMap<String, Object>();

			//
			cql = new StringBuffer();
			cql.append(" SELECT * FROM " + keyspace + "." + StorageConstants.TAG_BLOB_OBJECT_TABLE + " WHERE " + CassandraColumns.OBJECT_ID + " = ? ");
	
			PreparedStatement statement =  PreparedStatementCacheFactory.getInstance().get(session, cql.toString(), ConsistencyLevel.ONE);
			
			BoundStatement query = new BoundStatement(statement);
			query.setUUID(0,    object_id);
			

			ResultSetFuture rsf = session.executeAsync(query);
			ResultSet rs = rsf.getUninterruptibly();
			
		    Iterator<Row> iter = rs.iterator();
		    while (iter.hasNext()) {
		        if (rs.getAvailableWithoutFetching() == 100 && !rs.isFullyFetched()) {
		           rs.fetchMoreResults();
		        }
		        Row row = iter.next();
		        Map<String, Object> map = new HashMap<String, Object>();
		        map.put("object_id",  row.getUUID(CassandraColumns.OBJECT_ID));
		        map.put("object",     Bytes.getArray(row.getBytes(CassandraColumns.OBJECT)));
				//
		        result.putAll(map);
		    };
		    
	
			//
			long end = System.currentTimeMillis() - start;

			//
			log.debug("Cassandra select BLOB data : exec_time=[" + end + "]");
			//
			return result;

		} catch (Exception ex) {
			log.error("Table[" + table + "] select BLOB data error : CQL=[" + cql.toString() + "], " + ex.getMessage(), ex);
			throw ex;
		} finally {
			
		}
	}

	
	
	
	@Override
	public JSONObject selectAlarmCount(Tag tag, long current_timestamp, String from, String to) throws Exception {
		//

		JSONObject object = null;

		Session session = null;

		String table = "";
		//
		try {

			//
			long start = System.currentTimeMillis();

			//
			session = CassandraSessionFixer.getSession();
			String keyspace = CassandraSessionFixer.getKeyspace();

			//
			StringBuffer cql = new StringBuffer();
			cql.append(" SELECT JSON * FROM " + keyspace
							+ "." + StorageConstants.TAG_ARLAM_TABLE  + " WHERE " + CassandraColumns.TAG_ID + " = ? AND " + CassandraColumns.TIMESTAMP + " >= ? AND "
							+ CassandraColumns.TIMESTAMP + " <= ? ALLOW FILTERING");
			
			PreparedStatement statement =  PreparedStatementCacheFactory.getInstance().get(session, cql.toString(), ConsistencyLevel.ONE);
			
					BoundStatement query = new BoundStatement(statement);
					query.setString(0, tag.getTag_id());
					query.setTimestamp(1, DateUtils.isoToTimestamp(from, true));
					query.setTimestamp(2, DateUtils.isoToTimestamp(to, true));

					ResultSetFuture rsf = session.executeAsync(query);
					ResultSet rs = rsf.getUninterruptibly();
					
					int info_count = 0;
					int warn_count = 0;
					int error_count = 0;
					List<Row> list = rs.all();
					if(list != null && list.size() > 0){
						Row row = list.get(0);
						JSONObject result = JSONObject.fromObject(row.getString(0));
						String priority = result.getString("priority");
						if(priority.equals("INFO")){
							info_count++;
						}
						if(priority.equals("WARN")){
							warn_count++;
						}
						if(priority.equals("ERROR")){
							error_count++;
						}
					};
					//
					object = new JSONObject();
					object.put("tag_id", tag.getTag_id());
					object.put("info_count",  info_count);
					object.put("warn_count",  warn_count);
					object.put("error_count", error_count);
			//
			long end = System.currentTimeMillis() - start;

			//
			log.debug("Cassandra select alarm error count snapshot : exec_time=[" + end + "]");
			//
			return object;

		} catch (Exception ex) {
			log.error("Table[" + table + "] select alarm error count for snapshot error : " + ex.getMessage(), ex);
			throw ex;
		} finally {
			//
			// if (session != null && !session.isClosed())
			// session.close();
		}
	};
	
	
	public JSONObject selectAlarmCountByAssetId(Asset asset, long current_timestamp, String from, String to) throws Exception{
		//

		JSONObject object = null;

		Session session = null;

		String table = "";
		//
		try {

			//
			long start = System.currentTimeMillis();

			//
			session = CassandraSessionFixer.getSession();
			String keyspace = CassandraSessionFixer.getKeyspace();

			//
			StringBuffer cql = new StringBuffer();
			cql.append(" SELECT JSON * FROM " + keyspace
							+ "." + StorageConstants.ASSET_ALARM_TABLE  + " WHERE " + CassandraColumns.ASSET_ID + " = ? AND " + CassandraColumns.TIMESTAMP + " >= ? AND "
							+ CassandraColumns.TIMESTAMP + " <= ? ALLOW FILTERING");
			
			PreparedStatement statement =  PreparedStatementCacheFactory.getInstance().get(session, cql.toString(), ConsistencyLevel.ONE);
			
					BoundStatement query = new BoundStatement(statement);
					query.setString(0, asset.getAsset_id());
					query.setTimestamp(1, DateUtils.isoToTimestamp(from, true));
					query.setTimestamp(2, DateUtils.isoToTimestamp(to, true));

					ResultSetFuture rsf = session.executeAsync(query);
					ResultSet rs = rsf.getUninterruptibly();
					
					int info_count = 0;
					int warn_count = 0;
					int error_count = 0;
					List<Row> list = rs.all();
					if(list != null && list.size() > 0){
						Row row = list.get(0);
						JSONObject result = JSONObject.fromObject(row.getString(0));
						String priority = result.getString("priority");
						if(priority.equals("INFO")){
							info_count++;
						}
						if(priority.equals("WARN")){
							warn_count++;
						}
						if(priority.equals("ERROR")){
							error_count++;
						}
					};
					//
					object = new JSONObject();
					object.put("asset_id", asset.getAsset_id());
					object.put("info_count",  info_count);
					object.put("warn_count",  warn_count);
					object.put("error_count", error_count);
			//
			long end = System.currentTimeMillis() - start;

			//
			log.debug("Cassandra select alarm error count snapshot : exec_time=[" + end + "]");
			//
			return object;

		} catch (Exception ex) {
			log.error("Table[" + table + "] select alarm error count for snapshot error : " + ex.getMessage(), ex);
			throw ex;
		} finally {
			//
			// if (session != null && !session.isClosed())
			// session.close();
		}
		
	}
	
	@Override
	public JSONObject selectAlarmCountByDate(Tag tag, int year, int month, int day) throws Exception {
		//

		JSONObject object = null;

		Session session = null;

		String table = "";
		//
		try {

			//
			long start = System.currentTimeMillis();

			//
			session = CassandraSessionFixer.getSession();
			String keyspace = CassandraSessionFixer.getKeyspace();

			//
			StringBuffer cql = new StringBuffer();
			cql.append(" SELECT SUM(alarm_count) as alarm_count,  SUM(alarm_count_by_info) as alarm_count_by_info,  SUM(alarm_count_by_warn) as alarm_count_by_warn,  SUM(alarm_count_by_error) as alarm_count_by_error FROM " + keyspace
							+ "." + StorageConstants.TAG_ARLAM_COUNT_TABLE  + " WHERE " + CassandraColumns.TAG_ID + " = ? AND "
							+ CassandraColumns.YEAR   + "  = ? AND "
							+ CassandraColumns.MONTH  + " = ? AND "
							+ CassandraColumns.DAY    + " = ?  "
							+ " ALLOW FILTERING ");
			
			PreparedStatement statement =  PreparedStatementCacheFactory.getInstance().get(session, cql.toString(), ConsistencyLevel.ONE);
			
					BoundStatement query = new BoundStatement(statement);
					query.setString(0, tag.getTag_id());
					query.setInt(1, year);
					query.setInt(2, month);
					query.setInt(3, day);

					//
					ResultSetFuture rsf = session.executeAsync(query);
					ResultSet rs = rsf.getUninterruptibly();
					
					//
					List<Row> list = rs.all();
					if(list != null && list.size() > 0){
						Row row = list.get(0);
						object = new JSONObject();
						object.put("tag_id", tag.getTag_id());
						object.put("alarm_count",          row.getLong("alarm_count"));
						object.put("alarm_count_by_info",  row.getLong("alarm_count_by_info"));
						object.put("alarm_count_by_warn",  row.getLong("alarm_count_by_warn"));
						object.put("alarm_count_by_error", row.getLong("alarm_count_by_error"));
					};
					//
			//
			long end = System.currentTimeMillis() - start;

			//
			log.debug("Cassandra select alarm error count snapshot : exec_time=[" + end + "]");
			//
			return object;

		} catch (Exception ex) {
			log.error("Table[" + table + "] select alarm error count for snapshot error : " + ex.getMessage(), ex);
			throw ex;
		} finally {
			//
			// if (session != null && !session.isClosed())
			// session.close();
		}
	}

	
	@Override
	public JSONObject selectAlarmErrorCountForSnapshot(Tag tag, long current_timestamp, String from, String to) throws Exception {
		//

		JSONObject object = null;

		Session session = null;

		String table = "";
		//
		try {

			//
			long start = System.currentTimeMillis();

			//
			session = CassandraSessionFixer.getSession();
			String keyspace = CassandraSessionFixer.getKeyspace();

			/*
			 * SELECT JSON tag_id, sum(alarm_count_by_error) AS error_count 
				FROM pp.tm_TAG_ALArM_COUNT
				WHERE year IN (2020, 2021)  
						AND (month, day, hour, minute) >= (01, 01, 01, 01)
							  AND (month, day, hour, minute) <= (12, 12, 12, 12) 
							  group by tag_id
							  allow filtering
			  */
			
			
			//
			StringBuffer cql = new StringBuffer();
			cql.append(" SELECT JSON tag_id, sum(alarm_count_by_error) AS error_count FROM " + keyspace + "." + StorageConstants.TAG_ARLAM_COUNT_TABLE + " ");
			cql.append(" WHERE " + CassandraColumns.TAG_ID + " = ?  ");
			cql.append("   AND year IN (?, ?)   ");
			cql.append("   AND (month, day, hour, minute) >= (?, ?, ?, ?)   ");
			cql.append("   AND (month, day, hour, minute) <= (?, ?, ?, ?)  ");
			    
		    LocalDateTime from_ldt = DateUtils.isoToLocalDateTime(from);
		    LocalDateTime to_ldt = DateUtils.isoToLocalDateTime(to);
		
		    PreparedStatement statement =  PreparedStatementCacheFactory.getInstance().get(session, cql.toString(), ConsistencyLevel.ONE);
			
			BoundStatement query = new BoundStatement(statement);
			int index = 0;
			query.setString(index++, tag.getTag_id());
			query.setInt(index++, from_ldt.getYear());
			query.setInt(index++, to_ldt.getYear());
			
			query.setInt(index++, from_ldt.getMonthValue());
			query.setInt(index++, from_ldt.getDayOfMonth());
			query.setInt(index++, from_ldt.getHour());
			query.setInt(index++, from_ldt.getMinute());
			
			query.setInt(index++, to_ldt.getMonthValue());
			query.setInt(index++, to_ldt.getDayOfMonth());
			query.setInt(index++, to_ldt.getHour());
			query.setInt(index++, to_ldt.getMinute());

			ResultSetFuture rsf = session.executeAsync(query);
			ResultSet rs = rsf.getUninterruptibly();
			
			List<Row> list = rs.all();
			if(list != null && list.size() > 0){
				Row row = list.get(0);
				object = JSONObject.fromObject(row.getString(0));
			}
		
			
			//
			long end = System.currentTimeMillis() - start;

			//
			log.debug("Cassandra select alarm error count snapshot : exec_time=[" + end + "]");
			//
			return object;

		} catch (Exception ex) {
			log.error("Table[" + table + "] select alarm error count for snapshot error : " + ex.getMessage(), ex);
			throw ex;
		} finally {
			//
			// if (session != null && !session.isClosed())
			// session.close();
		}
	}

	
	@Override
	public JSONObject selectPointAggregatation(Tag tag, String from, String to, String sampling, String term) throws Exception {
		//

		JSONObject object = null;

		Session session = null;

		String table = "";
		StringBuffer cql = null;
		//
		try {

			//
			long start = System.currentTimeMillis();

			//
			session = CassandraSessionFixer.getSession();
			String keyspace = CassandraSessionFixer.getKeyspace();

			//
			cql = new StringBuffer();
			if (StringUtils.isNotEmpty(sampling) && StringUtils.isNotEmpty(term)) {

				String java_type = tag.getJava_type();
				
				//샘플링 포인트 집계
				if (getJavaType(java_type).equals("int") || getJavaType(java_type).equals("long") || getJavaType(java_type).equals("float") || getJavaType(java_type).equals("double")) {

					String function_name = super.getSamplingFunctionName();

					cql.append(" SELECT " + CassandraColumns.TIMESTAMP + ", MIN(" + function_name + ") as min, MAX(" + function_name + ") as max, AVG(" + function_name + ") as avg, SUM(" + function_name + ") as sum  FROM " + keyspace
							+ "." + StorageConstants.TAG_POINT_SAMPLING_TABLE + "_" + term + " WHERE " + CassandraColumns.TAG_ID + " = ? AND " + CassandraColumns.TIMESTAMP + " >= ? AND "
							+ CassandraColumns.TIMESTAMP + " <= ?  " + " ALLOW FILTERING");

					
					PreparedStatement statement =  PreparedStatementCacheFactory.getInstance().get(session, cql.toString(), ConsistencyLevel.ONE);
					
					BoundStatement query = new BoundStatement(statement);
					query.setString(0, tag.getTag_id());
					query.setTimestamp(1, DateUtils.isoToTimestamp(from, true));
					query.setTimestamp(2, DateUtils.isoToTimestamp(to, true));
					
					ResultSetFuture rsf = session.executeAsync(query);
					ResultSet rs = rsf.getUninterruptibly();
					
					List<Row> list = rs.all();

					Collections.reverse(list);

					object = new JSONObject();
					for (int i = 0; list != null && i < list.size(); i++) {
						Row row = list.get(i);

						if(sampling.equals("COUNT")){ //카운트일경우 Int를 반환
							object.put("min", row.getInt("min"));
							object.put("max", row.getInt("max"));
							object.put("avg", row.getInt("avg"));
							object.put("sum", row.getInt("sum"));
						}else{
							object.put("min", row.getDouble("min"));
							object.put("max", row.getDouble("max"));
							object.put("avg", row.getDouble("avg"));
							object.put("sum", row.getDouble("sum"));
						}
					}
					
				//문자형 및 기타 
				} else {
					
					object = new JSONObject();
					object.put("min", "");
					object.put("max", "");
					object.put("avg", "");
					object.put("sum", "");
					
				}

			} else {

				String java_type = tag.getJava_type();
				if (getJavaType(java_type).equals("int") || getJavaType(java_type).equals("long") || getJavaType(java_type).equals("float") || getJavaType(java_type).equals("double")) {

					String filed_name = getFieldNameWithCondition(java_type);
					//
					cql.append(" SELECT MIN(" + filed_name + ") as min, MAX(" + filed_name + ") as max, AVG(" + filed_name + ") as avg, SUM(" + filed_name + ") as sum FROM " + keyspace + "." + StorageConstants.TAG_POINT_TABLE
							+ " WHERE  " + CassandraColumns.TAG_ID + " = ? " 
							+ " AND  " + CassandraColumns.TIMESTAMP + " >= ? AND  " + CassandraColumns.TIMESTAMP + " <= ? "
							
							+ " ALLOW FILTERING");

					PreparedStatement statement =  PreparedStatementCacheFactory.getInstance().get(session, cql.toString(), ConsistencyLevel.ONE);
					
					BoundStatement query = new BoundStatement(statement);
					query.setString(0, tag.getTag_id());
					query.setTimestamp(1, DateUtils.isoToTimestamp(from, true));
					query.setTimestamp(2, DateUtils.isoToTimestamp(to, true));
					

					ResultSetFuture rsf = session.executeAsync(query);
					ResultSet rs = rsf.getUninterruptibly();
					
					List<Row> list = rs.all();

					Collections.reverse(list);

					object = new JSONObject();
					for (int i = 0; list != null && i < list.size(); i++) {
						Row row = list.get(i);

						super.addJSONValueFromRow(row, object, java_type, "min");
						super.addJSONValueFromRow(row, object, java_type, "max");
						super.addJSONValueFromRow(row, object, java_type, "avg");
						super.addJSONValueFromRow(row, object, java_type, "sum");
					}

				} else {
					object = new JSONObject();
					object.put("min", "");
					object.put("max", "");
					object.put("avg", "");
					object.put("sum", "");
				}

			}
			//
			long end = System.currentTimeMillis() - start;

			//
			log.debug("Cassandra select tag data aggregate : exec_time=[" + end + "]");
			//
			return object;

		} catch (Exception ex) {
			log.error("Table[" + table + "] select tag aggregate error : CQL=[" + cql.toString() + "], " + ex.getMessage(), ex);
			throw ex;
		} finally {
			//
			// if (session != null && !session.isClosed())
			// session.close();
		}
	}

	
	
	@Override
	public JSONArray selectPointSnapshot(String site_id, String from, String to, String term, int limit) throws Exception {
		//

		JSONArray array = null;

		Session session = null;

		String table = "";
		StringBuffer cql = null;
		//
		try {

			//
			long start = System.currentTimeMillis();

			//
			session = CassandraSessionFixer.getSession();
			String keyspace = CassandraSessionFixer.getKeyspace();
			
			//데이 리스트
			List<Date> dates = DateUtils.getBetweenDates(DateUtils.isoToTimestamp(from, true), DateUtils.isoToTimestamp(to, true));

			//
			cql = new StringBuffer();
			cql.append(" SELECT " + CassandraColumns.SNAPSHOT_ID + ", " + CassandraColumns.TIMESTAMP + ", " + CassandraColumns.DATA_MAP + ", " + CassandraColumns.ALARM_MAP + "  FROM " + keyspace + "."
					+ StorageConstants.TAG_POINT_SNAPSHOT_TABLE +  " WHERE  " + CassandraColumns.SITE_ID + " = ? AND " + CassandraColumns.SNAPSHOT_ID + " = ? AND " + CassandraColumns.DATE + " = ? AND "  + CassandraColumns.HOUR + " = ? AND " + CassandraColumns.TIMESTAMP + " >= ? AND  " + CassandraColumns.TIMESTAMP + " <= ?  LIMIT ? ALLOW FILTERING");

			PreparedStatement statement =  PreparedStatementCacheFactory.getInstance().get(session, cql.toString(), ConsistencyLevel.ONE);
			
			List<Row> list =  new ArrayList<Row>();
			List<ResultSetFuture> futures = new ArrayList<>();
	        for (Date date : dates) {
	        	for (int hour = 25 - 1; hour >= 0; hour--) {
					BoundStatement query = new BoundStatement(statement);
					query.setString(0, site_id);
					query.setString(1, "SNAPSHOT_" + term.replace(" ", "_"));
					query.setInt(2, DateUtils.formatInt(date));
					query.setInt(3, hour);
					query.setTimestamp(4, DateUtils.isoToTimestamp(from, true));
					query.setTimestamp(5, DateUtils.isoToTimestamp(to, true));
					query.setInt(6, limit);
					
					query.setFetchSize(QUERY_SNAPSHOT_FETCH_SIZE);
					
					ResultSetFuture future = session.executeAsync(query);
		            futures.add(future);
	        	};
	        };
	        
	        for (ResultSetFuture future : futures) {
	            try {
	            	ResultSet rs = future.getUninterruptibly();
	                Iterator<Row> iter = rs.iterator();
	    		    while (iter.hasNext()) {
	    		        if (rs.getAvailableWithoutFetching() == QUERY_SNAPSHOT_FETCH_SIZE && !rs.isFullyFetched()) {
	    		           rs.fetchMoreResults();
	    		        };
	    		    Row row = iter.next();
	                list.add(row);
	    		    };
	            } catch (Exception qe) {
	            	log.error("Select tag snapshot partition query error : CQL=[" + cql.toString() + "], " + qe.getMessage(), qe);
	            }
	        };
		        
	        //
			array = new JSONArray();
			for (int i = 0; list != null && i < list.size(); i++) {
				Row row = list.get(i);
				//
				JSONObject json = new JSONObject();
				String snapshot_id = row.getString(CassandraColumns.SNAPSHOT_ID);
				long timestamp = row.getTimestamp(CassandraColumns.TIMESTAMP).getTime();
				JSONObject data = JSONObject.fromObject(row.getMap(CassandraColumns.DATA_MAP, String.class, String.class));
				JSONObject alarm = JSONObject.fromObject(row.getMap(CassandraColumns.ALARM_MAP, String.class, Integer.class));
				json.put("snapshot_id", snapshot_id);
				json.put("timestamp", timestamp);
				json.put("data", data);
				json.put("alarm", alarm);
				//
				if (!data.isEmpty()) {
					array.add(json);
				}
			}

			//
			long end = System.currentTimeMillis() - start;

			//
			log.debug("Cassandra select tag data snapshot : exec_time=[" + end + "]");
			//
			return array;

		} catch (Exception ex) {
			log.error("Table[" + table + "] select tag snapshot error : CQL=[" + cql.toString() + "], " + ex.getMessage(), ex);
			throw ex;
		} finally {
			//
			// if (session != null && !session.isClosed())
			// session.close();
		}
	}
	
	
	@Override
	public JSONArray selectAssetData(Asset asset, int date, String from, String to, int limit) throws Exception {
		//

		JSONArray array = null;

		Session session = null;

		String table = "";
		StringBuffer cql = null;
		//
		try {

			//
			long start = System.currentTimeMillis();

			//
			session = CassandraSessionFixer.getSession();
			String keyspace = CassandraSessionFixer.getKeyspace();

			//
			//
			table = StorageConstants.ASSET_DATA_TABLE;
			//
			cql = new StringBuffer();
			//
			//  카산드라 4.0 새로운 컨디션 방법
			//  and (hour,minute,second,data_timestamp) >= (0,0,0,  1498853265000);
			//  and (hour,minute,second,data_timestamp) < (24,61,61,1467317265000) 
		    //  
			//
			cql.append(" SELECT JSON * FROM " + keyspace + "." + table );
			cql.append("  WHERE  " + CassandraColumns.ASSET_ID  + " = ? ");
			cql.append("    AND  " + CassandraColumns.DATE      + " = ? ");
			cql.append("    AND  " + CassandraColumns.HOUR      + " = ? ");
			cql.append("    AND  (" + CassandraColumns.MINUTE + "," + CassandraColumns.TIMESTAMP + ") >= (?,?) ");
			cql.append("    AND  (" + CassandraColumns.MINUTE + "," + CassandraColumns.TIMESTAMP + ") <= (?,?)  ");
			cql.append("    LIMIT  " + limit + "  ");

			PreparedStatement statement =  PreparedStatementCacheFactory.getInstance().get(session, cql.toString(), ConsistencyLevel.ONE);
			
			List<Row> list =  new ArrayList<Row>();
			List<ResultSetFuture> futures = new ArrayList<>();
			for (int hour = 25 - 1; hour >= 0; hour--) { //24시간 FOR
				BoundStatement query = new BoundStatement(statement);
				query.setString(0, asset.getAsset_id());
				query.setInt(1, date);
				query.setInt(2, hour);
				query.setInt(3, 0);
				query.setTimestamp(4, DateUtils.isoToTimestamp(from, true));
				query.setInt(5, 61);
				query.setTimestamp(6, DateUtils.isoToTimestamp(to, true));
				
				query.setFetchSize(QUERY_ASSET_FETCH_SIZE);
				
				ResultSetFuture future = session.executeAsync(query);
	            futures.add(future);
	        };
	        
	        for (ResultSetFuture future : futures) {
	            try {
	                ResultSet rs = future.getUninterruptibly();
	                Iterator<Row> iter = rs.iterator();
	    		    while (iter.hasNext()) {
	    		        if (rs.getAvailableWithoutFetching() == QUERY_ASSET_FETCH_SIZE && !rs.isFullyFetched()) {
	    		           rs.fetchMoreResults();
	    		        };
	    		    Row row = iter.next();
	                list.add(row);
	    		    };
	            } catch (Exception qe) {
	            	log.error("CQL Async partition query error : CQL=[" + cql.toString() + "], " + qe.getMessage(), qe);
	            }
	        };

			array = new JSONArray();
			for (int i = 0; list != null && i < list.size(); i++) {
				Row row = list.get(i);
				String json_str = row.getString(0);
				json_str = json_str.replaceAll("NaN", "null"); //
				JSONObject json = JSONObject.fromObject(json_str);
				array.add(json);
			}
			//
			long end = System.currentTimeMillis() - start;

			//
			log.debug("Cassandra select asset data list : exec_time=[" + end + "]");
			//
			return array;

		} catch (Exception ex) {
			log.error("Table[" + table + "] select asset data list error : CQL=[" + cql.toString() + "], " + ex.getMessage(), ex);
			throw ex;
		} finally {
			//
			// if (session != null && !session.isClosed())
			// session.close();
		}
	}

	@Override 
	public JSONArray selectAssetAlarm(Asset asset, String from, String to, int limit) throws Exception {
		//

		JSONArray array = null;

		Session session = null;

		StringBuffer cql = null;
		//
		try {

			//
			long start = System.currentTimeMillis();

			//
			session = CassandraSessionFixer.getSession();
			String keyspace = CassandraSessionFixer.getKeyspace();

			//
			cql = new StringBuffer();
			
			//
			cql.append(" SELECT JSON * FROM " + keyspace + "." + StorageConstants.ASSET_ALARM_TABLE + "_MV_BY_ASSET_ID " );
			cql.append("  WHERE  " + CassandraColumns.ASSET_ID + " = ? ");
			cql.append("    AND  " + CassandraColumns.TIMESTAMP + " >= ? ");
			cql.append("    AND  " + CassandraColumns.TIMESTAMP + " <= ?  ");
			cql.append("    LIMIT  " + limit + "  ");
			cql.append("   ");

			PreparedStatement statement =  PreparedStatementCacheFactory.getInstance().get(session, cql.toString(), ConsistencyLevel.ONE);
			
			BoundStatement query = new BoundStatement(statement);
			
			int index = 0;
			query.setString(index++, asset.getAsset_id());
			query.setTimestamp(index++, DateUtils.isoToTimestamp(from, true));
			query.setTimestamp(index++, DateUtils.isoToTimestamp(to, true));

			ResultSetFuture rsf = session.executeAsync(query);
			ResultSet rs = rsf.getUninterruptibly();

			List<Row> list = rs.all();

			array = new JSONArray();
			for (int i = 0; list != null && i < list.size(); i++) {
				Row row = list.get(i);
				String json_str = row.getString(0);
				json_str = json_str.replaceAll("NaN", "null"); //
				JSONObject json = JSONObject.fromObject(json_str);
				array.add(json);
			}
			//
			long end = System.currentTimeMillis() - start;

			//
			log.debug("Cassandra select asset alarm list : exec_time=[" + end + "]");
			//
			return array;

		} catch (Exception ex) {
			log.error("Table[" + StorageConstants.ASSET_ALARM_TABLE + "_MV_BY_ASSET_ID] select asset alarm list error : CQL=[" + cql.toString() + "], " + ex.getMessage(), ex);
			throw ex;
		} finally {
			//
			// if (session != null && !session.isClosed())
			// session.close();
		}
	};
	
	
	@Override 
	public JSONObject selectAssetAlarmCountByPriority(Asset asset, String from, String to, String priority, int limit) throws Exception {
		//

		JSONObject object = null;

		Session session = null;

		StringBuffer cql = null;
		//
		try {

			//
			long start = System.currentTimeMillis();

			//
			session = CassandraSessionFixer.getSession();
			String keyspace = CassandraSessionFixer.getKeyspace();

			//
			cql = new StringBuffer();
			
			//
			cql.append(" SELECT JSON PRIORITY, COUNT(*) AS COUNT FROM " + keyspace + "." + StorageConstants.ASSET_ALARM_TABLE + " " );
			cql.append("  WHERE  " + CassandraColumns.ASSET_ID + " = ? ");
			cql.append("    AND  " + CassandraColumns.TIMESTAMP + " >= ? ");
			cql.append("    AND  " + CassandraColumns.TIMESTAMP + " <= ?  ");
			cql.append("    AND " + CassandraColumns.PRIORITY   + " =  ?  ");
			cql.append("    LIMIT  " + limit + "  ");
			cql.append("  ALLOW FILTERING ");

			PreparedStatement statement =  PreparedStatementCacheFactory.getInstance().get(session, cql.toString(), ConsistencyLevel.ONE);
			
			BoundStatement query = new BoundStatement(statement);
			
			int index = 0;
			query.setString(index++, asset.getAsset_id());
			query.setTimestamp(index++, DateUtils.isoToTimestamp(from, true));
			query.setTimestamp(index++, DateUtils.isoToTimestamp(to, true));
			query.setString(index++, priority);

			ResultSetFuture rsf = session.executeAsync(query);
			ResultSet rs = rsf.getUninterruptibly();
			
			List<Row> list = rs.all();

			object = new JSONObject();
			for (int i = 0; list != null && i < list.size(); i++) {
				Row row = list.get(i);
				String json_str = row.getString(0);
				json_str = json_str.replaceAll("NaN", "null"); //
				object = JSONObject.fromObject(json_str);
			}
			//
			long end = System.currentTimeMillis() - start;

			//
			log.debug("Cassandra select asset alarm list : exec_time=[" + end + "]");
			//
			return object;

		} catch (Exception ex) {
			log.error("Table[" + StorageConstants.ASSET_ALARM_TABLE + "] select asset alarm list error : CQL=[" + cql.toString() + "], " + ex.getMessage(), ex);
			throw ex;
		} finally {
			//
			// if (session != null && !session.isClosed())
			// session.close();
		}
	};
	
	@Override 
	public JSONArray  selectAssetTimeline(Asset asset, String from, String to, int limit) throws Exception{
		//

		JSONArray array = null;

		Session session = null;

		StringBuffer cql = null;
		//
		try {

			//
			long start = System.currentTimeMillis();

			//
			session = CassandraSessionFixer.getSession();
			String keyspace = CassandraSessionFixer.getKeyspace();

			//
			cql = new StringBuffer();
			
			//
			cql.append(" SELECT JSON * FROM " + keyspace + "." + StorageConstants.ASSET_TIMELINE_TABLE + " " );
			cql.append("  WHERE  " + CassandraColumns.ASSET_ID + " = ? ");
			cql.append("    AND  " + CassandraColumns.TIMESTAMP + " >= ? ");
			cql.append("    AND  " + CassandraColumns.TIMESTAMP + " <= ?  ");
			cql.append("    LIMIT  " + limit + "  ");
			cql.append("  ALLOW FILTERING ");

			PreparedStatement statement =  PreparedStatementCacheFactory.getInstance().get(session, cql.toString(), ConsistencyLevel.ONE);
			
			BoundStatement query = new BoundStatement(statement);
			
			int index = 0;
			query.setString(index++, asset.getAsset_id());
			query.setTimestamp(index++, DateUtils.isoToTimestamp(from, true));
			query.setTimestamp(index++, DateUtils.isoToTimestamp(to, true));

			ResultSetFuture rsf = session.executeAsync(query);
			ResultSet rs = rsf.getUninterruptibly();
			
			
			List<Row> list = rs.all();

			array = new JSONArray();
			for (int i = 0; list != null && i < list.size(); i++) {
				Row row = list.get(i);
				String json_str = row.getString(0);
				json_str = json_str.replaceAll("NaN", "null"); //
				JSONObject json = JSONObject.fromObject(json_str);
				array.add(json);
			}
			//
			long end = System.currentTimeMillis() - start;

			//
			log.debug("Cassandra select asset timeline list : exec_time=[" + end + "]");
			//
			return array;

		} catch (Exception ex) {
			log.error("Table[" + StorageConstants.ASSET_TIMELINE_TABLE + "] select asset timeline list error : CQL=[" + cql.toString() + "], " + ex.getMessage(), ex);
			throw ex;
		} finally {
			//
			// if (session != null && !session.isClosed())
			// session.close();
		}
	}
	
	@Override
	public JSONArray selectAssetEvent(Asset asset, String from, String to, int limit) throws Exception {
		//

		JSONArray array = null;

		Session session = null;

		StringBuffer cql = null;
		//
		try {

			//
			long start = System.currentTimeMillis();

			//
			session = CassandraSessionFixer.getSession();
			String keyspace = CassandraSessionFixer.getKeyspace();

			//
			cql = new StringBuffer();
			
			//
			cql.append(" SELECT JSON * FROM " + keyspace + "." + StorageConstants.ASSET_EVENT_TABLE + " " );
			cql.append("  WHERE  " + CassandraColumns.ASSET_ID + " = ? ");
			cql.append("    AND  " + CassandraColumns.TIMESTAMP + " >= ? ");
			cql.append("    AND  " + CassandraColumns.TIMESTAMP + " <= ?  ");
			cql.append("    LIMIT  " + limit + "  ");
			cql.append("  ALLOW FILTERING ");

			PreparedStatement statement =  PreparedStatementCacheFactory.getInstance().get(session, cql.toString(), ConsistencyLevel.ONE);
			
			BoundStatement query = new BoundStatement(statement);
			
			int index = 0;
			query.setString(index++, asset.getAsset_id());
			query.setTimestamp(index++, DateUtils.isoToTimestamp(from, true));
			query.setTimestamp(index++, DateUtils.isoToTimestamp(to, true));

			ResultSetFuture rsf = session.executeAsync(query);
			ResultSet rs = rsf.getUninterruptibly();
			
			
			List<Row> list = rs.all();

			array = new JSONArray();
			for (int i = 0; list != null && i < list.size(); i++) {
				Row row = list.get(i);
				String json_str = row.getString(0);
				json_str = json_str.replaceAll("NaN", "null"); //
				JSONObject json = JSONObject.fromObject(json_str);
				array.add(json);
			}
			//
			long end = System.currentTimeMillis() - start;

			//
			log.debug("Cassandra select asset health status list : exec_time=[" + end + "]");
			//
			return array;

		} catch (Exception ex) {
			log.error("Table[" + StorageConstants.ASSET_EVENT_TABLE + "] select asset event list error : CQL=[" + cql.toString() + "], " + ex.getMessage(), ex);
			throw ex;
		} finally {
			//
			// if (session != null && !session.isClosed())
			// session.close();
		}
	};
	
	
	@Override
	public JSONArray selectAssetEvent(Asset asset, int limit) throws Exception {
		//

		JSONArray array = null;

		Session session = null;

		StringBuffer cql = null;
		//
		try {

			//
			long start = System.currentTimeMillis();

			//
			session = CassandraSessionFixer.getSession();
			String keyspace = CassandraSessionFixer.getKeyspace();

			//
			cql = new StringBuffer();
			
			//
			cql.append(" SELECT JSON * FROM " + keyspace + "." + StorageConstants.ASSET_EVENT_TABLE + " " );
			cql.append("  WHERE  " + CassandraColumns.ASSET_ID + " = ? ");
			cql.append("    LIMIT  " + limit + "  ");
			cql.append("  ALLOW FILTERING ");

			PreparedStatement statement =  PreparedStatementCacheFactory.getInstance().get(session, cql.toString(), ConsistencyLevel.ONE);
			
			BoundStatement query = new BoundStatement(statement);
			
			int index = 0;
			query.setString(index++, asset.getAsset_id());

			ResultSetFuture rsf = session.executeAsync(query);
			ResultSet rs = rsf.getUninterruptibly();
			
			
			List<Row> list = rs.all();

			array = new JSONArray();
			for (int i = 0; list != null && i < list.size(); i++) {
				Row row = list.get(i);
				String json_str = row.getString(0);
				json_str = json_str.replaceAll("NaN", "null"); //
				JSONObject json = JSONObject.fromObject(json_str);
				array.add(json);
			}
			//
			long end = System.currentTimeMillis() - start;

			//
			log.debug("Cassandra select asset health status list : exec_time=[" + end + "]");
			//
			return array;

		} catch (Exception ex) {
			log.error("Table[" + StorageConstants.ASSET_EVENT_TABLE + "] select asset event list error : CQL=[" + cql.toString() + "], " + ex.getMessage(), ex);
			throw ex;
		} finally {
			//
			// if (session != null && !session.isClosed())
			// session.close();
		}
	}


	@Override
	public JSONArray selectAssetAggregation(Asset asset, String from, String to, int limit) throws Exception {
		//

		JSONArray array = null;

		Session session = null;

		StringBuffer cql = null;
		//
		try {

			//
			long start = System.currentTimeMillis();

			//
			session = CassandraSessionFixer.getSession();
			String keyspace = CassandraSessionFixer.getKeyspace();

			//
			cql = new StringBuffer();
			
			//
			cql.append(" SELECT JSON * FROM " + keyspace + "." + StorageConstants.ASSET_AGGREGATION_TABLE + " " );
			cql.append("  WHERE  " + CassandraColumns.ASSET_ID + " = ? ");
			cql.append("    AND  " + CassandraColumns.TIMESTAMP + " >= ? ");
			cql.append("    AND  " + CassandraColumns.TIMESTAMP + " <= ?  ");
			cql.append("    LIMIT  " + limit + "  ");
			cql.append("  ALLOW FILTERING ");

			PreparedStatement statement =  PreparedStatementCacheFactory.getInstance().get(session, cql.toString(), ConsistencyLevel.ONE);
			
			BoundStatement query = new BoundStatement(statement);
			
			int index = 0;
			query.setString(index++, asset.getAsset_id());
			query.setTimestamp(index++, DateUtils.isoToTimestamp(from, true));
			query.setTimestamp(index++, DateUtils.isoToTimestamp(to, true));

			ResultSetFuture rsf = session.executeAsync(query);
			ResultSet rs = rsf.getUninterruptibly();
			
			
			List<Row> list = rs.all();

			array = new JSONArray();
			for (int i = 0; list != null && i < list.size(); i++) {
				Row row = list.get(i);
				String json_str = row.getString(0);
				json_str = json_str.replaceAll("NaN", "null"); //
				JSONObject json = JSONObject.fromObject(json_str);
				array.add(json);
			}
			//
			long end = System.currentTimeMillis() - start;

			//
			log.debug("Cassandra select asset aggregation list : exec_time=[" + end + "]");
			//
			return array;

		} catch (Exception ex) {
			log.error("Table[" + StorageConstants.ASSET_AGGREGATION_TABLE + "] select asset aggregation list error : CQL=[" + cql.toString() + "], " + ex.getMessage(), ex);
			throw ex;
		} finally {
			//
			// if (session != null && !session.isClosed())
			// session.close();
		}
	}
	
	@Override
	public JSONArray selectAssetContext(Asset asset, String from, String to, int limit) throws Exception {
		//

		JSONArray array = null;

		Session session = null;

		StringBuffer cql = null;
		//
		try {

			//
			long start = System.currentTimeMillis();

			//
			session = CassandraSessionFixer.getSession();
			String keyspace = CassandraSessionFixer.getKeyspace();

			//
			cql = new StringBuffer();
			
			//
			cql.append(" SELECT JSON * FROM " + keyspace + "." + StorageConstants.ASSET_CONTEXT_TABLE + " " );
			cql.append("  WHERE  " + CassandraColumns.ASSET_ID + " = ? ");
			cql.append("    AND  " + CassandraColumns.TIMESTAMP + " >= ? ");
			cql.append("    AND  " + CassandraColumns.TIMESTAMP + " <= ?  ");
			cql.append("    LIMIT  " + limit + "  ");
			cql.append("  ALLOW FILTERING ");

			PreparedStatement statement =  PreparedStatementCacheFactory.getInstance().get(session, cql.toString(), ConsistencyLevel.ONE);
			
			BoundStatement query = new BoundStatement(statement);
			
			int index = 0;
			query.setString(index++, asset.getAsset_id());
			query.setTimestamp(index++, DateUtils.isoToTimestamp(from, true));
			query.setTimestamp(index++, DateUtils.isoToTimestamp(to, true));

			ResultSetFuture rsf = session.executeAsync(query);
			ResultSet rs = rsf.getUninterruptibly();
			
			
			List<Row> list = rs.all();

			array = new JSONArray();
			for (int i = 0; list != null && i < list.size(); i++) {
				Row row = list.get(i);
				String json_str = row.getString(0);
				json_str = json_str.replaceAll("NaN", "null"); //
				JSONObject json = JSONObject.fromObject(json_str);
				array.add(json);
			}
			//
			long end = System.currentTimeMillis() - start;

			//
			log.debug("Cassandra select asset context list : exec_time=[" + end + "]");
			//
			return array;

		} catch (Exception ex) {
			log.error("Table[" + StorageConstants.ASSET_CONTEXT_TABLE + "] select asset context list error : CQL=[" + cql.toString() + "], " + ex.getMessage(), ex);
			throw ex;
		} finally {
			//
			// if (session != null && !session.isClosed())
			// session.close();
		}
	}

	

	
	@Override 
	public JSONArray selectAssetHealthStatus(Asset asset, String from, String to) throws Exception {
		//

		JSONArray array = null;

		Session session = null;

		StringBuffer cql = null;
		//
		try {

			//
			long start = System.currentTimeMillis();

			//
			session = CassandraSessionFixer.getSession();
			String keyspace = CassandraSessionFixer.getKeyspace();

			//
			cql = new StringBuffer();
			
			//
			cql.append(" SELECT JSON * FROM " + keyspace + "." + StorageConstants.ASSET_HEALTH_STATUS_TABLE + " " );
			cql.append("  WHERE  " + CassandraColumns.ASSET_ID + " = ? ");
			cql.append("    AND  " + CassandraColumns.TIMESTAMP + " >= ? ");
			cql.append("    AND  " + CassandraColumns.TIMESTAMP + " <= ?  ");
			cql.append("  ALLOW FILTERING ");

			PreparedStatement statement =  PreparedStatementCacheFactory.getInstance().get(session, cql.toString(), ConsistencyLevel.ONE);
			
			BoundStatement query = new BoundStatement(statement);
			
			int index = 0;
			query.setString(index++, asset.getAsset_id());
			query.setTimestamp(index++, DateUtils.isoToTimestamp(from, true));
			query.setTimestamp(index++, DateUtils.isoToTimestamp(to, true));

			ResultSetFuture rsf = session.executeAsync(query);
			ResultSet rs = rsf.getUninterruptibly();
			
			List<Row> list = rs.all();

			array = new JSONArray();
			for (int i = 0; list != null && i < list.size(); i++) {
				Row row = list.get(i);
				String json_str = row.getString(0);
				json_str = json_str.replaceAll("NaN", "null"); //
				JSONObject json = JSONObject.fromObject(json_str);
				array.add(json);
			}
			//
			long end = System.currentTimeMillis() - start;

			//
			log.debug("Cassandra select asset health status list : exec_time=[" + end + "]");
			//
			return array;

		} catch (Exception ex) {
			log.error("Table[" + StorageConstants.ASSET_HEALTH_STATUS_TABLE + "] select asset health status list error : CQL=[" + cql.toString() + "], " + ex.getMessage(), ex);
			throw ex;
		} finally {
			//
			// if (session != null && !session.isClosed())
			// session.close();
		}
	};
	
	
	@Override 
	public JSONArray selectAssetConnectionStatus(Asset asset, String from, String to) throws Exception {
		//

		JSONArray array = null;

		Session session = null;

		StringBuffer cql = null;
		//
		try {

			//
			long start = System.currentTimeMillis();

			//
			session = CassandraSessionFixer.getSession();
			String keyspace = CassandraSessionFixer.getKeyspace();

			//
			cql = new StringBuffer();
			
			//
			cql.append(" SELECT JSON * FROM " + keyspace + "." + StorageConstants.ASSET_CONNECTION_STATUS_TABLE + " " );
			cql.append("  WHERE  " + CassandraColumns.ASSET_ID + " = ? ");
			cql.append("    AND  " + CassandraColumns.TIMESTAMP + " >= ? ");
			cql.append("    AND  " + CassandraColumns.TIMESTAMP + " <= ?  ");
			cql.append("  ALLOW FILTERING ");

			PreparedStatement statement =  PreparedStatementCacheFactory.getInstance().get(session, cql.toString(), ConsistencyLevel.ONE);
			
			BoundStatement query = new BoundStatement(statement);
			
			int index = 0;
			query.setString(index++, asset.getAsset_id());
			query.setTimestamp(index++, DateUtils.isoToTimestamp(from, true));
			query.setTimestamp(index++, DateUtils.isoToTimestamp(to, true));

			ResultSetFuture rsf = session.executeAsync(query);
			ResultSet rs = rsf.getUninterruptibly();
			
			List<Row> list = rs.all();

			array = new JSONArray();
			for (int i = 0; list != null && i < list.size(); i++) {
				Row row = list.get(i);
				String json_str = row.getString(0);
				json_str = json_str.replaceAll("NaN", "null"); //
				JSONObject json = JSONObject.fromObject(json_str);
				array.add(json);
			}
			//
			long end = System.currentTimeMillis() - start;

			//
			log.debug("Cassandra select asset connection status list : exec_time=[" + end + "]");
			//
			return array;

		} catch (Exception ex) {
			log.error("Table[" + StorageConstants.ASSET_CONNECTION_STATUS_TABLE + "] select asset connection status list error : CQL=[" + cql.toString() + "], " + ex.getMessage(), ex);
			throw ex;
		} finally {
			//
			// if (session != null && !session.isClosed())
			// session.close();
		}
	}
	
	
	@Override 
	public JSONArray selectSystemHealthStatus(String from, String to) throws Exception {
		//

		JSONArray array = null;

		Session session = null;

		StringBuffer cql = null;
		//
		try {

			//
			long start = System.currentTimeMillis();

			//
			session = CassandraSessionFixer.getSession();
			String keyspace = CassandraSessionFixer.getKeyspace();

			//
			cql = new StringBuffer();
		
			//
			cql.append(" SELECT JSON * FROM " + StorageConstants.SYSTEM_KEYSPACE  + "." + StorageConstants.SYSTEM_HEALTH_STATUS_TABLE + " " );
			cql.append("  WHERE  " + CassandraColumns.APP_NAME + " = ? ");
			cql.append("    AND  " + CassandraColumns.TIMESTAMP + " >= ? ");
			cql.append("    AND  " + CassandraColumns.TIMESTAMP + " <= ?  ");
			cql.append("  ALLOW FILTERING ");

			PreparedStatement statement =  PreparedStatementCacheFactory.getInstance().get(session, cql.toString(), ConsistencyLevel.ONE);
			
			BoundStatement query = new BoundStatement(statement);
			
			int index = 0;
			query.setString(index++,    StorageConstants.SYSTEM_APP_NAME);
			query.setTimestamp(index++, DateUtils.isoToTimestamp(from, true));
			query.setTimestamp(index++, DateUtils.isoToTimestamp(to, true));

			ResultSetFuture rsf = session.executeAsync(query);
			ResultSet rs = rsf.getUninterruptibly();
			
			List<Row> list = rs.all();

			array = new JSONArray();
			for (int i = 0; list != null && i < list.size(); i++) {
				Row row = list.get(i);
				String json_str = row.getString(0);
				json_str = json_str.replaceAll("NaN", "null"); //
				JSONObject json = JSONObject.fromObject(json_str);
				array.add(json);
			}
			//
			long end = System.currentTimeMillis() - start;

			//
			log.debug("Cassandra select system health status list : exec_time=[" + end + "]");
			//
			return array;

		} catch (Exception ex) {
			log.error("Table[" + StorageConstants.SYSTEM_HEALTH_STATUS_TABLE + "] select system health status list error : CQL=[" + cql.toString() + "], " + ex.getMessage(), ex);
			throw ex;
		} finally {
			//
			// if (session != null && !session.isClosed())
			// session.close();
		}
	}

	
	/*
	 * (non-Javadoc)
	 * @see plantpulse.cep.engine.storage.select.StorageSelectDAO#selectSystemLogList(java.lang.String, java.lang.String, java.lang.String, java.lang.String, int)
	 */
	@Override
	public JSONArray selectSystemLogList(String from, String to, String app_name, String level, String message, int limit) throws Exception {
		//

		JSONArray array = null;

		Session session = null;

		String table = "";
		StringBuffer cql = null;
		//
		try {

			//
			long start = System.currentTimeMillis();

			//
			session = CassandraSessionFixer.getSession();
			String keyspace = CassandraSessionFixer.getKeyspace();

			//
			cql = new StringBuffer();
			//
			cql.append(" SELECT *  FROM " + StorageConstants.SYSTEM_KEYSPACE + "." + StorageConstants.SYSTEM_LOG_TABLE  );
			cql.append("  WHERE  " + LoggingAppender.APP_NAME  + " = ? ");
			cql.append("    AND  " + LoggingAppender.TIMESTAMP + " >= ? ");
			cql.append("    AND  " + LoggingAppender.TIMESTAMP + " <= ?  ");
			if(StringUtils.isNotEmpty(level)){
				cql.append("    AND  " + LoggingAppender.LEVEL + " = ?  ");
			}
			if(StringUtils.isNotEmpty(message)){
				cql.append("    AND  " + LoggingAppender.MESSAGE + " LIKE ?  ");
			}
			cql.append("    LIMIT  " + limit + "  ");
			cql.append("  ALLOW FILTERING ");

			PreparedStatement statement =  PreparedStatementCacheFactory.getInstance().get(session, cql.toString(), ConsistencyLevel.ONE);
			
			BoundStatement query = new BoundStatement(statement);
			
			int index = 0;
			query.setString(index++,    app_name);
			query.setTimestamp(index++, DateUtils.isoToTimestamp(from, true));
			query.setTimestamp(index++, DateUtils.isoToTimestamp(to, true));
			if(StringUtils.isNotEmpty(level)){
				query.setString(index++, level);
			}
			if(StringUtils.isNotEmpty(message)){
				query.setString(index++, "%" + message + "%");
			}

			ResultSetFuture rsf = session.executeAsync(query);
			ResultSet rs = rsf.getUninterruptibly();
			
			List<Row> list = rs.all();

			array = new JSONArray();
			for (int i = 0; list != null && i < list.size(); i++) {
				Row row = list.get(i);
				JSONObject json = new JSONObject();
				json.put(LoggingAppender.APP_NAME,    row.getString(LoggingAppender.APP_NAME));
				json.put(LoggingAppender.TIMESTAMP,   row.getTimestamp(LoggingAppender.TIMESTAMP).getTime());
				json.put(LoggingAppender.TIMESTAMP + "_fmd",   plantpulse.cep.engine.utils.DateUtils.format(row.getTimestamp(LoggingAppender.TIMESTAMP)));
				json.put(LoggingAppender.LEVEL,       row.getString(LoggingAppender.LEVEL));
				json.put(LoggingAppender.CLASS_NAME,  row.getString(LoggingAppender.CLASS_NAME));
				json.put(LoggingAppender.METHOD_NAME, row.getString(LoggingAppender.METHOD_NAME));
				json.put(LoggingAppender.LINE_NUMBER, row.getString(LoggingAppender.LINE_NUMBER));
				json.put(LoggingAppender.MESSAGE,     row.getString(LoggingAppender.MESSAGE));
				array.add(json);
			}
			//
			long end = System.currentTimeMillis() - start;

			//
			log.debug("Cassandra select system log list : exec_time=[" + end + "]");
			//
			return array;

		} catch (Exception ex) {
			log.error("Table[" + table + "] select system log list error : CQL=[" + cql.toString() + "], " + ex.getMessage(), ex);
			throw ex;
		} finally {
			//
			// if (session != null && !session.isClosed())
			// session.close();
		}
	};
	
	
	@Override
	public JSONArray selectListenerPushCacheList(String url, int limit) throws Exception {
		//

		JSONArray array = null;

		Session session = null;

		String table = "";
		StringBuffer cql = null;
		//
		try {

			//
			long start = System.currentTimeMillis();

			//
			session = CassandraSessionFixer.getSession();
			String keyspace = CassandraSessionFixer.getKeyspace();

			//
			cql = new StringBuffer();
			//
			cql.append(" SELECT * FROM " + keyspace + "." + StorageConstants.OPTION_LISTENER_PUSH_CACHE_TABLE  );
			cql.append("  WHERE URL = ? ");
			cql.append("  LIMIT  ?  ");

			PreparedStatement statement =  PreparedStatementCacheFactory.getInstance().get(session, cql.toString(), ConsistencyLevel.ONE);
			
			BoundStatement query = new BoundStatement(statement);
			
			int index = 0;
			query.setString(index++, url);
			query.setInt(index++,    limit);

			ResultSetFuture rsf = session.executeAsync(query);
			ResultSet rs = rsf.getUninterruptibly();
			
			
			List<Row> list = rs.all();

			array = new JSONArray();
			for (int i = 0; list != null && i < list.size(); i++) {
				Row row = list.get(i);
				//JSONObject json = JSONObject.fromObject(row.getString("json"));
				array.add(row.getString("json"));
			}
			//
			long end = System.currentTimeMillis() - start;

			//
			log.debug("Cassandra select system log list : exec_time=[" + end + "]");
			//
			return array;

		} catch (Exception ex) {
			log.error("Table[" + table + "] select system log list error : CQL=[" + cql.toString() + "], " + ex.getMessage(), ex);
			throw ex;
		} finally {
			//
			// if (session != null && !session.isClosed())
			// session.close();
		}
	}
	

	/*
	 * (non-Javadoc)
	 * 
	 * @see
	 * plantpulse.cep.engine.storage.select.StorageSelectDAO#countTagTotal()
	 */
	@Override
	public long countPointTotal() throws Exception {
		//

		long total_count = 0;

		Session session = null;

		String table = "";
		StringBuffer cql = new StringBuffer();
		//
		try {

			//
			long start = System.currentTimeMillis();

			//
			//
			session = CassandraSessionFixer.getSession();
			String keyspace = CassandraSessionFixer.getKeyspace();
			
			//
			cql = new StringBuffer();
			cql.append(" SELECT * FROM " + keyspace + "." + StorageConstants.TAG_POINT_COUNT_TABLE + " ");

			PreparedStatement statement =  PreparedStatementCacheFactory.getInstance().get(session, cql.toString(), ConsistencyLevel.ONE);

			BoundStatement query = new BoundStatement(statement);
			
			ResultSetFuture rsf = session.executeAsync(query);
			ResultSet rs = rsf.getUninterruptibly();

			List<Row> list = rs.all();
			for (int i = 0; list != null && i < list.size(); i++) {
				Row row = list.get(i);
				total_count += row.getLong(CassandraColumns.COUNT);
			}

			//
			long end = System.currentTimeMillis() - start;

			//
			log.debug("Cassandra count tag point total : exec_time=[" + end + "]");
			//
			return total_count;

		} catch (Exception ex) {
			log.error("Table[" + table + "] total point count error : CQL=[" + cql.toString() + "], " + ex.getMessage(), ex);
			throw ex;
		} finally {
			//
			// if (session != null && !session.isClosed())
			// session.close();
		}
	}

	@Override
	public long countPointTotalBySite(Site site) throws Exception {
		//

		long total_count = 0;

		Session session = null;

		String table = "";
		StringBuffer cql = new StringBuffer();
		//
		try {

			//
			long start = System.currentTimeMillis();

			//
			//
			session = CassandraSessionFixer.getSession();
			String keyspace = CassandraSessionFixer.getKeyspace();
			//
			cql = new StringBuffer();
			cql.append(" SELECT * FROM " + keyspace + "." + StorageConstants.TAG_POINT_COUNT_BY_SITE_TABLE + " ");
			cql.append("   WHERE " + CassandraColumns.SITE_ID + " = ? ");

			PreparedStatement statement =  PreparedStatementCacheFactory.getInstance().get(session, cql.toString(), ConsistencyLevel.ONE);

			BoundStatement query = new BoundStatement(statement);
			query.setString(0, site.getSite_id());
			
			ResultSetFuture rsf = session.executeAsync(query);
			ResultSet rs = rsf.getUninterruptibly();

			List<Row> list = rs.all();
			for (int i = 0; list != null && i < list.size(); i++) {
				Row row = list.get(i);
				total_count += row.getLong(CassandraColumns.COUNT);
			}

			//
			long end = System.currentTimeMillis() - start;

			//
			log.debug("Cassandra count site point total : exec_time=[" + end + "]");
			//
			return total_count;

		} catch (Exception ex) {
			log.error("Table[" + table + "] total point count by site error : CQL=[" + cql.toString() + "], " + ex.getMessage(), ex);
			throw ex;
		} finally {
			//
			// if (session != null && !session.isClosed())
			// session.close();
		}
	}

	


	@Override
	public long countPointTotalByOPC(OPC opc) throws Exception {
		//

		long total_count = 0;

		Session session = null;

		String table = "";
		StringBuffer cql = new StringBuffer();
		//
		try {

			//
			long start = System.currentTimeMillis();

			//
			//
			session = CassandraSessionFixer.getSession();
			String keyspace = CassandraSessionFixer.getKeyspace();
			//
			cql = new StringBuffer();
			cql.append(" SELECT * FROM " + keyspace + "." + StorageConstants.TAG_POINT_COUNT_BY_OPC_TABLE + " ");
			cql.append("   WHERE " + CassandraColumns.OPC_ID + " = ? ");

			PreparedStatement statement =  PreparedStatementCacheFactory.getInstance().get(session, cql.toString(), ConsistencyLevel.ONE);

			BoundStatement query = new BoundStatement(statement);
			query.setString(0, opc.getOpc_id());
			
			ResultSetFuture rsf = session.executeAsync(query);
			ResultSet rs = rsf.getUninterruptibly();

			List<Row> list = rs.all();
			for (int i = 0; list != null && i < list.size(); i++) {
				Row row = list.get(i);
				total_count += row.getLong(CassandraColumns.COUNT);
			}

			//
			long end = System.currentTimeMillis() - start;

			//
			log.debug("Cassandra count opc point total : exec_time=[" + end + "]");
			//
			return total_count;

		} catch (Exception ex) {
			log.error("Table[" + table + "] total point count by opc error : CQL=[" + cql.toString() + "], " + ex.getMessage(), ex);
			throw ex;
		} finally {
			//
			// if (session != null && !session.isClosed())
			// session.close();
		}
	}
	
	
	@Override
	public long countPoint(Tag tag) throws Exception {
		//

		long total_count = 0;

		Session session = null;

		String table = "";
		StringBuffer cql = new StringBuffer();
		//
		try {

			//
			long start = System.currentTimeMillis();

			//
			//
			session = CassandraSessionFixer.getSession();
			String keyspace = CassandraSessionFixer.getKeyspace();
			//
			cql = new StringBuffer();
			cql.append(" SELECT * FROM " + keyspace + "." + StorageConstants.TAG_POINT_COUNT_TABLE + " ");
			cql.append(" WHERE " + CassandraColumns.TAG_ID + " = ? ");

			PreparedStatement statement =  PreparedStatementCacheFactory.getInstance().get(session, cql.toString(), ConsistencyLevel.ONE);

			BoundStatement query = new BoundStatement(statement);
			query.setString(0, tag.getTag_id());
			
			ResultSetFuture rsf = session.executeAsync(query);
			ResultSet rs = rsf.getUninterruptibly();

			List<Row> list = rs.all();
			for (int i = 0; list != null && i < list.size(); i++) {
				Row row = list.get(i);
				total_count += row.getLong(CassandraColumns.COUNT);
			}

			//
			long end = System.currentTimeMillis() - start;

			//
			log.debug("Cassandra count tag  : exec_time=[" + end + "]");
			//
			return total_count;

		} catch (Exception ex) {
			log.error("Table[" + table + "] count tag error : CQL=[" + cql.toString() + "], " + ex.getMessage(), ex);
			throw ex;
		} finally {
			//
			// if (session != null && !session.isClosed())
			// session.close();
		}
	}


	@Override
	public JSONObject selectClusterStatusLast() throws Exception {
		//

		JSONObject json = null;
		Session session = null;
		StringBuffer cql = null;
		//
		try {

			//
			long start = System.currentTimeMillis();

			//
			//
			String keyspace = CassandraSessionFixer.getKeyspace();
			session = CassandraSessionFixer.getSession();

			//
			cql = new StringBuffer();
			cql.append(" SELECT * FROM " + keyspace + "_mon.tm_monitor_cluster_status WHERE TYPE = ? " + " LIMIT 1 ");
			

			PreparedStatement statement =  PreparedStatementCacheFactory.getInstance().get(session, cql.toString(), ConsistencyLevel.ONE);
			
			BoundStatement query = new BoundStatement(statement);
			query.setString(0, "CLUSTER");
			//query.setTimestamp(1, DateUtils.isoToTimestamp(from, true));
			//query.setTimestamp(2, DateUtils.isoToTimestamp(to, true));

			ResultSetFuture rsf = session.executeAsync(query);
			ResultSet rs = rsf.getUninterruptibly();

				Row row = rs.one();
				if(row != null){
					json = new JSONObject();
				    json.put("timestamp", new Date(row.getTimestamp(CassandraColumns.TIMESTAMP).getTime()));
				    json.put("type",       row.getString(CassandraColumns.TYPE));
				    json.put("json",       row.getString(CassandraColumns.JSON));
				}

			//
			long end = System.currentTimeMillis() - start;

			//
			log.debug("Cluster status last  : exec_time=[" + end + "]");
			//
			return json;

		} catch (Exception ex) {
			log.error("Cluster status last error : CQL=[" + cql.toString() + "], " + ex.getMessage(), ex);
			throw ex;
		} finally {
		}
	}
	
	
	@Override
	public JSONObject selectClusterMetricsLast() throws Exception {
		//

		JSONObject json = null;
		Session session = null;
		StringBuffer cql = null;
		//
		try {

			//
			long start = System.currentTimeMillis();

			//
			//
			String keyspace = CassandraSessionFixer.getKeyspace();
			session = CassandraSessionFixer.getSession();

			//
			cql = new StringBuffer();
			cql.append(" SELECT * FROM " + keyspace + "_mon.tm_monitor_cluster_metrics WHERE TYPE = ? " + " LIMIT 1 ");
			

			PreparedStatement statement =  PreparedStatementCacheFactory.getInstance().get(session, cql.toString(), ConsistencyLevel.ONE);
			
			BoundStatement query = new BoundStatement(statement);
			query.setString(0, "CLUSTER");
			//query.setTimestamp(1, DateUtils.isoToTimestamp(from, true));
			//query.setTimestamp(2, DateUtils.isoToTimestamp(to, true));

			ResultSetFuture rsf = session.executeAsync(query);
			ResultSet rs = rsf.getUninterruptibly();

				Row row = rs.one();
				if(row != null){
					json = new JSONObject();
				    json.put("timestamp", new Date(row.getTimestamp(CassandraColumns.TIMESTAMP).getTime()));
				    json.put("type",       row.getString(CassandraColumns.TYPE));
				    json.put("json",       row.getString(CassandraColumns.JSON));
				}

			//
			long end = System.currentTimeMillis() - start;

			//
			log.debug("Cluster status last  : exec_time=[" + end + "]");
			//
			return json;

		} catch (Exception ex) {
			log.error("Cluster status last error : CQL=[" + cql.toString() + "], " + ex.getMessage(), ex);
			throw ex;
		} finally {
		}
	}



	
	

}
