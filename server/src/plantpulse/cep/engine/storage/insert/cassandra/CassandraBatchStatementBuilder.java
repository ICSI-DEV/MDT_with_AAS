package plantpulse.cep.engine.storage.insert.cassandra;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.json.JSONArray;

import com.datastax.driver.core.PreparedStatement;
import com.datastax.driver.core.Session;

import plantpulse.cep.engine.config.ConfigurationManager;
import plantpulse.cep.engine.storage.StorageConstants;
import plantpulse.dbutils.cassandra.SessionManager;

/**
 * ScyllaDBBatchStatementBuilder
 * @author lsb
 *
 */
public  class CassandraBatchStatementBuilder {
	
	private static Log log = LogFactory.getLog(CassandraBatchStatementBuilder.class);
	
	private static class CassandraStatementBuilderHolder {
		static CassandraBatchStatementBuilder instance = new CassandraBatchStatementBuilder();
	}

	public static CassandraBatchStatementBuilder getInstance() {
		return CassandraStatementBuilderHolder.instance;
	}
	
	private Session session = null;
	private String keyspace = null;
	
	private PreparedStatement TAG_DATA_INSERT_STATEMENT = null;
	private PreparedStatement  TAG_DATA_INSERT_RAW_STATEMENT = null;
	
	private PreparedStatement TAG_DATA_LIMIT_1_DAYS_INSERT_STATEMENT = null;
	private PreparedStatement TAG_DATA_LIMIT_7_DAYS_INSERT_STATEMENT = null;
	private PreparedStatement TAG_DATA_LIMIT_15_DAYS_INSERT_STATEMENT = null;
	private PreparedStatement TAG_DATA_LIMIT_31_DAYS_INSERT_STATEMENT = null;
	
	private PreparedStatement TAG_COUNT_UPDATE_STATEMENT = null;
	private PreparedStatement TAG_COUNT_BY_OPC_UPDATE_STATEMENT = null;
	private PreparedStatement TAG_COUNT_BY_SITE_UPDATE_STATEMENT = null;
	private PreparedStatement OPC_LAST_POINT_UPDATE_STATEMENT = null;
	private PreparedStatement SITE_SUMMARY_UPDATE_STATEMENT = null;
	
	private boolean initialized = false;
	
	public CassandraBatchStatementBuilder init() throws Exception {
		if(!initialized){
			//
			String host = ConfigurationManager.getInstance().getServer_configuration().getStorage_host();
			int port = Integer.parseInt(ConfigurationManager.getInstance().getServer_configuration().getStorage_port());
			String user = ConfigurationManager.getInstance().getServer_configuration().getStorage_user();
			String password = ConfigurationManager.getInstance().getServer_configuration().getStorage_password();
			String keyspace = ConfigurationManager.getInstance().getServer_configuration().getStorage_keyspace();
			//
			this.session = SessionManager.getSession(host, port, user, password, keyspace);
			this.keyspace = keyspace;
			buildStatement();
			initialized = true;
			
			log.info("Cassandra batch prepared statement builder initiialized.");
		}
		return this;
	}
	
	private void buildStatement() throws Exception {
		
		
		try {
			
			
			String append_columns = ConfigurationManager.getInstance().getServer_configuration().getStorage_append_columns();
			
			/* 태그 포인트  입력 */
			/* ----------------------------------------------------------------------------------------------------------------------- */
			StringBuffer cql = new StringBuffer();
			cql.append(" INSERT INTO  " + keyspace + "." + StorageConstants.TAG_POINT_TABLE + " ( ");
			cql.append(" " + CassandraColumns.TAG_ID  + ",  ");
			cql.append(" " + CassandraColumns.TIMESTAMP  + ",  ");
			cql.append(" " + CassandraColumns.VALUE      + ", ");
			cql.append(" " + CassandraColumns.TYPE       + "        ,   ");
			cql.append(" " + CassandraColumns.QUALITY    + ",  ");
			cql.append(" " + CassandraColumns.ERROR_CODE + ", ");
			//
			JSONArray columns = JSONArray.fromObject(append_columns);
			for(int i=0; columns != null && i < columns.size(); i++){
				cql.append(" " + columns.getJSONObject(i).getString("column") + ", ");
			}
			
			//
			cql.append(" " + CassandraColumns.ATTRIBUTE + " ");
			cql.append(" )  ");
			cql.append(" VALUES ");
			cql.append(" ( ");
			cql.append(" ?, ");
			cql.append(" ?, ");
			cql.append(" ?, ");
			cql.append(" ?, ");
			cql.append(" ?, ");
			cql.append(" ?, ");
			//
			for(int i=0; columns != null && i < columns.size(); i++){
				cql.append("  ? , ");
			}
			cql.append(" ?  ");
			cql.append(" ) USING TIMESTAMP ? ; ");
	
			TAG_DATA_INSERT_STATEMENT = session.prepare(cql.toString());
			
			
			/* 태그 포인트 원본  입력 */
			/* ----------------------------------------------------------------------------------------------------------------------- */
			cql = new StringBuffer();
			cql.append(" INSERT INTO  " + keyspace + "." + StorageConstants.TAG_POINT_RAW_TABLE + " ( ");
			cql.append(" " + CassandraColumns.YYYY    + ",  ");
			cql.append(" " + CassandraColumns.MM      + ",  ");
			cql.append(" " + CassandraColumns.TAG_ID  + ",  ");
			cql.append(" " + CassandraColumns.TIMESTAMP  + ",  ");
			cql.append(" " + CassandraColumns.VALUE      + ", ");
			cql.append(" " + CassandraColumns.TYPE       + "        ,   ");
			cql.append(" " + CassandraColumns.QUALITY    + ",  ");
			cql.append(" " + CassandraColumns.ERROR_CODE + ", ");
			//
			columns = JSONArray.fromObject(append_columns);
			for(int i=0; columns != null && i < columns.size(); i++){
				cql.append(" " + columns.getJSONObject(i).getString("column") + ", ");
			}
			
			//
			cql.append(" " + CassandraColumns.ATTRIBUTE + " ");
			cql.append(" )  ");
			cql.append(" VALUES ");
			cql.append(" ( ");
			cql.append(" ?, ");
			cql.append(" ?, ");
			cql.append(" ?, ");
			cql.append(" ?, ");
			cql.append(" ?, ");
			cql.append(" ?, ");
			cql.append(" ?, ");
			cql.append(" ?, ");
			//
			for(int i=0; columns != null && i < columns.size(); i++){
				cql.append("  ? , ");
			}
			cql.append(" ?  ");
			cql.append(" ) ; ");
	
			TAG_DATA_INSERT_RAW_STATEMENT  = session.prepare(cql.toString());
			

			/* 버켓 1일 데이터 */
			/* ----------------------------------------------------------------------------------------------------------------------- */
			/*
			cql = new StringBuffer();
			cql.append(" INSERT INTO  " + keyspace + "." + StorageConstants.TAG_POINT_LIMIT_1_DAYS_TABLE + " ( ");
			cql.append(" " + CassandraColumns.TAG_ID  + ",  ");
			cql.append(" " + CassandraColumns.TIMESTAMP  + ",  ");
			cql.append(" " + CassandraColumns.VALUE      + ", ");
			cql.append(" " + CassandraColumns.TYPE       + "        ,   ");
			cql.append(" " + CassandraColumns.QUALITY    + ",  ");
			cql.append(" " + CassandraColumns.ERROR_CODE + ", ");
			//
			columns = JSONArray.fromObject(append_columns);
			for(int i=0; columns != null && i < columns.size(); i++){
				cql.append(" " + columns.getJSONObject(i).getString("column") + ", ");
			}
			
			//
			cql.append(" " + CassandraColumns.ATTRIBUTE + " ");
			cql.append(" )  ");
			cql.append(" VALUES ");
			cql.append(" ( ");
			cql.append(" ?, ");
			cql.append(" ?, ");
			cql.append(" ?, ");
			cql.append(" ?, ");
			cql.append(" ?, ");
			cql.append(" ?, ");
			//
			for(int i=0; columns != null && i < columns.size(); i++){
				cql.append("  ? , ");
			}
			cql.append(" ?  ");
			cql.append(" ) ; ");
			TAG_DATA_LIMIT_1_DAYS_INSERT_STATEMENT  = session.prepare(cql.toString());
			*/
			

			/* 버켓 7일 데이터
			cql = new StringBuffer();
			cql.append(" INSERT INTO  " + keyspace + "." + StorageConstants.TAG_POINT_LIMIT_7_DAYS_TABLE + " ( ");
			cql.append(" " + CassandraColumns.TAG_ID  + ",  ");
			cql.append(" " + CassandraColumns.TIMESTAMP  + ",  ");
			cql.append(" " + CassandraColumns.VALUE      + ", ");
			cql.append(" " + CassandraColumns.TYPE       + "        ,   ");
			cql.append(" " + CassandraColumns.QUALITY    + ",  ");
			cql.append(" " + CassandraColumns.ERROR_CODE + ", ");
			//
			columns = JSONArray.fromObject(append_columns);
			for(int i=0; columns != null && i < columns.size(); i++){
				cql.append(" " + columns.getJSONObject(i).getString("column") + ", ");
			}
			
			//
			cql.append(" " + CassandraColumns.ATTRIBUTE + " ");
			cql.append(" )  ");
			cql.append(" VALUES ");
			cql.append(" ( ");
			cql.append(" ?, ");
			cql.append(" ?, ");
			cql.append(" ?, ");
			cql.append(" ?, ");
			cql.append(" ?, ");
			cql.append(" ?, ");
			//
			for(int i=0; columns != null && i < columns.size(); i++){
				cql.append("  ? , ");
			}
			cql.append(" ?  ");
			cql.append(" ) ; ");
			TAG_DATA_LIMIT_7_DAYS_INSERT_STATEMENT  = session.prepare(cql.toString());
			 */
			
			/* 버켓 15일 데이터 
			cql = new StringBuffer();
			cql.append(" INSERT INTO  " + keyspace + "." + StorageConstants.TAG_POINT_LIMIT_15_DAYS_TABLE + " ( ");
			cql.append(" " + CassandraColumns.TAG_ID  + ",  ");
			cql.append(" " + CassandraColumns.TIMESTAMP  + ",  ");
			cql.append(" " + CassandraColumns.VALUE      + ", ");
			cql.append(" " + CassandraColumns.TYPE       + "        ,   ");
			cql.append(" " + CassandraColumns.QUALITY    + ",  ");
			cql.append(" " + CassandraColumns.ERROR_CODE + ", ");
			//
			columns = JSONArray.fromObject(append_columns);
			for(int i=0; columns != null && i < columns.size(); i++){
				cql.append(" " + columns.getJSONObject(i).getString("column") + ", ");
			}
			
			//
			cql.append(" " + CassandraColumns.ATTRIBUTE + " ");
			cql.append(" )  ");
			cql.append(" VALUES ");
			cql.append(" ( ");
			cql.append(" ?, ");
			cql.append(" ?, ");
			cql.append(" ?, ");
			cql.append(" ?, ");
			cql.append(" ?, ");
			cql.append(" ?, ");
			//
			for(int i=0; columns != null && i < columns.size(); i++){
				cql.append("  ? , ");
			}
			cql.append(" ?  ");
			cql.append(" ) ; ");
			TAG_DATA_LIMIT_15_DAYS_INSERT_STATEMENT  = session.prepare(cql.toString());
			*/
			
			/* 버켓 31일 데이터 
			cql = new StringBuffer();
			cql.append(" INSERT INTO  " + keyspace + "." + StorageConstants.TAG_POINT_LIMIT_31_DAYS_TABLE + " ( ");
			cql.append(" " + CassandraColumns.TAG_ID  + ",  ");
			cql.append(" " + CassandraColumns.TIMESTAMP  + ",  ");
			cql.append(" " + CassandraColumns.VALUE      + ", ");
			cql.append(" " + CassandraColumns.TYPE       + "        ,   ");
			cql.append(" " + CassandraColumns.QUALITY    + ",  ");
			cql.append(" " + CassandraColumns.ERROR_CODE + ", ");
			//
			columns = JSONArray.fromObject(append_columns);
			for(int i=0; columns != null && i < columns.size(); i++){
				cql.append(" " + columns.getJSONObject(i).getString("column") + ", ");
			}
			
			//
			cql.append(" " + CassandraColumns.ATTRIBUTE + " ");
			cql.append(" )  ");
			cql.append(" VALUES ");
			cql.append(" ( ");
			cql.append(" ?, ");
			cql.append(" ?, ");
			cql.append(" ?, ");
			cql.append(" ?, ");
			cql.append(" ?, ");
			cql.append(" ?, ");
			//
			for(int i=0; columns != null && i < columns.size(); i++){
				cql.append("  ? , ");
			}
			cql.append(" ?  ");
			cql.append(" ) ; ");
			TAG_DATA_LIMIT_31_DAYS_INSERT_STATEMENT  = session.prepare(cql.toString());
			*/
			
			/* ----------------------------------------------------------------------------------------------------------------------- */
			
			/* 태그 카운트 업데이트 */
			StringBuffer cql_count = new StringBuffer();
			cql_count.append(" UPDATE   " + keyspace + "." + StorageConstants.TAG_POINT_COUNT_TABLE + " ");
			cql_count.append("    SET   " + CassandraColumns.COUNT + " = " + CassandraColumns.COUNT + " + ? ");
			cql_count.append("  WHERE   " + CassandraColumns.TAG_ID + " = ? ");
	
			TAG_COUNT_UPDATE_STATEMENT = session.prepare(cql_count.toString());
			//TAG_COUNT_UPDATE_STATEMENT.setConsistencyLevel(ConsistencyLevel.ONE);
			
			/* OPC별 태그 카운트 업데이트 */
			StringBuffer cql_count_by_opc = new StringBuffer();
			cql_count_by_opc.append(" UPDATE   " + keyspace + "." + StorageConstants.TAG_POINT_COUNT_BY_OPC_TABLE + " ");
			cql_count_by_opc.append("    SET   " + CassandraColumns.COUNT + " = " + CassandraColumns.COUNT + " + ? ");
			cql_count_by_opc.append("  WHERE   " + CassandraColumns.OPC_ID + " = ? ");
	
			TAG_COUNT_BY_OPC_UPDATE_STATEMENT = session.prepare(cql_count_by_opc.toString());
			//TAG_COUNT_BY_OPC_UPDATE_STATEMENT.setConsistencyLevel(ConsistencyLevel.ONE);
			
			/* 사이트별 태그 카운트 업데이트 */
			StringBuffer cql_count_by_site = new StringBuffer();
			cql_count_by_site.append(" UPDATE   " + keyspace + "." + StorageConstants.TAG_POINT_COUNT_BY_SITE_TABLE + " ");
			cql_count_by_site.append("    SET   " + CassandraColumns.COUNT + " = " + CassandraColumns.COUNT + " + ? ");
			cql_count_by_site.append("  WHERE   " + CassandraColumns.SITE_ID + " = ? ");
	
			TAG_COUNT_BY_SITE_UPDATE_STATEMENT = session.prepare(cql_count_by_site.toString());
			//TAG_COUNT_BY_SITE_UPDATE_STATEMENT.setConsistencyLevel(ConsistencyLevel.ONE);
			
			/* OPC 포인트 마지막 업데이트 */
			StringBuffer cql_opc_last_point_update = new StringBuffer();
			cql_opc_last_point_update.append(" UPDATE   " + keyspace + "." + StorageConstants.OPC_POINT_LAST_UPDATE_TABLE + " ");
			cql_opc_last_point_update.append("    SET   " + CassandraColumns.LAST_UPDATE + " = ? ");
			cql_opc_last_point_update.append("  WHERE   " + CassandraColumns.OPC_ID + " = ? ");
	
			OPC_LAST_POINT_UPDATE_STATEMENT = session.prepare(cql_opc_last_point_update.toString());
			//OPC_LAST_POINT_UPDATE_STATEMENT.setConsistencyLevel(ConsistencyLevel.ONE);
			
			
			/* ----------------------------------------------------------------------------------------------------------------------- */
			
			/* 사이트 요약 카운트 업데이트 */
			StringBuffer cql_site_summary = new StringBuffer();
			cql_site_summary.append(" UPDATE   " + keyspace + "." + StorageConstants.SITE_SUMMARY_TABLE + " ");
			cql_site_summary.append("    SET   " + CassandraColumns.DATA_COUNT + " = " + CassandraColumns.DATA_COUNT + " + ? ");
			cql_site_summary.append("  WHERE   " + CassandraColumns.SITE_ID + " = ? ");
			cql_site_summary.append("    AND   " + CassandraColumns.YEAR + " = ? ");
			cql_site_summary.append("    AND   " + CassandraColumns.MONTH + " = ? ");
			cql_site_summary.append("    AND   " + CassandraColumns.DAY + " = ? ");
			cql_site_summary.append("    AND   " + CassandraColumns.HOUR + " = ? ");
			cql_site_summary.append("    AND   " + CassandraColumns.MINUTE + " = ? ");
	
			SITE_SUMMARY_UPDATE_STATEMENT = session.prepare(cql_site_summary.toString());
			//SITE_SUMMARY_UPDATE_STATEMENT.setConsistencyLevel(ConsistencyLevel.ONE);
			
			log.info("Cassandra batch prepared statement build completed.");
		
		}catch(Exception ex){
			log.error("Cassandra batch prepared statement build error : " + ex.getMessage(), ex);
			throw ex;
		}
	}

	public PreparedStatement getTAG_DATA_INSERT_STATEMENT() throws Exception {
		return TAG_DATA_INSERT_STATEMENT;
	}
	
	public PreparedStatement getTAG_DATA_INSERT_RAW_STATEMENT() throws Exception {
		return TAG_DATA_INSERT_RAW_STATEMENT;
	}
	
	public PreparedStatement getTAG_COUNT_UPDATE_STATEMENT() throws Exception {
		return TAG_COUNT_UPDATE_STATEMENT;
	}

	public PreparedStatement getTAG_COUNT_BY_OPC_UPDATE_STATEMENT()  throws Exception{
		return TAG_COUNT_BY_OPC_UPDATE_STATEMENT;
	}

	public PreparedStatement getTAG_COUNT_BY_SITE_UPDATE_STATEMENT()  throws Exception{
		return TAG_COUNT_BY_SITE_UPDATE_STATEMENT;
	}

	public PreparedStatement getOPC_LAST_POINT_UPDATE_STATEMENT() {
		return OPC_LAST_POINT_UPDATE_STATEMENT;
	}

	public PreparedStatement getSITE_SUMMARY_UPDATE_STATEMENT()  throws Exception {
		return SITE_SUMMARY_UPDATE_STATEMENT;
	}
	
	public PreparedStatement getTAG_DATA_LIMIT_1_DAYS_INSERT_STATEMENT() {
		return TAG_DATA_LIMIT_1_DAYS_INSERT_STATEMENT;
	}
	public PreparedStatement getTAG_DATA_LIMIT_7_DAYS_INSERT_STATEMENT() {
		return TAG_DATA_LIMIT_7_DAYS_INSERT_STATEMENT;
	}

	public PreparedStatement getTAG_DATA_LIMIT_15_DAYS_INSERT_STATEMENT() {
		return TAG_DATA_LIMIT_15_DAYS_INSERT_STATEMENT;
	}

	public PreparedStatement getTAG_DATA_LIMIT_31_DAYS_INSERT_STATEMENT() {
		return TAG_DATA_LIMIT_31_DAYS_INSERT_STATEMENT;
	}

	public boolean isInitialized() {
		return initialized;
	}

	
}
