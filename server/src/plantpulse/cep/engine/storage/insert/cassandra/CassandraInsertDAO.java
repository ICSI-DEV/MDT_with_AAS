package plantpulse.cep.engine.storage.insert.cassandra;

import java.nio.ByteBuffer;
import java.sql.Timestamp;
import java.text.SimpleDateFormat;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.util.Date;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.UUID;
import java.util.concurrent.Executors;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;

import org.apache.commons.lang.StringEscapeUtils;
import org.apache.commons.lang.StringUtils;
import org.apache.commons.lang.math.NumberUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.json.JSONArray;
import org.json.JSONObject;
import org.realdisplay.framework.util.DateUtils;

import com.datastax.driver.core.BatchStatement;
import com.datastax.driver.core.BatchStatement.Type;
import com.datastax.driver.core.BoundStatement;
import com.datastax.driver.core.ConsistencyLevel;
import com.datastax.driver.core.ExecutionInfo;
import com.datastax.driver.core.Host;
import com.datastax.driver.core.PreparedStatement;
import com.datastax.driver.core.QueryTrace;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.ResultSetFuture;
import com.datastax.driver.core.Session;
import com.datastax.driver.core.TypeTokens;
import com.datastax.driver.core.exceptions.NoHostAvailableException;
import com.datastax.driver.core.exceptions.QueryExecutionException;
import com.datastax.driver.core.exceptions.QueryValidationException;
import com.datastax.driver.core.utils.UUIDs;
import com.google.common.reflect.TypeToken;
import com.google.common.util.concurrent.FutureCallback;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListeningExecutorService;
import com.google.common.util.concurrent.MoreExecutors;

import plantpulse.cep.domain.AssetStatement;
import plantpulse.cep.domain.BLOB;
import plantpulse.cep.domain.Trigger;
import plantpulse.cep.domain.TriggerAttribute;
import plantpulse.cep.engine.CEPEngineManager;
import plantpulse.cep.engine.monitoring.statistics.DataFlowErrorCount;
import plantpulse.cep.engine.properties.PropertiesLoader;
import plantpulse.cep.engine.storage.DAOHelper;
import plantpulse.cep.engine.storage.StorageConstants;
import plantpulse.cep.engine.storage.StorageTTL;
import plantpulse.cep.engine.storage.insert.StorageInsertDAO;
import plantpulse.cep.engine.storage.session.CassandraSessionFixer;
import plantpulse.cep.engine.storage.statement.PreparedStatementCacheFactory;
import plantpulse.cep.engine.storage.utils.StorageUtils;
import plantpulse.cep.engine.utils.AliasUtils;
import plantpulse.cep.engine.utils.ObjectCreationChecker;
import plantpulse.cep.engine.utils.TimestampUtils;
import plantpulse.domain.AlarmConfig;
import plantpulse.domain.Asset;
import plantpulse.domain.MetaModel;
import plantpulse.domain.Metadata;
import plantpulse.domain.OPC;
import plantpulse.domain.Security;
import plantpulse.domain.Site;
import plantpulse.domain.Tag;
import plantpulse.domain.User;
import plantpulse.event.opc.Point;

/**
 * 
 * CassandraInsertDAO
 * 
 * 
 * @author lsb
 * 
 */
public class CassandraInsertDAO extends DAOHelper implements StorageInsertDAO {
	
	

	private static final Log log = LogFactory.getLog(CassandraInsertDAO.class);

	private static final int  ZSTD_COMPRESSION_LEVEL = Integer.parseInt(PropertiesLoader.getStorage_properties().getProperty("storage.compresion.zstd.level", "3")) ; //최고 수준의 압축률
	
	
	/**
	 * 키스페이스 생성
	 * 
	 * @param site
	 * @throws Exception
	 */
	@Override
	public void createKeyspace() throws Exception {
		//

		Session session = null;
		String keyspace = "";
		String replication = "";
		//
		try {

			//
			session     = CassandraSessionFixer.getSession();
			keyspace    = CassandraSessionFixer.getKeyspace();
			replication = CassandraSessionFixer.getReplication();
			//
			
			//
			if (!ObjectCreationChecker.getInstance().hasObject(keyspace)) {
				//
				StringBuffer createKS = new StringBuffer();
				createKS.append(" CREATE KEYSPACE IF NOT EXISTS " + keyspace + " ");
				createKS.append(" WITH REPLICATION = " + replication + "  AND DURABLE_WRITES = false; ");
				session.execute(createKS.toString());
				//
				log.info("Keyspace [" + keyspace + "] is created, With replication = [" + replication + "]");
				
				session.execute("USE "+  keyspace + ";");
			};

			//
		} catch (Exception ex) {
			log.error("[" + keyspace + "] create error : " + ex.getMessage(), ex);
			throw ex;
		} finally {
			//
			// if (session != null && !session.isClosed())
			// session.close();
		}
	}

	public void createFunctionAndAggregate() throws Exception {

		//

		Session session = null;
		String keyspace = "";
		//
		try {

			session     = CassandraSessionFixer.getSession();
			keyspace    = CassandraSessionFixer.getKeyspace();
			
		

			StringBuffer createFA = new StringBuffer();
			//형변환 함수 추가
			//TO_INT
			createFA = new StringBuffer();
			createFA.append("  CREATE OR REPLACE FUNCTION " + keyspace + "." +  "TO_INT(input text) RETURNS NULL ON NULL INPUT RETURNS int LANGUAGE java AS ' ");
			createFA.append("     return (input == null || input.equals(\"\")) ? null : Integer.parseInt(input); ");
			createFA.append(" '; ");
			session.execute(createFA.toString());

			//TO_DOUBLE
			createFA = new StringBuffer();
			createFA.append("  CREATE OR REPLACE FUNCTION " + keyspace + "." +  "TO_DOUBLE(input text) RETURNS NULL ON NULL INPUT RETURNS double LANGUAGE java AS ' ");
			createFA.append("     return (input == null || input.equals(\"\")) ? null : Double.parseDouble(input); ");
			createFA.append(" '; ");
			session.execute(createFA.toString());
			
			//TO_FLOAT
			createFA = new StringBuffer();
			createFA.append("  CREATE OR REPLACE FUNCTION " + keyspace + "." +  "TO_FLOAT(input text) RETURNS NULL ON NULL INPUT RETURNS float LANGUAGE java AS ' ");
			createFA.append("     return (input == null || input.equals(\"\")) ? null : Float.parseFloat(input); ");
			createFA.append(" '; ");
			session.execute(createFA.toString());
			
			//TO_BOOLEAN
			createFA = new StringBuffer();
			createFA.append("  CREATE OR REPLACE FUNCTION " + keyspace + "." +  "TO_BOOLEAN(input text) RETURNS NULL ON NULL INPUT RETURNS boolean LANGUAGE java AS ' ");
			createFA.append("     return (input == null || input.equals(\"\")) ? null : Boolean.parseBoolean(input); ");
			createFA.append(" '; ");
			session.execute(createFA.toString());
			
			
			//BUCKET
			/*
			createFA = new StringBuffer();
			createFA.append("  CREATE OR REPLACE FUNCTION " + keyspace + "." +  "BUCKET(input bigint) RETURNS NULL ON NULL INPUT RETURNS timestamp LANGUAGE java AS ' ");
			createFA.append("     return  new java.util.Date((input - (Math.abs(input) % 1814400000L))); ");
			createFA.append(" '; ");
			session.execute(createFA.toString());
			
			
			//LAST_BUCKET
			createFA = new StringBuffer();
			createFA.append("  CREATE OR REPLACE FUNCTION " + keyspace + "." +  "BUCKET_LAST() RETURNS NULL ON NULL INPUT RETURNS timestamp LANGUAGE java AS ' ");
			createFA.append("     return new java.util.Date((System.currentTimeMillis() - (Math.abs(System.currentTimeMillis()) % 1814400000L))); ");
			createFA.append(" '; ");
			session.execute(createFA.toString());
			
			
			//BUCKET_IN
			createFA = new StringBuffer();
			createFA.append("  CREATE OR REPLACE FUNCTION " + keyspace + "." +  "BUCKET_IN(str_date text, end_date text) RETURNS NULL ON NULL INPUT RETURNS list<timestamp> LANGUAGE java AS $$ ");
			createFA.append("  List<java.util.Date> buckets = new ArrayList<java.util.Date>(); ");
			createFA.append("  try { ");
			createFA.append("  java.text.DateFormat formatter = new java.text.SimpleDateFormat(\"yyyy-MM-dd HH:mm:ss\"); ");
			createFA.append("  java.text.DateFormat full_formatter = new java.text.SimpleDateFormat(\"yyyy-MM-dd HH:mm:ss.SSS\"); ");
			createFA.append("  Date startDate = null; ");
			createFA.append("  Date endDate  = null; ");
			createFA.append("  if(str_date.length() == 19) { ");
			createFA.append("  	startDate = (Date) formatter.parse(str_date); ");
			createFA.append("  	endDate = (Date) formatter.parse(end_date); ");
			createFA.append("  }else if(str_date.length() == 23) { ");
			createFA.append("  	startDate = (Date) full_formatter.parse(str_date); ");
			createFA.append("  	endDate   = (Date) full_formatter.parse(end_date); ");
			createFA.append("  }else { ");
			createFA.append("  	throw new Exception(\"Not a date format : \" +  str_date + \" ~ \" + end_date); ");
			createFA.append("  }; ");
			createFA.append("  long interval = 24 * 1000 * 60 * 60;  ");
			createFA.append("  long endTime = endDate.getTime();  ");
			createFA.append("  long curTime = startDate.getTime(); ");
			createFA.append("  while (curTime <= endTime) { ");
			createFA.append("  	Date curdate = new Date(curTime); ");
			createFA.append("  	java.util.Date bucket = new java.util.Date((curdate.getTime() - (Math.abs(curdate.getTime()) % 1814400000L))); ");
			createFA.append("  	if (!buckets.contains(bucket)) { ");
			createFA.append("  		buckets.add(bucket); ");
			createFA.append("  	} ");
			createFA.append("  	curTime += interval; ");
			createFA.append("  }; ");;
			createFA.append("  }catch(java.lang.Exception ex) { ");
			createFA.append("   ex.printStackTrace(); ");
			createFA.append("  } ");
			createFA.append(" return buckets; ");
			createFA.append(" $$; ");
			session.execute(createFA.toString());
			*/
			 
	
			//시계열 함수 
			//TIMESTAMP_MINUTES_AGO
			createFA = new StringBuffer();
			createFA.append("  CREATE OR REPLACE FUNCTION " + keyspace  + ".TIMESTAMP_MINUTES_AGO( arg int ) ");
			createFA.append("    RETURNS NULL ON NULL INPUT  ");
			createFA.append("    RETURNS timestamp  ");
			createFA.append("    LANGUAGE java  ");
			createFA.append("    AS $$  ");
			createFA.append("    return new java.util.Date(System.currentTimeMillis() - arg * 60 * 1000); $$; ");
			session.execute(createFA.toString()); 
			
			//TIMESTAMP_HOURS_AGO
			createFA = new StringBuffer();
			createFA.append("  CREATE OR REPLACE FUNCTION " + keyspace  + ".TIMESTAMP_HOURS_AGO( arg int ) ");
			createFA.append("    RETURNS NULL ON NULL INPUT  ");
			createFA.append("    RETURNS timestamp  ");
			createFA.append("    LANGUAGE java  ");
			createFA.append("    AS $$  ");
			createFA.append("    return new java.util.Date(System.currentTimeMillis() - arg * 60 * 60 * 1000); $$; ");
			session.execute(createFA.toString()); 
			
			//TIMESTAMP_DAYS_AGO
			createFA = new StringBuffer();
			createFA.append("  CREATE OR REPLACE FUNCTION " + keyspace  + ".TIMESTAMP_DAYS_AGO( arg int ) ");
			createFA.append("    RETURNS NULL ON NULL INPUT  ");
			createFA.append("    RETURNS timestamp  ");
			createFA.append("    LANGUAGE java  ");
			createFA.append("    AS $$  ");
			createFA.append("    return new java.util.Date(System.currentTimeMillis() - arg * 24 * 60 * 60 * 1000); $$; ");
			session.execute(createFA.toString()); 
			
			//TIMESTAMP_NOW
			createFA = new StringBuffer();
			createFA.append("  CREATE OR REPLACE FUNCTION " + keyspace + "." +  "TIMESTAMP_NOW() RETURNS NULL ON NULL INPUT RETURNS timestamp LANGUAGE java AS $$  ");
			createFA.append("     return new java.util.Date(System.currentTimeMillis()); ");
			createFA.append("  $$ ; ");
			session.execute(createFA.toString());
			

			
			//콜렉션(Map 및 List) 함수 
			//LOOKUP
			createFA = new StringBuffer();
			createFA.append("  CREATE OR REPLACE FUNCTION " + keyspace + "." +  "LOOKUP(k text, m map<text, text>) ");
			createFA.append("    RETURNS NULL ON NULL INPUT  ");
			createFA.append("    RETURNS TEXT  "); 
			createFA.append("    LANGUAGE java  ");
			createFA.append("    AS ' return (String) m.get(k);  ';  ");
			session.execute(createFA.toString());
			
			//MAP_VALUE
			createFA = new StringBuffer();
			createFA.append("  CREATE OR REPLACE FUNCTION " + keyspace + "." +  "MAP_VALUE(k text, m map<text, text>) ");
			createFA.append("    RETURNS NULL ON NULL INPUT  ");
			createFA.append("    RETURNS TEXT  "); 
			createFA.append("    LANGUAGE java  ");
			createFA.append("    AS ' return (String) m.get(k);  ';  ");
			session.execute(createFA.toString());
			
			Thread.sleep(300);
			
			
	        //집계 합수 추가
			//STDDEV
			createFA = new StringBuffer();
			createFA.append("  CREATE OR REPLACE FUNCTION " + keyspace + "." +  "STDDEV_STATE ( state tuple<int,double,double>, val double ) CALLED ON NULL INPUT RETURNS tuple<int,double,double> LANGUAGE java AS ");
			createFA.append(
					"  'int n = state.getInt(0); double mean = state.getDouble(1); double m2 = state.getDouble(2); n++; double delta = val - mean; mean += delta / n; m2 += delta * (val - mean); state.setInt(0, n); state.setDouble(1, mean); state.setDouble(2, m2); return state;'; ");
			session.execute(createFA.toString());
			Thread.sleep(300);

			createFA = new StringBuffer();
			createFA.append("  CREATE OR REPLACE FUNCTION " + keyspace + "." +  "STDDEV_FINAL ( state tuple<int,double,double> ) CALLED ON NULL INPUT RETURNS double LANGUAGE java AS  ");
			createFA.append("  'int n = state.getInt(0); double m2 = state.getDouble(2); if (n < 1) { return null; } return Math.sqrt(m2 / (n - 1));'; ");
			session.execute(createFA.toString());
			Thread.sleep(300);

			createFA = new StringBuffer();
			createFA.append("  CREATE OR REPLACE AGGREGATE  " + keyspace + "." +  "STDDEV ( double )  ");
			createFA.append("  SFUNC stddev_state STYPE tuple<int,double,double> FINALFUNC stddev_final INITCOND (0,0,0); ");
			session.execute(createFA.toString());
			
			Thread.sleep(300);
			
		

			log.info("Cassandra Function and Aggregate is created.");

			//
		} catch (Exception ex) {
			log.error("[" + keyspace + "] function and aggregate create error : " + ex.getMessage(), ex);
			throw ex;
		} finally {
			//
			// if (session != null && !session.isClosed())
			// session.close();
		}

	}
	
	
	@Override
	public void createTableForMetadata() throws Exception {

		Session session = null;
		//
		try {

			 session     = CassandraSessionFixer.getSession();
			 String keyspace    = CassandraSessionFixer.getKeyspace();
			
			// SASI 인덱스 옵션
			 /*
			StringBuffer sasi_option = new StringBuffer();
			sasi_option.append(" USING 'org.apache.cassandra.index.sasi.SASIIndex' ");
			sasi_option.append("     WITH OPTIONS = { ");
			sasi_option.append(" 	'mode': 'CONTAINS', ");
			sasi_option.append(" 	'analyzer_class': 'org.apache.cassandra.index.sasi.analyzer.StandardAnalyzer', ");
			sasi_option.append(" 	'analyzed': 'true', ");
			sasi_option.append(" 	'tokenization_skip_stop_words': 'and, the, or', ");
			sasi_option.append(" 	'tokenization_enable_stemming': 'true', ");
			sasi_option.append(" 	'tokenization_normalize_lowercase': 'true', ");
			sasi_option.append(" 	'tokenization_locale': 'ko' ");
			sasi_option.append(" } ");
			*/
			StringBuffer sasi_option = new StringBuffer();
			sasi_option.append(" USING 'org.apache.cassandra.index.sasi.SASIIndex' ");
			sasi_option.append("     WITH OPTIONS = { ");
			sasi_option.append(" 	'mode': 'CONTAINS',");
			sasi_option.append(" 	'analyzer_class': 'org.apache.cassandra.index.sasi.analyzer.NonTokenizingAnalyzer',");
			sasi_option.append(" 	'case_sensitive': 'false' ");
			sasi_option.append(" } ");
			
			//메타스토어 동기화 테이블 생성
			
			StringBuffer cql = new StringBuffer();
	
			//MM_SITE
			cql = new StringBuffer();
			cql.append(" CREATE TABLE IF NOT EXISTS " + keyspace + "." + StorageConstants.SITE_TABLE + " ( ");
			cql.append("		  " + CassandraColumns.SITE_ID   + "     varchar,  ");
			cql.append("		  " + CassandraColumns.SITE_NAME + "     varchar,  ");
			cql.append("		  " + CassandraColumns.DESCRIPTION + "   varchar,  ");
			cql.append("		  " + CassandraColumns.LAT + "     varchar,  ");
			cql.append("		  " + CassandraColumns.LNG + "     varchar,  ");
			cql.append("		  " + CassandraColumns.INSERT_DATE + "     timestamp,  ");
			cql.append("		  " + CassandraColumns.UPDATE_DATE + "     timestamp,  ");
			cql.append("		  PRIMARY KEY (" + CassandraColumns.SITE_ID + ")  ");
			cql.append("   )  ");
			cql.append("   WITH  caching= {'keys':'ALL', 'rows_per_partition':'ALL'} ");
			cql.append("     AND compression = { 'class' : 'ZstdCompressor' } ");
			cql.append("     AND gc_grace_seconds = 3660  ");
			
			session.execute(cql.toString());
			
			session.execute(" CREATE CUSTOM INDEX IF NOT EXISTS " + StorageConstants.SITE_TABLE  + "_T_IDX_SITE_NAME on " + keyspace + "." + StorageConstants.SITE_TABLE  + " (" + CassandraColumns.SITE_NAME + ") " + sasi_option.toString());
			
			log.info("Cassandra table [" + keyspace + "." + StorageConstants.SITE_TABLE + "] is created.");
	
			//MM_OPC
			cql = new StringBuffer();
			cql.append(" CREATE TABLE IF NOT EXISTS " + keyspace + "." + StorageConstants.OPC_TABLE + " ( ");
			cql.append("		  " + CassandraColumns.OPC_ID   + "     varchar,  ");
			cql.append("		  " + CassandraColumns.SITE_ID   + "     varchar,  ");
			cql.append("		  " + CassandraColumns.OPC_NAME + "     varchar,  ");
			cql.append("		  " + CassandraColumns.OPC_TYPE + "     varchar,  ");
			cql.append("		  " + CassandraColumns.OPC_SERVER_IP + "     varchar,  ");
			cql.append("		  " + CassandraColumns.DESCRIPTION + "   varchar,  ");
			cql.append("		  " + CassandraColumns.INSERT_DATE + "     timestamp,  ");
			cql.append("		  " + CassandraColumns.UPDATE_DATE + "     timestamp,  ");
			cql.append("		  PRIMARY KEY (" + CassandraColumns.OPC_ID + ")  ");
			cql.append("   )  ");
			cql.append("   WITH  caching= {'keys':'ALL', 'rows_per_partition':'ALL'} ");
			cql.append("     AND compression = { 'class' : 'ZstdCompressor' } ");
			cql.append("     AND gc_grace_seconds = 3660  ");
			session.execute(cql.toString());
			
			session.execute(" CREATE CUSTOM INDEX IF NOT EXISTS " + StorageConstants.OPC_TABLE  + "_T_IDX_OPC_NAME      on " + keyspace + "." + StorageConstants.OPC_TABLE  + " (" + CassandraColumns.OPC_NAME + ") " + sasi_option.toString());
			session.execute(" CREATE CUSTOM INDEX IF NOT EXISTS " + StorageConstants.OPC_TABLE  + "_T_IDX_OPC_SERVER_IP on " + keyspace + "." + StorageConstants.OPC_TABLE  + " (" + CassandraColumns.OPC_SERVER_IP + ") " + sasi_option.toString());
			
			log.info("Cassandra table [" + keyspace + "." + StorageConstants.OPC_TABLE + "] is created.");
			
			
			//TAG_TABLE
			cql = new StringBuffer();
			cql.append(" CREATE TABLE IF NOT EXISTS " + keyspace + "." + StorageConstants.TAG_TABLE + " ( ");
			cql.append("		  " + CassandraColumns.TAG_ID   + "     varchar,  ");
			cql.append("		  " + CassandraColumns.OPC_ID   + "     varchar,  ");
			cql.append("		  " + CassandraColumns.SITE_ID   + "    varchar,  ");
			cql.append("		  " + CassandraColumns.TAG_NAME + "     varchar,  ");
			cql.append("		  " + CassandraColumns.TAG_SOURCE + "     varchar,  ");
			cql.append("		  " + CassandraColumns.JAVA_TYPE + "     varchar,  ");
			cql.append("		  " + CassandraColumns.ALIAS_NAME + "     varchar,  ");
			cql.append("		  " + CassandraColumns.IMPORTANCE + "     int,  ");
			cql.append("		  " + CassandraColumns.INTERVAL + "     varchar,  ");
			cql.append("		  " + CassandraColumns.UNIT + "      varchar,  ");
			cql.append("		  " + CassandraColumns.TRIP_HI + "   varchar,  ");
			cql.append("		  " + CassandraColumns.HI + "        varchar,  ");
			cql.append("		  " + CassandraColumns.HI_HI + "     varchar,  ");
			cql.append("		  " + CassandraColumns.LO + "        varchar,  ");
			cql.append("		  " + CassandraColumns.LO_LO + "     varchar,  ");
			cql.append("		  " + CassandraColumns.TRIP_LO + "     varchar,  ");
			cql.append("		  " + CassandraColumns.MIN_VALUE + "     varchar,  ");
			cql.append("		  " + CassandraColumns.MAX_VALUE + "     varchar,  ");
			cql.append("		  " + CassandraColumns.DISPLAY_FORMAT + "     varchar,  ");
			cql.append("		  " + CassandraColumns.LAT + "     varchar,  ");
			cql.append("		  " + CassandraColumns.LNG + "     varchar,  ");
			cql.append("		  " + CassandraColumns.LINKED_ASSET_ID + "     varchar,  ");
			cql.append("		  " + CassandraColumns.DESCRIPTION + "   varchar,  ");
			cql.append("		  " + CassandraColumns.INSERT_DATE + "     timestamp,  ");
			cql.append("		  " + CassandraColumns.UPDATE_DATE + "     timestamp,  ");
			cql.append("		  PRIMARY KEY (" + CassandraColumns.TAG_ID + ")  ");
			cql.append("   )  ");
			cql.append("   WITH  caching= {'keys':'ALL', 'rows_per_partition':'ALL'} ");
			cql.append("     AND compression = { 'class' : 'ZstdCompressor' } ");
			cql.append("     AND gc_grace_seconds = 3660  ");
			session.execute(cql.toString());
			
			
			session.execute(" CREATE CUSTOM INDEX IF NOT EXISTS " + StorageConstants.TAG_TABLE  + "_T_IDX_TAG_NAME          on " + keyspace + "." + StorageConstants.TAG_TABLE  + " (" + CassandraColumns.TAG_NAME + ") " + sasi_option.toString());
			session.execute(" CREATE CUSTOM INDEX IF NOT EXISTS " + StorageConstants.TAG_TABLE  + "_T_IDX_TAG_SOURCE        on " + keyspace + "." + StorageConstants.TAG_TABLE  + " (" + CassandraColumns.TAG_SOURCE + ") " + sasi_option.toString());
			session.execute(" CREATE CUSTOM INDEX IF NOT EXISTS " + StorageConstants.TAG_TABLE  + "_T_IDX_JAVA_TYPE         on " + keyspace + "." + StorageConstants.TAG_TABLE  + " (" + CassandraColumns.JAVA_TYPE + ") " + sasi_option.toString());
			session.execute(" CREATE CUSTOM INDEX IF NOT EXISTS " + StorageConstants.TAG_TABLE  + "_T_IDX_LINKED_ASSET_ID   on " + keyspace + "." + StorageConstants.TAG_TABLE  + " (" + CassandraColumns.LINKED_ASSET_ID + ") " + sasi_option.toString());
			session.execute(" CREATE CUSTOM INDEX IF NOT EXISTS " + StorageConstants.TAG_TABLE  + "_T_IDX_ALIAS_NAME        on " + keyspace + "." + StorageConstants.TAG_TABLE  + " (" + CassandraColumns.ALIAS_NAME + ") " + sasi_option.toString());
			session.execute(" CREATE CUSTOM INDEX IF NOT EXISTS " + StorageConstants.TAG_TABLE  + "_T_IDX_DESCRIPTION       on " + keyspace + "." + StorageConstants.TAG_TABLE  + " (" + CassandraColumns.DESCRIPTION + ") " + sasi_option.toString());
			
			
			log.info("Cassandra table [" + keyspace + "." + StorageConstants.TAG_TABLE + "] is created.");
			
			
			//ALARM_TABLE			
			cql = new StringBuffer();
			cql.append(" CREATE TABLE IF NOT EXISTS " + keyspace + "." + StorageConstants.ALARM_TABLE + " ( ");
			cql.append("		  " + CassandraColumns.ALARM_CONFIG_ID   + "      varchar,  ");
			cql.append("		  " + CassandraColumns.TAG_ID   + "               varchar,  ");
			cql.append("		  " + CassandraColumns.ALARM_CONFIG_NAME + "      varchar,  ");
			cql.append("		  " + CassandraColumns.ALARM_CONFIG_PRIORITY + "  varchar,  ");
			cql.append("		  " + CassandraColumns.ALARM_CONFIG_DESC + "      varchar,  ");
			cql.append("		  " + CassandraColumns.ALARM_TYPE + "     varchar,  ");
			cql.append("		  " + CassandraColumns.EPL + "            varchar,  ");
			cql.append("		  " + CassandraColumns.CONDITION + "     varchar,  ");
			cql.append("		  " + CassandraColumns.MESSAGE   + "     varchar,  ");
			cql.append("		  " + CassandraColumns.RECIEVE_ME + "    boolean,  ");
			cql.append("		  " + CassandraColumns.RECIEVE_OTHERS + " varchar,  ");
			cql.append("		  " + CassandraColumns.SEND_EMAIL + "     boolean,  ");
			cql.append("		  " + CassandraColumns.SEND_SMS + "       boolean,  ");
			cql.append("		  " + CassandraColumns.DUPLICATE_CHECK + "   boolean,  ");
			cql.append("		  " + CassandraColumns.DUPLICATE_CHECK_TIME + "   int,  ");
			cql.append("		  " + CassandraColumns.INSERT_DATE + "     timestamp,  ");
			cql.append("		  " + CassandraColumns.UPDATE_DATE + "     timestamp,  ");
			cql.append("		  PRIMARY KEY (" + CassandraColumns.TAG_ID + ")  ");
			cql.append("   )  ");
			cql.append("   WITH  caching= {'keys':'ALL', 'rows_per_partition':'ALL'} ");
			cql.append("     AND compression = { 'class' : 'ZstdCompressor' } ");
			cql.append("     AND gc_grace_seconds = 3660  ");
			session.execute(cql.toString());
			
			session.execute(" CREATE CUSTOM INDEX IF NOT EXISTS " + StorageConstants.ALARM_TABLE  + "_T_IDX_ALARM_CONFIG_NAME on " + keyspace + "." + StorageConstants.ALARM_TABLE  + " (" + CassandraColumns.ALARM_CONFIG_NAME + ") " + sasi_option.toString());
			
			log.info("Cassandra table [" + keyspace + "." + StorageConstants.ALARM_TABLE + "] is created.");

			
			//ASSET_TABLE			
			cql = new StringBuffer();
			cql.append(" CREATE TABLE IF NOT EXISTS " + keyspace + "." + StorageConstants.ASSET_TABLE + " ( ");
			cql.append("		  " + CassandraColumns.ASSET_ID   + "     varchar,  ");
			cql.append("		  " + CassandraColumns.PARENT_ASSET_ID   + "  varchar,  ");
			cql.append("		  " + CassandraColumns.SITE_ID   + "       varchar,  ");
			cql.append("		  " + CassandraColumns.ASSET_NAME + "      varchar,  ");
			cql.append("		  " + CassandraColumns.ASSET_TYPE + "      varchar,  ");
			cql.append("		  " + CassandraColumns.ASSET_ORDER + "     int,  ");
			cql.append("		  " + CassandraColumns.ASSET_SVG_IMG  + "  varchar,  ");
			cql.append("		  " + CassandraColumns.TABLE_TYPE     + "  varchar,  ");
			cql.append("		  " + CassandraColumns.DESCRIPTION  + "    varchar,  ");
			cql.append("		  " + CassandraColumns.INSERT_DATE + "     timestamp,  ");
			cql.append("		  " + CassandraColumns.UPDATE_DATE + "     timestamp,  ");
			cql.append("		  PRIMARY KEY (" + CassandraColumns.ASSET_ID + ")  ");
			cql.append("   )  ");
			cql.append("   WITH  caching= {'keys':'ALL', 'rows_per_partition':'ALL'} ");
			cql.append("     AND compression = { 'class' : 'ZstdCompressor' } ");
			cql.append("     AND gc_grace_seconds = 3660  ");
			session.execute(cql.toString());
			
			session.execute(" CREATE CUSTOM INDEX IF NOT EXISTS " + StorageConstants.ASSET_TABLE  + "_T_IDX_ASSET_NAME on " + keyspace + "." + StorageConstants.ASSET_TABLE  + " (" + CassandraColumns.ASSET_NAME + ") " + sasi_option.toString());
			
			log.info("Cassandra table [" + keyspace + "." + StorageConstants.ASSET_TABLE + "] is created.");

			//USER_TABLE			
			cql = new StringBuffer();
			cql.append(" CREATE TABLE IF NOT EXISTS " + keyspace + "." + StorageConstants.USER_TABLE + " ( ");
			cql.append("		  " + CassandraColumns.USER_ID   + "     varchar,  ");
			cql.append("		  " + CassandraColumns.SECURITY_ID   + "     varchar,  ");
			cql.append("		  " + CassandraColumns.PASSWORD   + "     varchar,  ");
			cql.append("		  " + CassandraColumns.ROLE   + "       varchar,  ");
			cql.append("		  " + CassandraColumns.NAME + "      varchar,  ");
			cql.append("		  " + CassandraColumns.EMAIL + "      varchar,  ");
			cql.append("		  " + CassandraColumns.PHONE + "     varchar,  ");
			cql.append("		  " + CassandraColumns.INSERT_DATE + "     timestamp,  ");
			cql.append("		  " + CassandraColumns.UPDATE_DATE + "     timestamp,  ");
			cql.append("		  PRIMARY KEY (" + CassandraColumns.USER_ID + ")  ");
			cql.append("   )  ");
			cql.append("   WITH  caching= {'keys':'ALL', 'rows_per_partition':'ALL'} ");
			cql.append("     AND compression = { 'class' : 'ZstdCompressor' } ");
			cql.append("     AND gc_grace_seconds = 3660  ");
			session.execute(cql.toString());
			
			session.execute(" CREATE CUSTOM INDEX IF NOT EXISTS " + StorageConstants.USER_TABLE  + "_T_IDX_NAME on " + keyspace + "." + StorageConstants.USER_TABLE  + " (" + CassandraColumns.NAME + ") " + sasi_option.toString());
			
			
			log.info("Cassandra table [" + keyspace + "." + StorageConstants.USER_TABLE + "] is created.");
			
			
			//SECURITY_TABLE			
			cql = new StringBuffer();
			cql.append(" CREATE TABLE IF NOT EXISTS " + keyspace + "." + StorageConstants.SECURITY_TABLE + " ( ");
			cql.append("		  " + CassandraColumns.SECURITY_ID   + "     varchar,  ");
			cql.append("		  " + CassandraColumns.SECURITY_DESC   + "     varchar,  ");
			cql.append("		  " + CassandraColumns.OBJECT_PERMISSION_ARRAY   + "     varchar,  ");
			cql.append("		  " + CassandraColumns.INSERT_DATE + "     timestamp,  ");
			cql.append("		  " + CassandraColumns.UPDATE_DATE + "     timestamp,  ");
			cql.append("		  PRIMARY KEY (" + CassandraColumns.SECURITY_ID + ")  ");
			cql.append("   )  ");
			cql.append("   WITH  caching= {'keys':'ALL', 'rows_per_partition':'ALL'} ");
			cql.append("     AND compression = { 'class' : 'ZstdCompressor' } ");
			cql.append("     AND gc_grace_seconds = 3660  ");
			session.execute(cql.toString());
			
			log.info("Cassandra table [" + keyspace + "." + StorageConstants.SECURITY_TABLE + "] is created.");
			
			
			//MM_METADATA
			cql = new StringBuffer();
			cql.append(" CREATE TABLE IF NOT EXISTS " + keyspace + "." + StorageConstants.METADATA_TABLE + " ( ");
			cql.append("		  " + CassandraColumns.OBJECT_ID   + "     varchar,  ");
			cql.append("		  " + CassandraColumns.KEY     + "     varchar,  ");
			cql.append("		  " + CassandraColumns.VALUE   + "     varchar,  ");
			cql.append("		  " + CassandraColumns.DESCRIPTION + "   varchar,  ");
			cql.append("		  " + CassandraColumns.INSERT_DATE + "     timestamp,  ");
			cql.append("		  PRIMARY KEY (" + CassandraColumns.OBJECT_ID + ", " + CassandraColumns.KEY  + ")  ");
			cql.append("   )  ");
			cql.append("   WITH  caching= {'keys':'ALL', 'rows_per_partition':'ALL'} ");
			cql.append("     AND compression = { 'class' : 'ZstdCompressor' } ");
			cql.append("     AND gc_grace_seconds = 3660  ");
			
			session.execute(cql.toString());
			
			log.info("Cassandra table [" + keyspace + "." + StorageConstants.METADATA_TABLE + "] is created.");
			
			
			//JSON_MODEL_TABLE			
			cql = new StringBuffer();
			cql.append(" CREATE TABLE IF NOT EXISTS " + keyspace + "." + StorageConstants.JSON_MODEL_TABLE + " ( ");
			cql.append("		  " + CassandraColumns.PATH   + "     varchar,  ");
			cql.append("		  " + CassandraColumns.PAYLOAD    + "     text,  ");
			cql.append("		  PRIMARY KEY (" + CassandraColumns.PATH + ")  ");
			cql.append("   )  ");
			cql.append("   WITH  caching= {'keys':'ALL', 'rows_per_partition':'ALL'} ");
			cql.append("     AND compression = { 'class' : 'ZstdCompressor' } ");
			cql.append("     AND gc_grace_seconds = 3660  ");
			session.execute(cql.toString());
			
			log.info("Cassandra table [" + keyspace + "." + StorageConstants.JSON_MODEL_TABLE + "] is created.");
			
			
			//
		} catch (Exception ex) {
			log.error("Metadata table create error : " + ex.getMessage(), ex);
			
			throw ex;
		} finally {
			//
			// if (session != null && !session.isClosed())
			// session.close();
		}

	}

	@Override
	public void createTableForPointAndAlarm() throws Exception {

		try {
			
			//
			Session session     = CassandraSessionFixer.getSession();
			String keyspace    = CassandraSessionFixer.getKeyspace();
			String append_columns =CassandraSessionFixer.getAppendColumns();
			
			
			// ----------------------------------------------------------------------------------------
			// 포인트 테이블 생성
			// ----------------------------------------------------------------------------------------
				
			try {
				
				
				
				    //
				    String td_key = keyspace + "." + StorageConstants.TAG_POINT_TABLE;
				    
					//
					StringBuffer createCF = new StringBuffer();
					createCF.append(" CREATE TABLE IF NOT EXISTS " + td_key + " ( ");
					createCF.append("		  " + CassandraColumns.TAG_ID    + "   varchar,  ");
					createCF.append("		  " + CassandraColumns.TIMESTAMP + "   timestamp,  ");
					createCF.append("		  " + CassandraColumns.VALUE + "       varchar,   ");
					createCF.append("		  " + CassandraColumns.TYPE  + "       varchar,   ");
					createCF.append("		  " + CassandraColumns.QUALITY + "     int,   ");
					createCF.append("		  " + CassandraColumns.ERROR_CODE + "  int,   ");
					createCF.append("		  " + CassandraColumns.ATTRIBUTE + "  frozen<map<varchar, varchar>>,   ");
					createCF.append("		  PRIMARY KEY ("  + CassandraColumns.TAG_ID + ",  " + CassandraColumns.TIMESTAMP + ")  ");
					createCF.append("		)  WITH CLUSTERING ORDER BY ( " + CassandraColumns.TIMESTAMP + " DESC)  ");
					createCF.append("     AND compaction = { 'class': 'TimeWindowCompactionStrategy', 'compaction_window_size': '24', 'compaction_window_unit': 'HOURS', 'timestamp_resolution': 'MILLISECONDS', 'min_threshold': '4', 'max_threshold': '32', 'unchecked_tombstone_compaction' : 'true'    } ");
					createCF.append("     AND caching = {'keys':'ALL', 'rows_per_partition':'NONE'} ");
					createCF.append("     AND compression = { 'class' : 'ZstdCompressor', 'chunk_length_in_kb': '16', 'compression_level': '" +  ZSTD_COMPRESSION_LEVEL + "' } ");
					createCF.append("     AND bloom_filter_fp_chance = 0.01    ");
					createCF.append("     AND default_time_to_live =  " + StorageTTL.TAG_POINT_TABLE_TTL); //기본 7일
					createCF.append("     AND gc_grace_seconds = 3600 ");
					createCF.append("     AND max_index_interval = 2048 ");
					createCF.append("     AND memtable_flush_period_in_ms = 300000 ");
					createCF.append("     AND min_index_interval = 128 ");
					createCF.append("     AND speculative_retry = 'NONE';  ");
					session.execute(createCF.toString());
		
					// 
					JSONArray columns = JSONArray.fromObject(append_columns);
					if(columns.size() > 0){
						log.info("Cassandra append columns for point / aggregation = " + append_columns);
					};
					for (int i = 0; columns != null && i < columns.size(); i++) {
						try {
							JSONObject column = columns.getJSONObject(i);
							StringBuffer createCC = new StringBuffer();
							createCC.append("ALTER TABLE  " + td_key + " ADD " + column.getString("column") + "  " + column.getString("type") + "; ");
							session.execute(createCC.toString());
							log.info("Cassandra table add column : <" + column.getString("column") + " " + column.getString("type") + ">");
						} catch (Exception ex) {
							//
						}
					}
					;

					//
					for (int i = 0; columns != null && i < columns.size(); i++) {
						try {
							JSONObject column = columns.getJSONObject(i);
							//
							StringBuffer createCIDX = new StringBuffer();
							createCIDX.append(" CREATE INDEX IF NOT EXISTS " + StorageConstants.TAG_POINT_TABLE + "_T_IDX_" + column.getString("column").toUpperCase() + " ON " + td_key + " ("
									+ column.getString("column") + "); ");
							session.execute(createCIDX.toString());
						} catch (Exception ex) {
							//
						}
					}
					;
					
					//
					/*
					StringBuffer idx = new StringBuffer();
					idx.append(" CREATE INDEX IF NOT EXISTS " + StorageConstants.TAG_POINT_TABLE + "_T_IDX_"+ CassandraColumns.TAG_ID + " ON " + td_key + " (" + CassandraColumns.TAG_ID + "); ");
					*/
					
					//SASI SPARSE INDEX
					/*
					StringBuffer idx = new StringBuffer();
					idx.append(" CREATE CUSTOM INDEX IF NOT EXISTS " + StorageConstants.TAG_POINT_TABLE  + "_T_TIMESTAMP ");
					idx.append(" ON " + keyspace + "." + StorageConstants.TAG_POINT_TABLE + " (" + CassandraColumns.TIMESTAMP + ")  ");
					idx.append(" USING 'org.apache.cassandra.index.sasi.SASIIndex'  ");
					idx.append(" WITH OPTIONS = { 'mode': 'SPARSE' };  ");
					session.execute(idx.toString());
					*/
					
					//
				
					log.info("Cassandra table [" + td_key + "] is created.");
				} catch (Exception ex) {
					log.error("Cassandra table and index create failed : " + ex.getMessage(), ex);
				} finally {
					// ;
				};
				
				
				// ----------------------------------------------------------------------------------------
				// 포인트 매트릭스 테이블 생성
				// ----------------------------------------------------------------------------------------
				
				try {
					
					//
					StringBuffer createCF = new StringBuffer();
					createCF.append(" CREATE TABLE IF NOT EXISTS " +  keyspace + "." + StorageConstants.TAG_POINT_MATRIX_TABLE + " ( ");
					createCF.append("		  " + CassandraColumns.DATE   + "      int,  ");
					createCF.append("		  " + CassandraColumns.TIMESTAMP + "   timestamp,  ");
					createCF.append("		  " + CassandraColumns.FROM      + "   timestamp,  ");
					createCF.append("		  " + CassandraColumns.TO        + "   timestamp,  ");
					createCF.append("		  " + CassandraColumns.POINT_MAP + "   frozen<map<text, frozen<list<text>>>>,   ");
					createCF.append("		  PRIMARY KEY (" + CassandraColumns.DATE + ", " + CassandraColumns.TIMESTAMP +  ")  ");
					createCF.append("		)  WITH CLUSTERING ORDER BY ( " + CassandraColumns.TIMESTAMP + " DESC)  ");
					createCF.append("     AND compaction = { 'class': 'TimeWindowCompactionStrategy', 'compaction_window_size': '1', 'compaction_window_unit': 'DAYS', 'timestamp_resolution': 'MILLISECONDS', 'max_threshold': '32', 'min_threshold': '4', 'unchecked_tombstone_compaction' : 'true'  } ");
					createCF.append("     AND caching = {'keys':'ALL', 'rows_per_partition':'NONE'} ");
					createCF.append("     AND compression = { 'class' : 'ZstdCompressor', 'chunk_length_in_kb': '16', 'compression_level': '" +  ZSTD_COMPRESSION_LEVEL + "' } ");
					createCF.append("     AND bloom_filter_fp_chance = 0.01    ");
					createCF.append("     AND default_time_to_live =  0 "); 
					createCF.append("     AND gc_grace_seconds = 0 ");
					createCF.append("     AND max_index_interval = 2048 ");
					createCF.append("     AND memtable_flush_period_in_ms = 0 ");
					createCF.append("     AND min_index_interval = 128 ");
					createCF.append("     AND speculative_retry = 'NONE';  ");
					session.execute(createCF.toString());
			
					//
					log.info("Cassandra table [" +  keyspace + "." + StorageConstants.TAG_POINT_MATRIX_TABLE + "] is created.");
				} catch (Exception ex) {
					log.error("Cassandra table and index create failed : " + ex.getMessage(), ex);
				} finally {
					// ;
				};
				
				
				// ----------------------------------------------------------------------------------------
				// 포인트 원본 테이블 생성
				// ----------------------------------------------------------------------------------------
				
				try {
					
					//
					StringBuffer createCF = new StringBuffer();
					createCF.append(" CREATE TABLE IF NOT EXISTS " +  keyspace + "." + StorageConstants.TAG_POINT_RAW_TABLE + " ( ");
					createCF.append("		  " + CassandraColumns.YYYY   + "      int,  ");
					createCF.append("		  " + CassandraColumns.MM     + "      int,  ");
					createCF.append("		  " + CassandraColumns.TAG_ID + "      varchar,  ");
					createCF.append("		  " + CassandraColumns.TIMESTAMP + "   timestamp,  ");
					createCF.append("		  " + CassandraColumns.VALUE + "       varchar,   ");
					createCF.append("		  " + CassandraColumns.TYPE  + "       varchar,   ");
					createCF.append("		  " + CassandraColumns.QUALITY + "     int,   ");
					createCF.append("		  " + CassandraColumns.ERROR_CODE + "  int,   ");
					createCF.append("		  " + CassandraColumns.ATTRIBUTE + "  frozen<map<varchar, varchar>>,   ");
					createCF.append("		  PRIMARY KEY ( (" + CassandraColumns.YYYY + ", " + CassandraColumns.MM + " ) , " + CassandraColumns.TAG_ID + ", " + CassandraColumns.TIMESTAMP + ")  ");
					createCF.append("		)  WITH CLUSTERING ORDER BY ( " + CassandraColumns.TAG_ID + " ASC, " + CassandraColumns.TIMESTAMP + " DESC)  ");
					createCF.append("     AND compaction = {  'class' : 'SizeTieredCompactionStrategy', 'min_sstable_size' : '50', 'min_threshold' : '4', 'max_threshold' : '32', 'unchecked_tombstone_compaction' : 'true', 'tombstone_compaction_interval' : '1800', 'tombstone_threshold' : '0.01'   }");
					createCF.append("     AND caching= {'keys':'ALL', 'rows_per_partition':'NONE'} ");
					createCF.append("     AND compression = { 'class' : 'ZstdCompressor',  'chunk_length_in_kb': '16',  'compression_level': '" +  ZSTD_COMPRESSION_LEVEL + "' } ");
					createCF.append("     AND bloom_filter_fp_chance = 0.01    ");
					createCF.append("     AND default_time_to_live =  0 ");
					createCF.append("     AND gc_grace_seconds = 60 ");
					createCF.append("     AND max_index_interval = 2048 ");
					createCF.append("     AND memtable_flush_period_in_ms = 0 ");
					createCF.append("     AND min_index_interval = 128 ");
					createCF.append("     AND speculative_retry = 'NONE';  ");
					session.execute(createCF.toString());
			
					//
					log.info("Cassandra table [" +  keyspace + "." + StorageConstants.TAG_POINT_RAW_TABLE + "] is created.");
				} catch (Exception ex) {
					log.error("Cassandra table and index create failed : " + ex.getMessage(), ex);
				} finally {
					// ;
				};
			
			

			// ----------------------------------------------------------------------------------------
			// 포인트 날자 제한 테이블 생성
			// ----------------------------------------------------------------------------------------
			/*
			try {
				
				//
				StringBuffer createCF = new StringBuffer();
				createCF.append(" CREATE TABLE IF NOT EXISTS " +  keyspace + "." + StorageConstants.TAG_POINT_LIMIT_1_DAYS_TABLE + " ( ");
				createCF.append("		  " + CassandraColumns.TAG_ID + "      varchar,  ");
				createCF.append("		  " + CassandraColumns.TIMESTAMP + "   timestamp,  ");
				createCF.append("		  " + CassandraColumns.VALUE + "       varchar,   ");
				createCF.append("		  " + CassandraColumns.TYPE  + "       varchar,   ");
				createCF.append("		  " + CassandraColumns.QUALITY + "     int,   ");
				createCF.append("		  " + CassandraColumns.ERROR_CODE + "  int,   ");
				createCF.append("		  " + CassandraColumns.ATTRIBUTE + "  frozen<map<varchar, varchar>>,   ");
				createCF.append("		  PRIMARY KEY (" + CassandraColumns.TAG_ID + ", " + CassandraColumns.TIMESTAMP + ")  ");
				createCF.append("		)  WITH CLUSTERING ORDER BY (" + CassandraColumns.TIMESTAMP + " DESC)  ");
				createCF.append("     AND compaction = { 'class': 'TimeWindowCompactionStrategy', 'compaction_window_size': '30', 'compaction_window_unit': 'MINUTES', 'timestamp_resolution': 'MILLISECONDS', 'max_threshold': '32',  'min_threshold': '4','unchecked_tombstone_compaction' : 'true'  }");
				createCF.append("     AND caching= {'keys':'ALL', 'rows_per_partition':'NONE'} ");
				createCF.append("     AND compression = { 'class' : 'ZstdCompressor',  'compression_level': '" +  ZSTD_COMPRESSION_LEVEL + "' } ");
				createCF.append("     AND bloom_filter_fp_chance = 0.01    ");
				createCF.append("     AND default_time_to_live =  " + StorageTTL.TAG_POINT_LIMIT_1_DAYS_TABLE_TTL);
				createCF.append("     AND gc_grace_seconds = 1860 ");
				createCF.append("     AND max_index_interval = 2048 ");
				createCF.append("     AND memtable_flush_period_in_ms = 0 ");
				createCF.append("     AND min_index_interval = 128 ");
				createCF.append("     AND speculative_retry = 'NONE';  ");
				session.execute(createCF.toString());
		
				//
				log.info("Cassandra table [" +  keyspace + "." + StorageConstants.TAG_POINT_LIMIT_1_DAYS_TABLE + "] is created.");
			} catch (Exception ex) {
				log.error("Cassandra table and index create failed : " + ex.getMessage(), ex);
			} finally {
				// ;
			};
			
			
			try {
				
				//
				StringBuffer createCF = new StringBuffer();
				createCF.append(" CREATE TABLE IF NOT EXISTS "  +  keyspace + "." + StorageConstants.TAG_POINT_LIMIT_7_DAYS_TABLE + " ( ");
				createCF.append("		  " + CassandraColumns.TAG_ID + "      varchar,  ");
				createCF.append("		  " + CassandraColumns.TIMESTAMP + "   timestamp,  ");
				createCF.append("		  " + CassandraColumns.VALUE + "       varchar,   ");
				createCF.append("		  " + CassandraColumns.TYPE  + "       varchar,   ");
				createCF.append("		  " + CassandraColumns.QUALITY + "     int,   ");
				createCF.append("		  " + CassandraColumns.ERROR_CODE + "  int,   ");
				createCF.append("		  " + CassandraColumns.ATTRIBUTE + "  frozen<map<varchar, varchar>>,   ");
				createCF.append("		  PRIMARY KEY (" + CassandraColumns.TAG_ID + ", " + CassandraColumns.TIMESTAMP + ")  ");
				createCF.append("		)  WITH CLUSTERING ORDER BY (" + CassandraColumns.TIMESTAMP + " DESC)  ");
				createCF.append("     AND compaction = { 'class': 'TimeWindowCompactionStrategy', 'compaction_window_size': '1', 'compaction_window_unit': 'HOURS', 'timestamp_resolution': 'MILLISECONDS', 'max_threshold': '32', 'min_threshold': '4','unchecked_tombstone_compaction' : 'true'  }");
				createCF.append("     AND caching= {'keys':'ALL', 'rows_per_partition':'NONE'} ");
				createCF.append("     AND compression = { 'class' : 'ZstdCompressor',  'compression_level': '" +  ZSTD_COMPRESSION_LEVEL + "' } ");
				createCF.append("     AND bloom_filter_fp_chance = 0.01    ");
				createCF.append("     AND default_time_to_live =  " + StorageTTL.TAG_POINT_LIMIT_7_DAYS_TABLE_TTL);
				createCF.append("     AND gc_grace_seconds = 3660 ");
				createCF.append("     AND max_index_interval = 2048 ");
				createCF.append("     AND memtable_flush_period_in_ms = 0 ");
				createCF.append("     AND min_index_interval = 128 ");
				createCF.append("     AND speculative_retry = 'NONE';  ");
				session.execute(createCF.toString());
		
				//
				log.info("Cassandra table [" +  keyspace + "." + StorageConstants.TAG_POINT_LIMIT_7_DAYS_TABLE + "] is created.");
			} catch (Exception ex) {
				log.error("Cassandra table and index create failed : " + ex.getMessage(), ex);
			} finally {
				// ;
			};
			
			
			try {
				
				//
				StringBuffer createCF = new StringBuffer();
				createCF.append(" CREATE TABLE IF NOT EXISTS "  +  keyspace + "." + StorageConstants.TAG_POINT_LIMIT_15_DAYS_TABLE + " ( ");
				createCF.append("		  " + CassandraColumns.TAG_ID + "      varchar,  ");
				createCF.append("		  " + CassandraColumns.TIMESTAMP + "   timestamp,  ");
				createCF.append("		  " + CassandraColumns.VALUE + "       varchar,   ");
				createCF.append("		  " + CassandraColumns.TYPE  + "       varchar,   ");
				createCF.append("		  " + CassandraColumns.QUALITY + "     int,   ");
				createCF.append("		  " + CassandraColumns.ERROR_CODE + "  int,   ");
				createCF.append("		  " + CassandraColumns.ATTRIBUTE + "  frozen<map<varchar, varchar>>,   ");
				createCF.append("		  PRIMARY KEY (" + CassandraColumns.TAG_ID + ", " + CassandraColumns.TIMESTAMP + ")  ");
				createCF.append("		)  WITH CLUSTERING ORDER BY (" + CassandraColumns.TIMESTAMP + " DESC)  ");
				createCF.append("     AND compaction = { 'class': 'TimeWindowCompactionStrategy', 'compaction_window_size': '12', 'compaction_window_unit': 'HOURS', 'timestamp_resolution': 'MILLISECONDS', 'max_threshold': '32', 'min_threshold': '4', 'unchecked_tombstone_compaction' : 'true'  }");
				createCF.append("     AND caching= {'keys':'ALL', 'rows_per_partition':'NONE'} ");
				createCF.append("     AND compression = { 'class' : 'ZstdCompressor',  'compression_level': '" +  ZSTD_COMPRESSION_LEVEL + "' } ");
				createCF.append("     AND bloom_filter_fp_chance = 0.01    ");
				createCF.append("     AND default_time_to_live =  " + StorageTTL.TAG_POINT_LIMIT_15_DAYS_TABLE_TTL);
				createCF.append("     AND gc_grace_seconds = 7260 ");
				createCF.append("     AND max_index_interval = 2048 ");
				createCF.append("     AND memtable_flush_period_in_ms = 0 ");
				createCF.append("     AND min_index_interval = 128 ");
				createCF.append("     AND speculative_retry = 'NONE';  ");
				session.execute(createCF.toString());
		
				//
				log.info("Cassandra table [" +  keyspace + "." + StorageConstants.TAG_POINT_LIMIT_15_DAYS_TABLE + "] is created.");
			} catch (Exception ex) {
				log.error("Cassandra table and index create failed : " + ex.getMessage(), ex);
			} finally {
				// ;
			};
			
			try {
				
				//
				StringBuffer createCF = new StringBuffer();
				createCF.append(" CREATE TABLE IF NOT EXISTS " +  keyspace + "."  + StorageConstants.TAG_POINT_LIMIT_31_DAYS_TABLE + " ( ");
				createCF.append("		  " + CassandraColumns.TAG_ID + "      varchar,  ");
				createCF.append("		  " + CassandraColumns.TIMESTAMP + "   timestamp,  ");
				createCF.append("		  " + CassandraColumns.VALUE + "       varchar,   ");
				createCF.append("		  " + CassandraColumns.TYPE  + "       varchar,   ");
				createCF.append("		  " + CassandraColumns.QUALITY + "     int,   ");
				createCF.append("		  " + CassandraColumns.ERROR_CODE + "  int,   ");
				createCF.append("		  " + CassandraColumns.ATTRIBUTE + "  frozen<map<varchar, varchar>>,   ");
				createCF.append("		  PRIMARY KEY (" + CassandraColumns.TAG_ID + ", " + CassandraColumns.TIMESTAMP + ")  ");
				createCF.append("		)  WITH CLUSTERING ORDER BY (" + CassandraColumns.TIMESTAMP + " DESC)  ");
				createCF.append("     AND compaction = { 'class': 'TimeWindowCompactionStrategy', 'compaction_window_size': '24', 'compaction_window_unit': 'HOURS', 'timestamp_resolution': 'MILLISECONDS' , 'max_threshold': '32', 'min_threshold': '4', 'unchecked_tombstone_compaction' : 'true'  }");
				createCF.append("     AND caching= {'keys':'ALL', 'rows_per_partition':'NONE'} ");
				createCF.append("     AND compression = { 'class' : 'ZstdCompressor',  'compression_level': '" +  ZSTD_COMPRESSION_LEVEL + "' } ");
				createCF.append("     AND bloom_filter_fp_chance = 0.01    ");
				createCF.append("     AND default_time_to_live =  " + StorageTTL.TAG_POINT_LIMIT_31_DAYS_TABLE_TTL);
				createCF.append("     AND gc_grace_seconds = 7260 ");
				createCF.append("     AND max_index_interval = 2048 ");
				createCF.append("     AND memtable_flush_period_in_ms = 0 ");
				createCF.append("     AND min_index_interval = 128 ");
				createCF.append("     AND speculative_retry = 'NONE';  ");
				session.execute(createCF.toString());
		
				//
				log.info("Cassandra table [" +  keyspace + "." + StorageConstants.TAG_POINT_LIMIT_31_DAYS_TABLE + "] is created.");
			} catch (Exception ex) {
				log.error("Cassandra table and index create failed : " + ex.getMessage(), ex);
			} finally {
				// ;
			};
			*/
			

			// ----------------------------------------------------------------------------------------
			// 포인트 맵 테이블 생성
			// ----------------------------------------------------------------------------------------
			String tr_key = keyspace + "." + StorageConstants.TAG_POINT_MAP_TABLE;
			

				try {

					//
					StringBuffer createCT = new StringBuffer();
					createCT.append("CREATE TABLE IF NOT EXISTS " + tr_key + " ");
					createCT.append(" (  ");
					createCT.append("    " + CassandraColumns.YEAR       + " int,  ");
					createCT.append("    " + CassandraColumns.MONTH      + " int,  ");
					createCT.append("    " + CassandraColumns.DAY        + " int,  ");
					createCT.append("    " + CassandraColumns.HOUR       + " int,  ");
					createCT.append("    " + CassandraColumns.MINUTE     + " int,  ");
					createCT.append("    " + CassandraColumns.TIMESTAMP + " timestamp,  ");
					createCT.append("    " + CassandraColumns.MAP  + " text,  ");
					createCT.append("    PRIMARY KEY ((" + CassandraColumns.YEAR + "," + CassandraColumns.MONTH + "," + CassandraColumns.DAY + ", " + CassandraColumns.HOUR + ", " + CassandraColumns.MINUTE + " ), " + CassandraColumns.TIMESTAMP + ")  ");
					createCT.append(" )  WITH CLUSTERING ORDER BY (" + CassandraColumns.TIMESTAMP + " DESC)  ");
					createCT.append("     AND compaction  = { 'class':  'TimeWindowCompactionStrategy', 'compaction_window_size': '10',  'compaction_window_unit': 'MINUTES', 'max_threshold': '32', 'min_threshold': '4', 'unchecked_tombstone_compaction' : 'true' } ");
					createCT.append("     AND compression = { 'class' : 'ZstdCompressor',  'compression_level': '" +  ZSTD_COMPRESSION_LEVEL + "'} ");
					createCT.append("     AND default_time_to_live =  "  + StorageTTL.TAG_POINT_MAP_TABLE_TTL); //
					createCT.append("     AND gc_grace_seconds = 4200 ");
					createCT.append("     AND max_index_interval = 2048 ");
					createCT.append("     AND memtable_flush_period_in_ms = 0 ");
					createCT.append("     AND min_index_interval = 128 ");
					createCT.append("     AND speculative_retry = 'NONE';  ");
					session.execute(createCT.toString());

					//
					
					//StringBuffer createIDX = new StringBuffer();
					//createIDX.append(" CREATE INDEX IF NOT EXISTS " + StorageConstants.TAG_POINT_MAP_TABLE + "_IDX_TIMESTAMP on " + tr_key + " (" + CassandraColumns.TIMESTAMP + "); ");
					//session.execute(createIDX.toString());
					
					//StringBuffer createIDX_ATTR = new StringBuffer();
					//createIDX_ATTR.append(" CREATE INDEX IF NOT EXISTS " + StorageConstants.TAG_POINT_MAP_TABLE + "_IDX_MAP on " + tr_key + " (ENTRIES(" + CassandraColumns.DATA_MAP + ")); ");
					//session.execute(createIDX_ATTR.toString());
					
					log.info("Cassandra table [" + tr_key + "] is created.");
				} catch (Exception ex) {
					log.error("Cassandra table [" + tr_key + "] and index create failed : " + ex.getMessage(), ex);
				} finally {
					// ;
				}
		
				// 포인트 카운트 테이블 생성
				String tm_key = keyspace + "." + StorageConstants.TAG_POINT_COUNT_TABLE;
				try {

					
					StringBuffer createCT = new StringBuffer();
					createCT.append("CREATE TABLE IF NOT EXISTS " + tm_key + " ");
					createCT.append(" (  ");
					createCT.append("    " + CassandraColumns.TAG_ID + " varchar,  ");
					createCT.append("    " + CassandraColumns.COUNT + " counter,  ");
					createCT.append("    PRIMARY KEY (" + CassandraColumns.TAG_ID + ")  ");
					createCT.append(" );   ");
					session.execute(createCT.toString());

					//
					log.info("Cassandra table [" + tm_key + "] is created.");
				} catch (Exception ex) {
					log.error("Cassandra table and index create failed : " + ex.getMessage(), ex);
				} finally {
					// ;
				}
				
				
				// 포인트 카운트 날자별 테이블 생성
				String tmd_key = keyspace + "." + StorageConstants.TAG_POINT_COUNT_BY_HOUR_TABLE;
				try {
					StringBuffer createCT = new StringBuffer();
					createCT.append("CREATE TABLE IF NOT EXISTS " + tmd_key + " ");
					createCT.append(" (  ");
					createCT.append("    " + CassandraColumns.TAG_ID + " varchar,  ");
					createCT.append("    " + CassandraColumns.COUNT  + " counter,  ");
					createCT.append("    " + CassandraColumns.DATE   + " int,  ");
					createCT.append("    " + CassandraColumns.HOUR   + " int,  ");
					createCT.append("    PRIMARY KEY ((" + CassandraColumns.TAG_ID + "), " +  CassandraColumns.DATE  + ", " + CassandraColumns.HOUR + " )  ");
					createCT.append(" ) WITH CLUSTERING ORDER BY (" + CassandraColumns.DATE + " DESC, " + CassandraColumns.HOUR + " DESC);  ");
					session.execute(createCT.toString());

					//
					log.info("Cassandra table [" + tmd_key + "] is created.");
				} catch (Exception ex) {
					log.error("Cassandra table and index create failed : " + ex.getMessage(), ex);
				} finally {
					// ;
				}
			
			
			// 사이트별 포인트 카운트 테이블 생성
			String tcs_key = keyspace + "." + StorageConstants.TAG_POINT_COUNT_BY_SITE_TABLE;
			
				try {

					StringBuffer createCT = new StringBuffer();
					createCT.append("CREATE TABLE IF NOT EXISTS " + tcs_key + " ");
					createCT.append(" (  ");
					createCT.append("    " + CassandraColumns.SITE_ID + " varchar,  ");
					createCT.append("    " + CassandraColumns.COUNT + " counter,  ");
					createCT.append("    PRIMARY KEY (" + CassandraColumns.SITE_ID + ")  ");
					createCT.append(" );   ");
					session.execute(createCT.toString());

					//
				
					log.info("Cassandra table [" + tcs_key + "] is created.");
				} catch (Exception ex) {
					log.error("Cassandra table and index create failed : " + ex.getMessage(), ex);
				} finally {
					// ;
				}
			

			// OPC 별 포인트 카운트 테이블 생성
			String tco_key = keyspace + "." + StorageConstants.TAG_POINT_COUNT_BY_OPC_TABLE;
			
				try {

					StringBuffer createCT = new StringBuffer();
					createCT.append("CREATE TABLE IF NOT EXISTS " + tco_key + " ");
					createCT.append(" (  ");
					createCT.append("    " + CassandraColumns.OPC_ID + " varchar,  ");
					createCT.append("    " + CassandraColumns.COUNT + " counter,  ");
					createCT.append("    PRIMARY KEY (" + CassandraColumns.OPC_ID + ")  ");
					createCT.append(" );   ");
					session.execute(createCT.toString());

					//
					
					log.info("Cassandra table [" + tco_key + "] is created.");
				} catch (Exception ex) {
					log.error("Cassandra table and index create failed : " + ex.getMessage(), ex);
				} finally {

				}
			
			
			
			// OPC 마지막 업데이트 테이블 생성
			String lco_key = keyspace + "." + StorageConstants.OPC_POINT_LAST_UPDATE_TABLE;
			
				try {

				
					StringBuffer createCT = new StringBuffer();
					createCT.append("CREATE TABLE IF NOT EXISTS " + lco_key + " ");
					createCT.append(" (  ");
					createCT.append("    " + CassandraColumns.OPC_ID + " varchar,  ");
					createCT.append("    " + CassandraColumns.LAST_UPDATE + "  timestamp,  ");
					createCT.append("    PRIMARY KEY (" + CassandraColumns.OPC_ID + ")  ");
					createCT.append(" );   ");
					session.execute(createCT.toString());

					//
				
					log.info("Cassandra table [" + lco_key + "] is created.");
				} catch (Exception ex) {
					log.error("Cassandra table and index create failed : " + ex.getMessage(), ex);
				} finally {

				}

			// 사이트 요약 테이블 생성
			String ssm_key = keyspace + "." + StorageConstants.SITE_SUMMARY_TABLE;
			

				try {

					
					StringBuffer createCT = new StringBuffer();
					createCT.append("CREATE TABLE IF NOT EXISTS " + ssm_key + " ( ");
					createCT.append("		" + CassandraColumns.SITE_ID + " varchar, ");
					createCT.append("		" + CassandraColumns.YEAR + " int,");
					createCT.append("		" + CassandraColumns.MONTH + " int,");
					createCT.append("		" + CassandraColumns.DAY + " int,");
					createCT.append("		" + CassandraColumns.HOUR + " int,");
					createCT.append("		" + CassandraColumns.MINUTE + " int,");
					createCT.append("		" + CassandraColumns.DATA_COUNT + " counter,");
					createCT.append("		" + CassandraColumns.ALARM_COUNT + " counter,");
					createCT.append("		" + CassandraColumns.ALARM_COUNT_BY_INFO + " counter,");
					createCT.append("		" + CassandraColumns.ALARM_COUNT_BY_WARN + " counter,");
					createCT.append("		" + CassandraColumns.ALARM_COUNT_BY_ERROR + " counter,");
					createCT.append("		primary key((" + CassandraColumns.SITE_ID + "), " + CassandraColumns.YEAR + ", " + CassandraColumns.MONTH + ", " + CassandraColumns.DAY + ", "
							+ CassandraColumns.HOUR + ", " + CassandraColumns.MINUTE + ")");
					createCT.append("		) WITH CLUSTERING ORDER BY (" + CassandraColumns.YEAR + " desc, " + CassandraColumns.MONTH + " desc, " + CassandraColumns.DAY + " desc, "
							+ CassandraColumns.HOUR + " desc, " + CassandraColumns.MINUTE + " desc); ");
					session.execute(createCT.toString());

					//
					log.info("Cassandra table [" + ssm_key + "] is created.");
				} catch (Exception ex) {
					log.error("Cassandra table and index create failed : " + ex.getMessage(), ex);
				} finally {
					// ;
				}
			
			
			
				// ----------------------------------------------------------------------------------------
				// 알람 테이블
				// ----------------------------------------------------------------------------------------
				String ta_key = keyspace + "." + StorageConstants.TAG_ARLAM_TABLE;
				
					try {

					
						StringBuffer createCA = new StringBuffer();
						createCA.append("CREATE TABLE IF NOT EXISTS " + ta_key + " ");
						createCA.append(" (  ");
						createCA.append("    " + CassandraColumns.TAG_ID + " varchar,  ");
						createCA.append("    " + CassandraColumns.ASSET_ID + " varchar,  ");
						createCA.append("    " + CassandraColumns.TIMESTAMP + " timestamp,  ");
						createCA.append("    " + CassandraColumns.PRIORITY + " varchar,  ");
						createCA.append("    " + CassandraColumns.DESCRIPTION + " text,  ");
						createCA.append("    " + CassandraColumns.ALARM_SEQ + " int,  ");
						createCA.append("    " + CassandraColumns.ALARM_CONFIG_ID + " varchar,  ");
						createCA.append("	 " + CassandraColumns.ATTRIBUTE + "  map<varchar, varchar>,   ");
						createCA.append("    PRIMARY KEY (" + CassandraColumns.TAG_ID + "," + CassandraColumns.TIMESTAMP + ")  ");
						createCA.append(" )  WITH  CLUSTERING ORDER BY (" + CassandraColumns.TIMESTAMP + " DESC) ");
						createCA.append("     AND compaction  = { 'class': 'TimeWindowCompactionStrategy', 'compaction_window_size': '28', 'compaction_window_unit': 'DAYS', 'max_threshold': '32', 'min_threshold': '4' } ");
						createCA.append("     AND compression = { 'class' : 'ZstdCompressor',  'compression_level': '" +  ZSTD_COMPRESSION_LEVEL + "' } ");
						createCA.append("     AND default_time_to_live = 0 ");
						createCA.append("     AND gc_grace_seconds = 10800 ");
						createCA.append("     AND max_index_interval = 2048 ");
						createCA.append("     AND memtable_flush_period_in_ms = 0 ");
						createCA.append("     AND min_index_interval = 128 ");
						createCA.append("     AND speculative_retry = 'NONE';  ");
						session.execute(createCA.toString());
						
						
						//異붽� �븘�뱶 28媛�  �벑濡� 2018_08_21
						//
						try {
							
							//10
							session.execute(" ALTER TABLE  " + keyspace + "." + StorageConstants.TAG_ARLAM_TABLE  + " ADD  "  + "SITE_ID"  + "  text  ");
							session.execute(" ALTER TABLE  " + keyspace + "." + StorageConstants.TAG_ARLAM_TABLE  + " ADD  "  + "SITE_NAME"  + "  text  ");
							session.execute(" ALTER TABLE  " + keyspace + "." + StorageConstants.TAG_ARLAM_TABLE  + " ADD  "  + "SITE_DESCRIPTION"  + "  text  ");
							session.execute(" ALTER TABLE  " + keyspace + "." + StorageConstants.TAG_ARLAM_TABLE  + " ADD  "  + "OPC_ID"  + "  text  ");
							session.execute(" ALTER TABLE  " + keyspace + "." + StorageConstants.TAG_ARLAM_TABLE  + " ADD  "  + "OPC_NAME"  + "  text  ");
							session.execute(" ALTER TABLE  " + keyspace + "." + StorageConstants.TAG_ARLAM_TABLE  + " ADD  "  + "OPC_DESCRIPTION"  + "  text  ");
							session.execute(" ALTER TABLE  " + keyspace + "." + StorageConstants.TAG_ARLAM_TABLE  + " ADD  "  + "TAG_NAME"   + "  text  ");
							session.execute(" ALTER TABLE  " + keyspace + "." + StorageConstants.TAG_ARLAM_TABLE  + " ADD  "  + "TAG_DESCRIPTION"  + "  text  ");
							session.execute(" ALTER TABLE  " + keyspace + "." + StorageConstants.TAG_ARLAM_TABLE  + " ADD  "  + "ASSET_NAME"   + "  text  ");
							session.execute(" ALTER TABLE  " + keyspace + "." + StorageConstants.TAG_ARLAM_TABLE  + " ADD  "  + "ASSET_DESCRIPTION"  + "  text  ");
							
							//12
							session.execute(" ALTER TABLE  " + keyspace + "." + StorageConstants.TAG_ARLAM_TABLE  + " ADD  TAG_JAVA_TYPE  text  ");
							session.execute(" ALTER TABLE  " + keyspace + "." + StorageConstants.TAG_ARLAM_TABLE  + " ADD  TAG_INTERVAL  int  ");
							session.execute(" ALTER TABLE  " + keyspace + "." + StorageConstants.TAG_ARLAM_TABLE  + " ADD  TAG_ALIAS_NAME  text  ");
							session.execute(" ALTER TABLE  " + keyspace + "." + StorageConstants.TAG_ARLAM_TABLE  + " ADD  TAG_UNIT  text  ");
							session.execute(" ALTER TABLE  " + keyspace + "." + StorageConstants.TAG_ARLAM_TABLE  + " ADD  TAG_DISPLAY_FORMAT  text  ");
							session.execute(" ALTER TABLE  " + keyspace + "." + StorageConstants.TAG_ARLAM_TABLE  + " ADD  TAG_IMPORTANCE  int  ");
							session.execute(" ALTER TABLE  " + keyspace + "." + StorageConstants.TAG_ARLAM_TABLE  + " ADD  TAG_LAT  text  ");
							session.execute(" ALTER TABLE  " + keyspace + "." + StorageConstants.TAG_ARLAM_TABLE  + " ADD  TAG_LNG  text  ");
							session.execute(" ALTER TABLE  " + keyspace + "." + StorageConstants.TAG_ARLAM_TABLE  + " ADD  TAG_MIN_VALUE  text  ");
							session.execute(" ALTER TABLE  " + keyspace + "." + StorageConstants.TAG_ARLAM_TABLE  + " ADD  TAG_MAX_VALUE  text  ");
							session.execute(" ALTER TABLE  " + keyspace + "." + StorageConstants.TAG_ARLAM_TABLE  + " ADD  TAG_NOTE  text  ");
							session.execute(" ALTER TABLE  " + keyspace + "." + StorageConstants.TAG_ARLAM_TABLE  + " ADD  TAG_LOCATION  text  ");
							
							//6
							session.execute(" ALTER TABLE  " + keyspace + "." + StorageConstants.TAG_ARLAM_TABLE  + " ADD  ALARM_CONFIG_NAME  text  ");
							session.execute(" ALTER TABLE  " + keyspace + "." + StorageConstants.TAG_ARLAM_TABLE  + " ADD  ALARM_CONFIG_DESCRIPTION  text  ");
							session.execute(" ALTER TABLE  " + keyspace + "." + StorageConstants.TAG_ARLAM_TABLE  + " ADD  ALARM_CONFIG_EPL  text  ");
							session.execute(" ALTER TABLE  " + keyspace + "." + StorageConstants.TAG_ARLAM_TABLE  + " ADD  ALARM_CONFIG_RECIEVE_ME  boolean  ");
							session.execute(" ALTER TABLE  " + keyspace + "." + StorageConstants.TAG_ARLAM_TABLE  + " ADD  ALARM_CONFIG_RECIEVE_OTHERS  text  ");
							session.execute(" ALTER TABLE  " + keyspace + "." + StorageConstants.TAG_ARLAM_TABLE  + " ADD  ALARM_CONFIG_INSERT_USER_ID  text  ");
							
							
							//
						}catch(Exception ex) {
							//
						}
						
						// 알람 인덱스
						StringBuffer createCDX1 = new StringBuffer();
						createCDX1.append(" CREATE INDEX IF NOT EXISTS "  + StorageConstants.TAG_ARLAM_TABLE  + "_T_IDX_ALARM_CONFIG_ID on " + ta_key + " (" + CassandraColumns.ALARM_CONFIG_ID + "); ");
						session.execute(createCDX1.toString());

						StringBuffer createCDX2 = new StringBuffer();
						createCDX2.append(" CREATE INDEX IF NOT EXISTS "  + StorageConstants.TAG_ARLAM_TABLE  + "_T_IDX_PRIORITY on " + ta_key + " (" + CassandraColumns.PRIORITY + "); ");
						session.execute(createCDX2.toString());
						
						StringBuffer createCDX3 = new StringBuffer();
						createCDX3.append(" CREATE INDEX IF NOT EXISTS "  + StorageConstants.TAG_ARLAM_TABLE  + "_T_IDX_ASSET_ID on " + ta_key + " (" + CassandraColumns.ASSET_ID + "); ");
						session.execute(createCDX3.toString());
						
						
						// SASI
						/*
						StringBuffer sasi_option = new StringBuffer();
						sasi_option.append(" USING 'org.apache.cassandra.index.sasi.SASIIndex' ");
						sasi_option.append("     WITH OPTIONS = { ");
						sasi_option.append(" 	'mode': 'CONTAINS', ");
						sasi_option.append(" 	'analyzer_class': 'org.apache.cassandra.index.sasi.analyzer.StandardAnalyzer', ");
						sasi_option.append(" 	'analyzed': 'true', ");
						sasi_option.append(" 	'tokenization_skip_stop_words': 'and, the, or', ");
						sasi_option.append(" 	'tokenization_enable_stemming': 'true', ");
						sasi_option.append(" 	'tokenization_normalize_lowercase': 'true', ");
						sasi_option.append(" 	'tokenization_locale': 'ko' ");
						sasi_option.append(" } ");
						*/
						StringBuffer sasi_option = new StringBuffer();
						sasi_option.append(" USING 'org.apache.cassandra.index.sasi.SASIIndex' ");
						sasi_option.append("     WITH OPTIONS = { ");
						sasi_option.append(" 	'mode': 'CONTAINS',");
						sasi_option.append(" 	'analyzer_class': 'org.apache.cassandra.index.sasi.analyzer.NonTokenizingAnalyzer',");
						sasi_option.append(" 	'case_sensitive': 'false' ");
						sasi_option.append(" } ");
						
						StringBuffer sasi_idx_cql = new StringBuffer();
						sasi_idx_cql.append(" CREATE CUSTOM INDEX IF NOT EXISTS " + StorageConstants.TAG_ARLAM_TABLE  + "_T_IDX_DESCRIPTION on " + ta_key + " (" + CassandraColumns.DESCRIPTION + ") " + sasi_option.toString());
						session.execute(sasi_idx_cql.toString());
						
						sasi_idx_cql = new StringBuffer();
						sasi_idx_cql.append(" CREATE CUSTOM INDEX IF NOT EXISTS " + StorageConstants.TAG_ARLAM_TABLE  + "_T_IDX_TAG_NAME on " + ta_key + " (" + CassandraColumns.TAG_NAME  + ") " + sasi_option.toString());
						session.execute(sasi_idx_cql.toString());
						
						sasi_idx_cql = new StringBuffer();
						sasi_idx_cql.append(" CREATE CUSTOM INDEX IF NOT EXISTS " + StorageConstants.TAG_ARLAM_TABLE  + "_T_IDX_ASSET_NAME on " + ta_key + " (" + CassandraColumns.ASSET_NAME + ") " + sasi_option.toString());
						session.execute(sasi_idx_cql.toString());
						
						sasi_idx_cql = new StringBuffer();
						sasi_idx_cql.append(" CREATE CUSTOM INDEX IF NOT EXISTS " + StorageConstants.TAG_ARLAM_TABLE  + "_T_IDX_SITE_NAME on " + ta_key + " (" + CassandraColumns.SITE_NAME  + ") " + sasi_option.toString());
						session.execute(sasi_idx_cql.toString());
						
						sasi_idx_cql = new StringBuffer();
						sasi_idx_cql.append(" CREATE CUSTOM INDEX IF NOT EXISTS " + StorageConstants.TAG_ARLAM_TABLE  + "_T_IDX_OPC_NAME on " + ta_key + " (" + CassandraColumns.OPC_NAME  + ") " + sasi_option.toString());
						session.execute(sasi_idx_cql.toString());
						
						sasi_idx_cql = new StringBuffer();
						sasi_idx_cql.append(" CREATE CUSTOM INDEX IF NOT EXISTS " + StorageConstants.TAG_ARLAM_TABLE  + "_T_IDX_TAG_LOCATION on " + ta_key + " (TAG_LOCATION) " + sasi_option.toString());
						session.execute(sasi_idx_cql.toString());
						
						
						//
						
						log.info("Cassandra table [" + ta_key + "] is created.");
					} catch (Exception ex) {
						log.error("Cassandra table and index create failed : " + ex.getMessage(), ex);
					} finally {
						// ;
					}
				
				
				//알람 온 테이블(현재 알람)
				String tac_key = keyspace + "." + StorageConstants.TAG_ARLAM_ON_TABLE;
				

					try {

						
						StringBuffer createCA = new StringBuffer();
						createCA.append("CREATE TABLE IF NOT EXISTS " + tac_key + " ");
						createCA.append(" (  ");
						createCA.append("    " + CassandraColumns.TAG_ID + "    varchar,  ");
						createCA.append("    " + CassandraColumns.ASSET_ID + "  varchar,  ");
						createCA.append("    " + CassandraColumns.TIMESTAMP + " timestamp,  ");
						createCA.append("    " + CassandraColumns.PRIORITY + "  varchar,  ");
						createCA.append("    " + CassandraColumns.DESCRIPTION + " text,  ");
						createCA.append("    " + CassandraColumns.ALARM_SEQ + "   int,  ");
						createCA.append("    " + CassandraColumns.ALARM_CONFIG_ID + " varchar,  ");
						createCA.append("	 " + CassandraColumns.ATTRIBUTE + "  map<varchar, varchar>,   ");
						createCA.append("    PRIMARY KEY (" + CassandraColumns.TAG_ID + ")  ");
						createCA.append(" ) ; ");

						session.execute(createCA.toString());
						

						StringBuffer createCDX1 = new StringBuffer();
						createCDX1.append(" CREATE INDEX IF NOT EXISTS " + StorageConstants.TAG_ARLAM_ON_TABLE  + "_T_IDX_ALARM_CONFIG_ID on " + tac_key + " (" + CassandraColumns.ALARM_CONFIG_ID + "); ");
						session.execute(createCDX1.toString());

						StringBuffer createCDX2 = new StringBuffer();
						createCDX2.append(" CREATE INDEX IF NOT EXISTS " + StorageConstants.TAG_ARLAM_ON_TABLE  + "_T_IDX_PRIORITY on " + tac_key + " (" + CassandraColumns.PRIORITY + "); ");
						session.execute(createCDX2.toString());
						
						StringBuffer createCDX3 = new StringBuffer();
						createCDX3.append(" CREATE INDEX IF NOT EXISTS " + StorageConstants.TAG_ARLAM_ON_TABLE  + "_T_IDX_ASSET_ID on " + tac_key + " (" + CassandraColumns.ASSET_ID + "); ");
						session.execute(createCDX3.toString());

						//
						log.info("Cassandra table [" + tac_key + "] is created.");
					} catch (Exception ex) {
						log.error("Cassandra table and index create failed : " + ex.getMessage(), ex);
					} finally {
						// ;
					}
				
				// 알람 카운트 테이블
				String tcc_key = keyspace + "." + StorageConstants.TAG_ARLAM_COUNT_TABLE;
				
					try {

						
						StringBuffer createCT = new StringBuffer();
						createCT.append("CREATE TABLE IF NOT EXISTS " + tcc_key + " ( ");
						createCT.append("		" + CassandraColumns.TAG_ID + " varchar, ");
						createCT.append("		" + CassandraColumns.YEAR + " int,");
						createCT.append("		" + CassandraColumns.MONTH + " int,");
						createCT.append("		" + CassandraColumns.DAY + " int,");
						createCT.append("		" + CassandraColumns.HOUR + " int,");
						createCT.append("		" + CassandraColumns.MINUTE + " int,");
						createCT.append("		" + CassandraColumns.ALARM_COUNT + " counter,");
						createCT.append("		" + CassandraColumns.ALARM_COUNT_BY_INFO + " counter,");
						createCT.append("		" + CassandraColumns.ALARM_COUNT_BY_WARN + " counter,");
						createCT.append("		" + CassandraColumns.ALARM_COUNT_BY_ERROR + " counter,");
						createCT.append("		primary key((" + CassandraColumns.TAG_ID + "), " + CassandraColumns.YEAR + ", " + CassandraColumns.MONTH + ", " + CassandraColumns.DAY + ", "
								+ CassandraColumns.HOUR + ", " + CassandraColumns.MINUTE + ")");
						createCT.append("		) WITH CLUSTERING ORDER BY (" + CassandraColumns.YEAR + " desc, " + CassandraColumns.MONTH + " desc, " + CassandraColumns.DAY + " desc, "
								+ CassandraColumns.HOUR + " desc, " + CassandraColumns.MINUTE + " desc); ");
						session.execute(createCT.toString());

						//
						log.info("Cassandra table [" + tcc_key + "] is created.");
					} catch (Exception ex) {
						log.error("Cassandra table and index create failed : " + ex.getMessage(), ex);
					} finally {
						// ;
					}
			

				
				
		} catch (Exception ex) {
			log.error("Cassandra keyspace and table create failed : " + ex.getMessage(), ex);
		} finally {
			//
		}
	};
	
	

	@Override
	public void createTableForBLOB() throws Exception {
	
		Session session = null;
		//
		try {
	
			//
			session     = CassandraSessionFixer.getSession();
			String keyspace    = CassandraSessionFixer.getKeyspace();
			
			//			
			StringBuffer createCF = new StringBuffer();
			createCF.append(" CREATE TABLE IF NOT EXISTS " + StorageConstants.TAG_BLOB_TABLE + " ( ");
			createCF.append("		  " + CassandraColumns.TAG_ID    + "   varchar,  ");
			createCF.append("		  " + CassandraColumns.TIMESTAMP + "   timestamp,  ");
			createCF.append("		  " + CassandraColumns.OBJECT_ID + "   uuid,   ");
			createCF.append("		  " + CassandraColumns.FILE_PATH + "   text,   ");
			createCF.append("		  " + CassandraColumns.FILE_SIZE + "   bigint,   ");
			createCF.append("		  " + CassandraColumns.MIME_TYPE + "   text,   ");
			createCF.append("		  " + CassandraColumns.ATTRIBUTE + "  map<varchar, varchar>,   ");
			createCF.append("		  PRIMARY KEY (" + CassandraColumns.TAG_ID + ",  " + CassandraColumns.TIMESTAMP + ")  ");
			createCF.append("		)  WITH CLUSTERING ORDER BY (" + CassandraColumns.TIMESTAMP + " DESC)  ");
			createCF.append("     AND compaction = { 'class': 'TimeWindowCompactionStrategy', 'compaction_window_size': '1', 'compaction_window_unit': 'DAYS', 'timestamp_resolution': 'MILLISECONDS', 'max_threshold': '32', 'min_threshold': '4', 'unchecked_tombstone_compaction' : 'true' }");
			createCF.append("     AND caching = {'keys':'ALL', 'rows_per_partition':'NONE'} ");
			createCF.append("     AND compression = { 'class' : 'ZstdCompressor',  'chunk_length_in_kb': '16', 'compression_level': '" +  ZSTD_COMPRESSION_LEVEL + "' } ");
			createCF.append("     AND bloom_filter_fp_chance = 0.01    ");
			createCF.append("     AND default_time_to_live =  " + StorageTTL.TAG_BLOB_TABLE_TTL);
			createCF.append("     AND gc_grace_seconds = 604800 ");
			createCF.append("     AND max_index_interval = 2048 ");
			createCF.append("     AND memtable_flush_period_in_ms = 0 ");
			createCF.append("     AND min_index_interval = 128 ");
			createCF.append("     AND speculative_retry = 'NONE';  ");
			session.execute(createCF.toString());
			
			StringBuffer c_idx_cql = new StringBuffer();
			c_idx_cql.append(" CREATE INDEX IF NOT EXISTS " + StorageConstants.TAG_BLOB_TABLE  + "_IDX_OBJECT_ID ON " +  keyspace + "." + StorageConstants.TAG_BLOB_TABLE + " (" + CassandraColumns.OBJECT_ID + ") ");
			session.execute(c_idx_cql.toString());
			
			c_idx_cql = new StringBuffer();
			c_idx_cql.append(" CREATE INDEX IF NOT EXISTS " + StorageConstants.TAG_BLOB_TABLE  + "_IDX_ATTRIBUTE ON " +  keyspace + "." + StorageConstants.TAG_BLOB_TABLE + " (ENTRIES(" + CassandraColumns.ATTRIBUTE + ")) ");
			session.execute(c_idx_cql.toString());
			
			// 
			log.info("Cassandra table [" + keyspace + "." + StorageConstants.TAG_BLOB_TABLE  + "] is created.");
			ObjectCreationChecker.getInstance().setObject(StorageConstants.TAG_BLOB_TABLE , true);
						
			
			StringBuffer cql = new StringBuffer();
			cql.append(" CREATE TABLE IF NOT EXISTS " + StorageConstants.TAG_BLOB_OBJECT_TABLE + " ( ");
			cql.append("		  " + CassandraColumns.OBJECT_ID    + "   uuid,  ");
			cql.append("		  " + CassandraColumns.OBJECT  + "        blob,   ");
			cql.append("		  PRIMARY KEY (" + CassandraColumns.OBJECT_ID + ")  ");
			cql.append("		) WITH compaction =  { 'class' : 'SizeTieredCompactionStrategy' }  ");
			cql.append("     AND caching = {'keys':'ALL', 'rows_per_partition':'NONE'} ");
			cql.append("     AND default_time_to_live =  " + StorageTTL.TAG_BLOB_TABLE_TTL);
			cql.append("     AND gc_grace_seconds = 604800 ");
			cql.append("     AND speculative_retry = 'NONE';  ");
			session.execute(cql.toString());
			
			
			log.info("Cassandra table [" + keyspace + "." + StorageConstants.TAG_BLOB_OBJECT_TABLE  + "] is created.");
			ObjectCreationChecker.getInstance().setObject(StorageConstants.TAG_BLOB_OBJECT_TABLE , true);
			
			//
		} catch (Exception ex) {
			log.error("[" + StorageConstants.TAG_BLOB_TABLE  + "] create error : " + ex.getMessage(), ex);
			throw ex;
		} finally {
			//
			// if (session != null && !session.isClosed())
			// session.close();
		}
	
	}

	
	@Override
	public void createTableForSampling() throws Exception {

		Session session = null;
		String sampling_table = "";
		//
		try {

			//
			session     = CassandraSessionFixer.getSession();
			String keyspace    = CassandraSessionFixer.getKeyspace();
			
			//
			for (int i = 0; i < StorageConstants.SAMPLING_TERMS_ARRAY.length; i++) {
				//
				sampling_table = StorageConstants.TAG_POINT_SAMPLING_TABLE + "_" + StorageConstants.SAMPLING_TERMS_ARRAY[i].replace(" ", "_").toLowerCase();
				//
				if (!ObjectCreationChecker.getInstance().hasObject(sampling_table)) {
					//
					StringBuffer createCF = new StringBuffer();
					createCF.append(" CREATE TABLE IF NOT EXISTS " + keyspace + "." + sampling_table + " ( ");
					createCF.append("		  " + CassandraColumns.TAG_ID + "     varchar,  ");
					createCF.append("		  " + CassandraColumns.TIMESTAMP + "  timestamp,  ");
					createCF.append("		  " + CassandraColumns.VALUE + "      varchar,  ");
					createCF.append("		  PRIMARY KEY (" + CassandraColumns.TAG_ID + ", " + CassandraColumns.TIMESTAMP + ")  ");
					createCF.append("		)  WITH CLUSTERING ORDER BY (" + CassandraColumns.TIMESTAMP + " DESC)  ");
					createCF.append("     AND compaction = { 'class': 'TimeWindowCompactionStrategy', 'compaction_window_size': '21',  'compaction_window_unit': 'DAYS', 'max_threshold': '32', 'min_threshold': '4', 'unchecked_tombstone_compaction' : 'true' } ");
					createCF.append("     AND  caching   = {'keys':'ALL', 'rows_per_partition':'NONE'} ");
					createCF.append("     AND compression = { 'class' : 'ZstdCompressor', 'compression_level': '" +  ZSTD_COMPRESSION_LEVEL + "' } ");
					createCF.append("     AND default_time_to_live =  " +  StorageTTL.TAG_POINT_SAMPLING_TABLE_TTL);
					createCF.append("     AND gc_grace_seconds = 10800 ");
					createCF.append("     AND max_index_interval = 2048 ");
					createCF.append("     AND memtable_flush_period_in_ms = 0 ");
					createCF.append("     AND min_index_interval = 128 ");
					createCF.append("     AND speculative_retry = 'NONE';  ");
					session.execute(createCF.toString());
				
					// 
					log.info("Cassandra table [" + keyspace + "." + sampling_table + "] is created.");
					ObjectCreationChecker.getInstance().setObject(sampling_table, true);
				}
			}
			//
		} catch (Exception ex) {
			log.error("[" + sampling_table + "] create error : " + ex.getMessage(), ex);
			throw ex;
		} finally {
			//
			// if (session != null && !session.isClosed())
			// session.close();
		}

	}
	


	@Override
	public void createTableForAggregation() throws Exception {

		Session session = null;
		String agg_table = "";
		//
		try {

			//
			session     = CassandraSessionFixer.getSession();
			String keyspace    = CassandraSessionFixer.getKeyspace();

			for (int i = 0; i < StorageConstants.AGGREGATION_TERMS_ARRAY.length; i++) {
				//
				agg_table = StorageConstants.TAG_POINT_AGGREGATION_TABLE + "_" + StorageConstants.AGGREGATION_TERMS_ARRAY[i].replace(" ", "_").toLowerCase();
				//
				if (!ObjectCreationChecker.getInstance().hasObject(agg_table)) {
					//
					StringBuffer createCF = new StringBuffer();
					createCF.append(" CREATE TABLE IF NOT EXISTS " + keyspace + "." + agg_table + " ( ");
					createCF.append("		  " + CassandraColumns.TAG_ID + "     varchar,  ");
					createCF.append("		  " + CassandraColumns.TIMESTAMP + "  timestamp,  ");
					createCF.append("		  " + CassandraColumns.COUNT + "      int,  ");
					createCF.append("		  " + CassandraColumns.FIRST + "      varchar,  ");
					createCF.append("		  " + CassandraColumns.LAST + "       varchar,  ");
					createCF.append("		  " + CassandraColumns.AVG + "        double,  ");
					createCF.append("		  " + CassandraColumns.MIN + "        double,  ");
					createCF.append("		  " + CassandraColumns.MAX + "        double,  ");
					createCF.append("		  " + CassandraColumns.SUM + "        double,  ");
					createCF.append("		  " + CassandraColumns.STDDEV + "     double,  ");
					createCF.append("		  " + CassandraColumns.MEDIAN + "     double,  ");
					createCF.append("		  PRIMARY KEY (" + CassandraColumns.TAG_ID + ", " + CassandraColumns.TIMESTAMP + ")  ");
					createCF.append("		)  WITH CLUSTERING ORDER BY (" + CassandraColumns.TIMESTAMP + " DESC)  ");
					createCF.append("     AND compaction = { 'class': 'TimeWindowCompactionStrategy', 'compaction_window_size': '21',  'compaction_window_unit': 'DAYS', 'max_threshold': '32', 'min_threshold': '4', 'unchecked_tombstone_compaction' : 'true' } ");
					createCF.append("     AND  caching= {'keys':'ALL', 'rows_per_partition':'NONE'} ");
					createCF.append("     AND compression = { 'class' : 'ZstdCompressor',  'compression_level': '" +  ZSTD_COMPRESSION_LEVEL + "'} ");
					createCF.append("     AND default_time_to_live =  " + StorageTTL.TAG_POINT_AGGREGATION_TTL);
					createCF.append("     AND gc_grace_seconds = 10800 ");
					createCF.append("     AND max_index_interval = 2048 ");
					createCF.append("     AND memtable_flush_period_in_ms = 0 ");
					createCF.append("     AND min_index_interval = 128 ");
					createCF.append("     AND speculative_retry = 'NONE';  ");
					session.execute(createCF.toString());
					
					

					// 
					log.info("Cassandra table [" + keyspace + "." + agg_table + "] is created.");
					ObjectCreationChecker.getInstance().setObject(agg_table, true);
				}
			}
			//
		} catch (Exception ex) {
			log.error("[" + agg_table + "] create error : " + ex.getMessage(), ex);
			throw ex;
		} finally {
			//
			// if (session != null && !session.isClosed())
			// session.close();
		}

	}

	@Override
	public void createTableForSnapshot() throws Exception {

		Session session = null;
		String snapshot_table = "";
		//
		try {

			//
			session     = CassandraSessionFixer.getSession();
			String keyspace    = CassandraSessionFixer.getKeyspace();

			// 
			snapshot_table = StorageConstants.TAG_POINT_SNAPSHOT_TABLE;
			//
			if (!ObjectCreationChecker.getInstance().hasObject(snapshot_table)) {
				//
				StringBuffer createCF = new StringBuffer();
				createCF.append(" CREATE TABLE IF NOT EXISTS " + keyspace + "." + snapshot_table + " ( ");
				createCF.append("		  " + CassandraColumns.SITE_ID      + "  varchar,  ");
				createCF.append("		  " + CassandraColumns.SNAPSHOT_ID  + "  varchar,  ");
				createCF.append("		  " + CassandraColumns.DATE         + "  int,  ");
				createCF.append("		  " + CassandraColumns.HOUR         + "  int,  ");
				createCF.append("		  " + CassandraColumns.TIMESTAMP    + "  timestamp,  ");
				createCF.append("		  " + CassandraColumns.DATA_MAP + "        frozen<map<varchar, varchar>>,  ");
				createCF.append("		  " + CassandraColumns.TIMESTAMP_MAP + "   frozen<map<varchar, bigint>>,  ");
				createCF.append("		  " + CassandraColumns.ALARM_MAP + "       frozen<map<varchar, int>>,   ");
				createCF.append("		  PRIMARY KEY ((" + CassandraColumns.SITE_ID  + ", " + CassandraColumns.SNAPSHOT_ID + ", "+ CassandraColumns.DATE + "), " + CassandraColumns.TIMESTAMP + ")  ");
				createCF.append("		)  WITH  CLUSTERING ORDER BY (" + CassandraColumns.TIMESTAMP + " DESC) ");
				createCF.append("     AND compaction = { 'class': 'TimeWindowCompactionStrategy', 'compaction_window_size': '24',  'compaction_window_unit': 'HOURS', 'max_threshold': '32', 'min_threshold': '4', 'unchecked_tombstone_compaction' : 'true' } ");
				createCF.append("     AND  caching= {'keys':'ALL', 'rows_per_partition':'NONE'} ");
				createCF.append("     AND compression = { 'class' : 'ZstdCompressor',  'chunk_length_in_kb': '16', 'compression_level': '" +  ZSTD_COMPRESSION_LEVEL + "'} ");
				createCF.append("     AND default_time_to_live = "  + StorageTTL.TAG_POINT_SNAPSHOT_TABLE_TTL);
				createCF.append("     AND gc_grace_seconds = 600 ");
				createCF.append("     AND max_index_interval = 2048 ");
				createCF.append("     AND memtable_flush_period_in_ms = 0 ");
				createCF.append("     AND min_index_interval = 128 ");
				createCF.append("     AND speculative_retry = 'NONE';  ");
				session.execute(createCF.toString());
				
				
				//SASI SPARSE INDEX
				/*
				StringBuffer idx = new StringBuffer();
				idx.append(" CREATE CUSTOM INDEX IF NOT EXISTS " + StorageConstants.TAG_POINT_SNAPSHOT_TABLE  + "_T_TIMESTAMP ");
				idx.append(" ON " + keyspace + "." + StorageConstants.TAG_POINT_SNAPSHOT_TABLE + " (" + CassandraColumns.TIMESTAMP + ")  ");
				idx.append(" USING 'org.apache.cassandra.index.sasi.SASIIndex'  ");
				idx.append(" WITH OPTIONS = { 'mode': 'SPARSE' };  ");
				session.execute(idx.toString());
				*/
				

				//
				log.info("Cassandra table [" + keyspace + "." + snapshot_table + "] is created.");
				ObjectCreationChecker.getInstance().setObject(snapshot_table, true);
			}
			// }
			//
		} catch (Exception ex) {
			log.error("[" + snapshot_table + "] create error : " + ex.getMessage(), ex);
			throw ex;
		} finally {
			//
			// if (session != null && !session.isClosed())
			// session.close();
		}

	}
	
	
	@Override
	public void createTableForArchive() throws Exception {

		Session session = null;
		//
		try {

			//
			session     = CassandraSessionFixer.getSession();
			String keyspace    = CassandraSessionFixer.getKeyspace();

			// 
			//
			StringBuffer cql = new StringBuffer();
			
			// --------------------------------------------------------------------
			// 포인트 아카이브 테이블 생성
			// --------------------------------------------------------------------
			cql = new StringBuffer();
			cql.append(" CREATE TABLE IF NOT EXISTS " + keyspace + "." + StorageConstants.TAG_POINT_ARCHIVE_TABLE + " ( ");
			cql.append("		  " + CassandraColumns.TAG_ID + "  varchar,  ");
			cql.append("		  " + CassandraColumns.DATE   + "  int,      ");
			cql.append("		  " + CassandraColumns.HOUR   + "  int,      ");
			for(int i=0;i<60;i++) { //60분 포인트 JSON 리스트 데이터 저장
				cql.append("	" + "M_" + (String.format("%02d", i))  + "  frozen<list<text>>,     ");
			};
			cql.append("		  " + CassandraColumns.ATTRIBUTE   + "  frozen<map<text,text>>,      ");
			cql.append("		  PRIMARY KEY (" + CassandraColumns.TAG_ID + ", " + CassandraColumns.DATE + ", " + CassandraColumns.HOUR + ")  ");
			cql.append("		)  WITH  CLUSTERING ORDER BY (" + CassandraColumns.DATE + " DESC, " + CassandraColumns.HOUR + " DESC) ");
			cql.append("     AND compaction = { 'class': 'TimeWindowCompactionStrategy', 'compaction_window_size': '24',  'compaction_window_unit': 'HOURS', 'max_threshold': '32', 'min_threshold': '4', 'unchecked_tombstone_compaction' : 'true' } ");
			cql.append("     AND  caching= {'keys':'ALL', 'rows_per_partition':'NONE'} ");
			cql.append("     AND compression = { 'class' : 'ZstdCompressor', 'chunk_length_in_kb': '16', 'compression_level': '" +  ZSTD_COMPRESSION_LEVEL + "'} ");
			cql.append("     AND default_time_to_live = "  + StorageTTL.TAG_POINT_ARCHIVE_TABLE_TTL);
			cql.append("     AND gc_grace_seconds   = 60 ");
			cql.append("     AND max_index_interval = 2048 ");
			cql.append("     AND memtable_flush_period_in_ms = 0 ");
			cql.append("     AND min_index_interval = 128 ");
			cql.append("     AND speculative_retry = 'NONE';  ");
			session.execute(cql.toString());
			
			//
			log.info("Cassandra table [" + keyspace + "." + StorageConstants.TAG_POINT_ARCHIVE_TABLE + "] is created.");
			
			
            //카운터
			cql = new StringBuffer();
			cql.append("CREATE TABLE IF NOT EXISTS " + keyspace + "." + StorageConstants.TAG_POINT_ARCHIVE_TABLE  + "_count ");
			cql.append(" (  ");
			cql.append("    " + CassandraColumns.TAG_ID + " varchar,  ");
			cql.append("    " + CassandraColumns.DATE   + " int,  ");
			cql.append("    " + CassandraColumns.HOUR   + " int,  ");
			cql.append("    " + CassandraColumns.COUNT  + " counter,  ");
			cql.append("    PRIMARY KEY (" + CassandraColumns.TAG_ID + ", " + CassandraColumns.DATE + ", " + CassandraColumns.HOUR  + ")  ");
			cql.append(" ) WITH  CLUSTERING ORDER BY (" + CassandraColumns.DATE + " DESC, " + CassandraColumns.HOUR + " DESC ) ");
			session.execute(cql.toString());
			
			log.info("Cassandra table [" + keyspace + "." + StorageConstants.TAG_POINT_ARCHIVE_TABLE + "_COUNT] is created.");
			
			
			//
			cql = new StringBuffer();
			cql.append(" CREATE TABLE IF NOT EXISTS " + keyspace + "." + StorageConstants.TAG_POINT_ARCHIVE_TABLE + "_daily ( ");
			cql.append("		  " + CassandraColumns.TAG_ID + "  varchar,  ");
			cql.append("		  " + CassandraColumns.DATE   + "  int,      ");
			cql.append("		  " + CassandraColumns.COUNT  + "  int,      ");
			for(int i=0;i<24;i++) { //24시간 포인트 JSON 리스트 데이터 저장
				cql.append("		  " + "H_" + (String.format("%02d", i))  + "  list<text>,     ");
			};
			cql.append("		  PRIMARY KEY (" + CassandraColumns.TAG_ID + ", " + CassandraColumns.DATE  + " )  ");
			cql.append("		)  WITH  CLUSTERING ORDER BY (" + CassandraColumns.DATE + " DESC " + " ) ");
			cql.append("     AND compaction = { 'class': 'TimeWindowCompactionStrategy', 'compaction_window_size': '24',  'compaction_window_unit': 'HOURS', 'max_threshold': '32', 'min_threshold': '4' } ");
			cql.append("     AND  caching= {'keys':'ALL', 'rows_per_partition':'NONE'} ");
			cql.append("     AND compression = { 'class' : 'ZstdCompressor',  'compression_level': '" +  ZSTD_COMPRESSION_LEVEL + "'} ");
			cql.append("     AND default_time_to_live = "  + StorageTTL.TAG_POINT_ARCHIVE_TABLE_TTL);
			cql.append("     AND gc_grace_seconds = 86500 ");
			cql.append("     AND max_index_interval = 2048 ");
			cql.append("     AND memtable_flush_period_in_ms = 0 ");
			cql.append("     AND min_index_interval = 128 ");
			cql.append("     AND speculative_retry = 'NONE';  ");
			session.execute(cql.toString());
			
			//
			log.info("Cassandra table [" + keyspace + "." + StorageConstants.TAG_POINT_ARCHIVE_TABLE + "_DAILY] is created.");
		
		} catch (Exception ex) {
			log.error("[" + StorageConstants.TAG_POINT_ARCHIVE_TABLE + "_DAILY] create error : " + ex.getMessage(), ex);
			throw ex;
		} finally {
			//
		}

	};
	
	
	@Override
	public void createTableForAssetTimeline() throws Exception {
		Session session = null;
		String asset_table = "";
		//
		try {

			//
			session     = CassandraSessionFixer.getSession();
			String keyspace    = CassandraSessionFixer.getKeyspace();

			// 
			asset_table = StorageConstants.ASSET_TIMELINE_TABLE;
			//
			if (!ObjectCreationChecker.getInstance().hasObject(asset_table)) {
				//
				StringBuffer cql = new StringBuffer();
				cql.append(" CREATE TABLE IF NOT EXISTS " + keyspace + "." + asset_table  + " ( ");
				cql.append("	" + CassandraColumns.ASSET_ID   +    " varchar, ");
				cql.append("	" + CassandraColumns.TIMESTAMP  +    " timestamp, ");
				cql.append("	" + CassandraColumns.TIMELINE_TYPE + " varchar, ");
				cql.append("	" + CassandraColumns.PAYLOAD + " varchar, ");
				cql.append("	PRIMARY KEY (" + CassandraColumns.ASSET_ID + ", "  + CassandraColumns.TIMESTAMP  + ")  ");
				cql.append("	 )  ");
				cql.append("	WITH clustering order by (" +  CassandraColumns.TIMESTAMP + " DESC) ");
				cql.append("     AND compaction = { 'class': 'TimeWindowCompactionStrategy', 'compaction_window_size': '28',  'compaction_window_unit': 'DAYS', 'max_threshold': '32', 'min_threshold': '4' } ");
				cql.append("     AND caching= {'keys':'ALL', 'rows_per_partition':'NONE'} ");
				cql.append("     AND compression = { 'class' : 'ZstdCompressor',  'compression_level': '" +  ZSTD_COMPRESSION_LEVEL + "' } ");
				cql.append("     AND default_time_to_live = 0 ");
				cql.append("     AND gc_grace_seconds = 864000 ");
				cql.append("     AND max_index_interval = 2048 ");
				cql.append("     AND memtable_flush_period_in_ms = 0 ");
				cql.append("     AND min_index_interval = 128 ");
				cql.append("     AND speculative_retry = 'NONE';  ");
				session.execute(cql.toString());
				
				//TODO V6.0 추가 CQL 스테이트먼트 정보
				try {
				session.execute(" ALTER TABLE  " + keyspace + "." + asset_table  + " ADD  "  + CassandraColumns.STATEMENT_NAME  + "  text  ");
				session.execute(" ALTER TABLE  " + keyspace + "." + asset_table  + " ADD  "  + CassandraColumns.STATEMENT_DESCRIPTION  + "  text  ");
				}catch(Exception ex) {
					
				}

				StringBuffer createIDX = new StringBuffer();
				createIDX.append(" CREATE INDEX IF NOT EXISTS " + StorageConstants.ASSET_TIMELINE_TABLE  + "_T_IDX_TIMELINE_TYPE on " + keyspace + "." + StorageConstants.ASSET_TIMELINE_TABLE + " (" + CassandraColumns.TIMELINE_TYPE + ") ");
				session.execute(createIDX.toString());

				// 
				log.info("Cassandra table [" + keyspace + "." + asset_table + "] is created.");
				ObjectCreationChecker.getInstance().setObject(asset_table, true);
			}
			// }
			//
		} catch (Exception ex) {
			log.error("[" + asset_table + "] create error : " + ex.getMessage(), ex);
			throw ex;
		} finally {
			//
			// if (session != null && !session.isClosed())
			// session.close();
		}
		
	}
	
	@Override
	public void createTableForAssetData() throws Exception {
		Session session = null;
		String asset_table = "";
		//
		try {

			//
			session     = CassandraSessionFixer.getSession();
			String keyspace    = CassandraSessionFixer.getKeyspace();

			// 
			asset_table = StorageConstants.ASSET_DATA_TABLE;
			//
			if (!ObjectCreationChecker.getInstance().hasObject(asset_table)) {
				
				//기본 1초/5초/10초/30초 간격 (설정값)
				StringBuffer cql = new StringBuffer();
				cql.append(" CREATE TABLE IF NOT EXISTS " + keyspace + "." + asset_table  + " ( ");
				cql.append("	" + CassandraColumns.ASSET_ID   + " varchar, ");
				cql.append("	" + CassandraColumns.ASSET_TYPE + " varchar, ");
				cql.append("	" + CassandraColumns.DATE       + " int, ");
				cql.append("	" + CassandraColumns.HOUR       + " int, ");
				cql.append("	" + CassandraColumns.MINUTE     + " int, ");
				cql.append("	" + CassandraColumns.TIMESTAMP  + " timestamp, ");
				cql.append("	" + CassandraColumns.DATA_MAP   + " frozen<map<varchar, varchar>>, ");
				cql.append("	" + CassandraColumns.TIMESTAMP_MAP   + " frozen<map<varchar, bigint>>, ");
				cql.append("	PRIMARY KEY ((" + CassandraColumns.ASSET_ID + "," + CassandraColumns.DATE  + "," + CassandraColumns.HOUR  + "), " + CassandraColumns.MINUTE  + ","  + CassandraColumns.TIMESTAMP  + ")  ");
				cql.append("	 )  ");
				cql.append("	WITH clustering order by (" + CassandraColumns.MINUTE + " DESC, " +  CassandraColumns.TIMESTAMP + " DESC) ");
				cql.append("     AND compaction = { 'class': 'TimeWindowCompactionStrategy', 'compaction_window_size': '24',  'compaction_window_unit': 'HOURS', 'min_threshold': '4' , 'max_threshold': '32',  'unchecked_tombstone_compaction' : 'true' } ");
				cql.append("     AND caching= {'keys':'ALL', 'rows_per_partition':'NONE'} ");
				cql.append("     AND compression = { 'class' : 'ZstdCompressor', 'chunk_length_in_kb': '16', 'compression_level': '" +  ZSTD_COMPRESSION_LEVEL + "' } ");
				cql.append("     AND default_time_to_live =  " + StorageTTL.TM_ASSET_DATA_TABLE_TTL);
				cql.append("     AND gc_grace_seconds   = 3600 ");
				cql.append("     AND max_index_interval = 2048 ");
				cql.append("     AND memtable_flush_period_in_ms = 0 ");
				cql.append("     AND min_index_interval = 128 ");
				cql.append("     AND speculative_retry = 'NONE';  ");
				session.execute(cql.toString());
				
				
				//샘플링 1분 텀
				cql = new StringBuffer();
				cql.append(" CREATE TABLE IF NOT EXISTS " + keyspace + "." + asset_table  + "_SAMPLING_" + "1_MINUTES ( ");
				cql.append("	" + CassandraColumns.ASSET_ID   + " varchar, ");
				cql.append("	" + CassandraColumns.ASSET_TYPE + " varchar, ");
				cql.append("	" + CassandraColumns.DATE       + " int, ");
				cql.append("	" + CassandraColumns.HOUR       + " int, ");
				cql.append("	" + CassandraColumns.MINUTE     + " int, ");
				cql.append("	" + CassandraColumns.TIMESTAMP  + " timestamp, ");
				cql.append("	" + CassandraColumns.DATA_MAP   + " frozen<map<varchar, varchar>>, ");
				cql.append("	" + CassandraColumns.TIMESTAMP_MAP   + " frozen<map<varchar, bigint>>, ");
				cql.append("	PRIMARY KEY ((" + CassandraColumns.ASSET_ID + "," + CassandraColumns.DATE  + "), " + CassandraColumns.HOUR  + ", " + CassandraColumns.MINUTE + ", "   + CassandraColumns.TIMESTAMP  + ")  ");
				cql.append("	 )  ");
				cql.append("	WITH clustering order by (" + CassandraColumns.HOUR  + " DESC, " + CassandraColumns.MINUTE + " DESC, " + CassandraColumns.TIMESTAMP + " DESC) ");
				cql.append("     AND compaction = { 'class': 'TimeWindowCompactionStrategy', 'compaction_window_size': '7',  'compaction_window_unit': 'DAYS', 'min_threshold': '4' , 'max_threshold': '32',  'unchecked_tombstone_compaction' : 'true' } ");
				cql.append("     AND caching= {'keys':'ALL', 'rows_per_partition':'NONE'} ");
				cql.append("     AND compression = { 'class' : 'ZstdCompressor', 'chunk_length_in_kb': '16', 'compression_level': '" +  ZSTD_COMPRESSION_LEVEL + "' } ");
				cql.append("     AND default_time_to_live =  " + StorageTTL.TM_ASSET_DATA_TABLE_TTL);
				cql.append("     AND gc_grace_seconds   = 3600 ");
				cql.append("     AND max_index_interval = 2048 ");
				cql.append("     AND memtable_flush_period_in_ms = 0 ");
				cql.append("     AND min_index_interval = 128 ");
				cql.append("     AND speculative_retry = 'NONE';  ");
				session.execute(cql.toString());
				
				//샘플링 1시간 텀
				cql = new StringBuffer();
				cql.append(" CREATE TABLE IF NOT EXISTS " + keyspace + "." + asset_table  + "_SAMPLING_" + "1_HOURS ( ");
				cql.append("	" + CassandraColumns.ASSET_ID   + " varchar, ");
				cql.append("	" + CassandraColumns.ASSET_TYPE + " varchar, ");
				cql.append("	" + CassandraColumns.DATE       + " int, ");
				cql.append("	" + CassandraColumns.HOUR       + " int, ");
				cql.append("	" + CassandraColumns.MINUTE     + " int, ");
				cql.append("	" + CassandraColumns.TIMESTAMP  + " timestamp, ");
				cql.append("	" + CassandraColumns.DATA_MAP   + " frozen<map<varchar, varchar>>, ");
				cql.append("	" + CassandraColumns.TIMESTAMP_MAP   + " frozen<map<varchar, bigint>>, ");
				cql.append("	PRIMARY KEY ((" + CassandraColumns.ASSET_ID + "," + CassandraColumns.DATE  + "), " + CassandraColumns.HOUR  + ", " + CassandraColumns.MINUTE + ", "  + CassandraColumns.TIMESTAMP  + ")  ");
				cql.append("	 )  ");
				cql.append("	WITH clustering order by (" + CassandraColumns.HOUR  + " DESC, " + CassandraColumns.MINUTE + " DESC, " + CassandraColumns.TIMESTAMP + " DESC) ");
				cql.append("     AND compaction = { 'class': 'TimeWindowCompactionStrategy', 'compaction_window_size': '31',  'compaction_window_unit': 'DAYS', 'min_threshold': '4', 'max_threshold': '32', 'unchecked_tombstone_compaction' : 'true' } ");
				cql.append("     AND caching= {'keys':'ALL', 'rows_per_partition':'NONE'} ");
				cql.append("     AND compression = { 'class' : 'ZstdCompressor', 'chunk_length_in_kb': '16', 'compression_level': '" +  ZSTD_COMPRESSION_LEVEL + "' } ");
				cql.append("     AND default_time_to_live =  " + StorageTTL.TM_ASSET_DATA_TABLE_TTL);
				cql.append("     AND gc_grace_seconds   = 3600 ");
				cql.append("     AND max_index_interval = 2048 ");
				cql.append("     AND memtable_flush_period_in_ms = 0 ");
				cql.append("     AND min_index_interval = 128 ");
				cql.append("     AND speculative_retry = 'NONE';  ");
				session.execute(cql.toString());
				
      
				//StringBuffer createIDX = new StringBuffer();
				//createIDX.append(" CREATE INDEX IF NOT EXISTS " + StorageConstants.ASSET_DATA_TABLE  + "_T_IDX_ASSET_TYPE on " + keyspace + "." + StorageConstants.ASSET_DATA_TABLE + " (" + CassandraColumns.ASSET_TYPE + ") ");
				//session.execute(createIDX.toString());
				
				//SPARSE INDEX
				/*
				StringBuffer idx = new StringBuffer();
				idx.append(" CREATE CUSTOM INDEX IF NOT EXISTS " + StorageConstants.ASSET_DATA_TABLE  + "_T_TIMESTAMP ");
				idx.append(" ON " + keyspace + "." + StorageConstants.ASSET_DATA_TABLE + " (" + CassandraColumns.TIMESTAMP + ")  ");
				idx.append(" USING 'org.apache.cassandra.index.sasi.SASIIndex'  ");
				idx.append(" WITH OPTIONS = { 'mode': 'SPARSE' };  ");
				session.execute(idx.toString());
				*/
				

				// 
				log.info("Cassandra table [" + keyspace + "." + asset_table + "] is created.");
				ObjectCreationChecker.getInstance().setObject(asset_table, true);
			}
			// }
			//
		} catch (Exception ex) {
			log.error("[" + asset_table + "] create error : " + ex.getMessage(), ex);
			throw ex;
		} finally {
			//
			// if (session != null && !session.isClosed())
			// session.close();
		}
		
	}
	
	
	@Override
	public void createTableForAssetAlarm() throws Exception {
		Session session = null;
		String table = "";
		
			try {
				
				//
				session     = CassandraSessionFixer.getSession();
				String keyspace    = CassandraSessionFixer.getKeyspace();
				
				// 에셋 알람 테이블
				table = keyspace + "." + StorageConstants.ASSET_ALARM_TABLE;

				
				StringBuffer createCA = new StringBuffer();
				createCA.append("CREATE TABLE IF NOT EXISTS " + table + " ");
				createCA.append(" (  ");
				createCA.append("    " + CassandraColumns.ASSET_ID    + " varchar,  ");
				createCA.append("    " + CassandraColumns.ALIAS_NAME  + " varchar,  ");
				createCA.append("    " + CassandraColumns.TAG_ID   + " varchar,  ");
				createCA.append("    " + CassandraColumns.TIMESTAMP + " timestamp,  ");
				createCA.append("    " + CassandraColumns.PRIORITY + " varchar,  ");
				createCA.append("    " + CassandraColumns.DESCRIPTION + " text,  ");
				createCA.append("    " + CassandraColumns.ALARM_SEQ   + " int,  ");
				createCA.append("    " + CassandraColumns.ALARM_CONFIG_ID + " varchar,  ");
				createCA.append("    PRIMARY KEY ((" + CassandraColumns.ASSET_ID + "," + CassandraColumns.ALIAS_NAME + ")," + CassandraColumns.TIMESTAMP + ")  ");
				createCA.append("		)  WITH  CLUSTERING ORDER BY (" + CassandraColumns.TIMESTAMP + " DESC)  ");
				createCA.append("     AND compaction = { 'class': 'TimeWindowCompactionStrategy', 'compaction_window_size': '28',  'compaction_window_unit': 'DAYS', 'max_threshold': '32', 'min_threshold': '4' } ");
				createCA.append("     AND compression = { 'class' : 'ZstdCompressor',  'compression_level': '" +  ZSTD_COMPRESSION_LEVEL + "' } ");
				createCA.append("     AND default_time_to_live = 0 ");
				createCA.append("     AND gc_grace_seconds = 10800 ");
				createCA.append("     AND max_index_interval = 2048 ");
				createCA.append("     AND memtable_flush_period_in_ms = 0 ");
				createCA.append("     AND min_index_interval = 128 ");
				createCA.append("     AND speculative_retry = 'NONE';  ");
				session.execute(createCA.toString());
				
				//8
				try {
					session.execute(" ALTER TABLE  " + keyspace + "." + StorageConstants.ASSET_ALARM_TABLE  + " ADD  "  + "SITE_ID"  + "  text  ");
					session.execute(" ALTER TABLE  " + keyspace + "." + StorageConstants.ASSET_ALARM_TABLE  + " ADD  "  + "SITE_NAME"  + "  text  ");
					session.execute(" ALTER TABLE  " + keyspace + "." + StorageConstants.ASSET_ALARM_TABLE  + " ADD  "  + "SITE_DESCRIPTION"  + "  text  ");
					session.execute(" ALTER TABLE  " + keyspace + "." + StorageConstants.ASSET_ALARM_TABLE  + " ADD  "  + "TAG_NAME"   + "  text  ");
					session.execute(" ALTER TABLE  " + keyspace + "." + StorageConstants.ASSET_ALARM_TABLE  + " ADD  "  + "TAG_DESCRIPTION"  + "  text  ");
					session.execute(" ALTER TABLE  " + keyspace + "." + StorageConstants.ASSET_ALARM_TABLE  + " ADD  "  + "TAG_LOCATION"     + "  text  ");
					session.execute(" ALTER TABLE  " + keyspace + "." + StorageConstants.ASSET_ALARM_TABLE  + " ADD  "  + "ASSET_NAME"   + "  text  ");
					session.execute(" ALTER TABLE  " + keyspace + "." + StorageConstants.ASSET_ALARM_TABLE  + " ADD  "  + "ASSET_DESCRIPTION"  + "  text  ");
					session.execute(" ALTER TABLE  " + keyspace + "." + StorageConstants.ASSET_ALARM_TABLE  + " ADD  "  + "AREA_ID"  + "  text  ");
					session.execute(" ALTER TABLE  " + keyspace + "." + StorageConstants.ASSET_ALARM_TABLE  + " ADD  "  + "AREA_NAME"  + "  text  ");
					session.execute(" ALTER TABLE  " + keyspace + "." + StorageConstants.ASSET_ALARM_TABLE  + " ADD  "  + "AREA_DESCRIPTION"  + "  text  ");
				}catch(Exception ex) {
					//
				}
				

				//
				StringBuffer createCDX1 = new StringBuffer();
				createCDX1.append(" CREATE INDEX IF NOT EXISTS " + StorageConstants.ASSET_ALARM_TABLE  + "_T_IDX_ALARM_CONFIG_ID on " + table + " (" + CassandraColumns.ALARM_CONFIG_ID + "); ");
				session.execute(createCDX1.toString());

				StringBuffer createCDX2 = new StringBuffer();
				createCDX2.append(" CREATE INDEX IF NOT EXISTS " + StorageConstants.ASSET_ALARM_TABLE  + "_T_IDX_PRIORITY on " + table + " (" + CassandraColumns.PRIORITY + "); ");
				session.execute(createCDX2.toString());
				
				//
				StringBuffer sasi_option = new StringBuffer();
				sasi_option.append(" USING 'org.apache.cassandra.index.sasi.SASIIndex' ");
				sasi_option.append("     WITH OPTIONS = { ");
				sasi_option.append(" 	'mode': 'CONTAINS',");
				sasi_option.append(" 	'analyzer_class': 'org.apache.cassandra.index.sasi.analyzer.NonTokenizingAnalyzer',");
				sasi_option.append(" 	'case_sensitive': 'false' ");
				sasi_option.append(" } ");
				
				StringBuffer c_idx_cql = new StringBuffer();
				c_idx_cql.append(" CREATE CUSTOM INDEX IF NOT EXISTS " + StorageConstants.ASSET_ALARM_TABLE  + "_T_IDX_DESCRIPTION on " + table + " (" + CassandraColumns.DESCRIPTION + ") " + sasi_option.toString());
				session.execute(c_idx_cql.toString());
				
				c_idx_cql = new StringBuffer();
				c_idx_cql.append(" CREATE CUSTOM INDEX IF NOT EXISTS " + StorageConstants.ASSET_ALARM_TABLE  + "_T_IDX_TAG_NAME on " + table + " (" + CassandraColumns.TAG_NAME  + ") " + sasi_option.toString());
				session.execute(c_idx_cql.toString());
				
				c_idx_cql = new StringBuffer();
				c_idx_cql.append(" CREATE CUSTOM INDEX IF NOT EXISTS " + StorageConstants.ASSET_ALARM_TABLE  + "_T_IDX_ASSET_NAME on " + table + " (" + CassandraColumns.ASSET_NAME + ") " + sasi_option.toString());
				session.execute(c_idx_cql.toString());
				
				c_idx_cql = new StringBuffer();
				c_idx_cql.append(" CREATE CUSTOM INDEX IF NOT EXISTS " + StorageConstants.ASSET_ALARM_TABLE  + "_T_IDX_SITE_NAME on " + table + " (" + CassandraColumns.SITE_NAME  + ") " + sasi_option.toString());
				session.execute(c_idx_cql.toString());
				
				c_idx_cql = new StringBuffer();
				c_idx_cql.append(" CREATE CUSTOM INDEX IF NOT EXISTS " + StorageConstants.ASSET_ALARM_TABLE  + "_T_IDX_AREA_NAME on " + table + " (AREA_NAME) " + sasi_option.toString());
				session.execute(c_idx_cql.toString());
				
				c_idx_cql = new StringBuffer();
				c_idx_cql.append(" CREATE CUSTOM INDEX IF NOT EXISTS " + StorageConstants.ASSET_ALARM_TABLE  + "_T_IDX_TAG_LOCATION on " + table + " (TAG_LOCATION) " + sasi_option.toString());
				session.execute(c_idx_cql.toString());
				

				//
				log.info("Cassandra table [" + table + "] is created.");
			} catch (Exception ex) {
				log.error("Cassandra table [" + table + "] and index create failed : " + ex.getMessage(), ex);
			} finally {
				// ;
			}
	};
	
	
	@Override
	public void createTableForAssetTemplateData(Asset asset, Map<String, Tag> tag_map) throws Exception {
		//
		//
		// ALTER TABLE whatever add if not exists name int;
		/*
	
		String table_name = "";
		Session session = null;
		StringBuffer cql = new StringBuffer();
		
		//
		try {
	
			//
			long start = System.currentTimeMillis();
	
			//
			String host = ConfigurationManager.getInstance().getServer_configuration().getStorage_host();
			int port = Integer.parseInt(ConfigurationManager.getInstance().getServer_configuration().getStorage_port());
			String user = ConfigurationManager.getInstance().getServer_configuration().getStorage_user();
			String password = ConfigurationManager.getInstance().getServer_configuration().getStorage_password();
			String keyspace = ConfigurationManager.getInstance().getServer_configuration().getStorage_keyspace();
			//
			session = CassandraSessionFixer.getSession();
			
			
			//1.
			//
			table_name =  StorageConstants.TAG_ASSET_TEMPLATE_DATA_TABLE_PREFIX + asset.getTable_type();
			
			if (!DBObjectChecker.getInstance().hasObject(table_name)) {
				try {
					cql = new StringBuffer();
					cql.append(" CREATE TABLE IF NOT EXISTS " + keyspace + "." + table_name  + " ( ");
					cql.append("	" + CassandraColumns.ASSET_ID   + " varchar, ");
					cql.append("	" + CassandraColumns.TIMESTAMP  + " timestamp, ");
					cql.append("	PRIMARY KEY (" + CassandraColumns.ASSET_ID + ", "  + CassandraColumns.TIMESTAMP  + ")  ");
					cql.append("	 )  ");
					cql.append("	WITH clustering order by (" +  CassandraColumns.TIMESTAMP + " DESC) ");
					cql.append("     AND compaction = { 'class': 'DateTieredCompactionStrategy', 'min_threshold': '12', 'max_threshold': '32', 'max_sstable_age_days': '0.083', 'base_time_seconds': '50' } ");
					cql.append("     AND compression = { 'class' : 'LZ4Compressor' } ");
					cql.append("     AND dclocal_read_repair_chance = 0.1 ");
					cql.append("     AND default_time_to_live = 0 ");
					cql.append("     AND gc_grace_seconds = 0 ");
					cql.append("     AND max_index_interval = 2048 ");
					cql.append("     AND memtable_flush_period_in_ms = 0 ");
					cql.append("     AND min_index_interval = 128 ");
					cql.append("     AND read_repair_chance = 0.0 ");
					cql.append("     AND speculative_retry = 'NONE';  ");
					session.execute(cql.toString());
					
					
					DBObjectChecker.getInstance().setObject(table_name, true);
				}catch(Exception ex){
					log.error("Asset table [" + table_name + "] create failed : " + ex.getMessage(), ex);
				}
			};
			
			//1. 
			Map<String,String> b_map = new HashMap<String,String>();
			KeyspaceMetadata ks = session.getCluster().getMetadata().getKeyspace(keyspace);
			TableMetadata table = ks.getTable(table_name);
			List<ColumnMetadata> colmds = table.getColumns();
			log.debug("Asset table column/map size : " + colmds.size() + " : " + tag_map.size());
			for(int i=0; i < colmds.size(); i++){
				ColumnMetadata meta = colmds.get(i);
				b_map.put(meta.getName().toUpperCase(), "");
			};
			
			//
			boolean is_add = false;
			List<String[]> add_columns_list = new ArrayList<String[]>();
			SortedSet<String> keys = new TreeSet<String>(tag_map.keySet());
			for (String key : keys) { 
				Tag tag = tag_map.get(key);
				try{
					if(!b_map.containsKey(key.toUpperCase())){
						is_add = true;
						
						String[] col = new String[] {key,  getColumnTypeForCassandra(getJavaType(tag.getJava_type())) };
						add_columns_list.add(col);
						//
						b_map.put(key, "");
					}
				}catch(Exception e){
				   log.error("Asset table [" + table_name + "] column add error : " + e.getMessage());
				}
			};
			
			if(is_add){
				   List<String> cn = new ArrayList<String>();
				   StringBuffer colb = new StringBuffer();
				   for(int i=0; i < add_columns_list.size(); i++){
					   String[] col = add_columns_list.get(i);
					   cn.add(col[0]);
					   colb.append(col[0] + " " + col[1]);
					   if(i != (add_columns_list.size()-1)){
						   colb.append(", ");
					   }
				   }
			       String add_column_cql = "ALTER TABLE " + keyspace + "." + table_name +  " ADD (" + colb.toString() + ")";
				   session.execute(add_column_cql);
				   log.info("Asset table [" + table_name + "] column added = [" + cn.toString() + "]");
			}
			
			//
			b_map.remove(CassandraColumns.TIMESTAMP.toUpperCase());
			b_map.remove(CassandraColumns.ASSET_ID.toUpperCase());
			
			AssetTableLayoutFactory.getInstance().put(table_name, b_map);
			
			long end = System.currentTimeMillis() - start;
			log.info("Cassandra table [" + table_name + "] is columns updated : columns=[" + b_map.toString() + "]exec_time=[" + end + "]");
	
		} catch (Exception ex) {
			log.error("Cassandra table [" + table_name + "] create error : CQL=[" + cql.toString() + "], " + ex.getMessage(), ex);
			throw ex;
		} finally {
			//
			// if (session != null && !session.isClosed())
			// session.close();
		}
		*/
	}

	@Override
	public void createTableForAssetHealthStatus() throws Exception {

		Session session = null;
		String health_table = "";
		//
		try {

			//
			session     = CassandraSessionFixer.getSession();
			String keyspace    = CassandraSessionFixer.getKeyspace();

			health_table = StorageConstants.ASSET_HEALTH_STATUS_TABLE;
			//
			if (!ObjectCreationChecker.getInstance().hasObject(health_table)) {
				//
				StringBuffer createCF = new StringBuffer();
				createCF.append(" CREATE TABLE IF NOT EXISTS " + keyspace + "." + health_table + " ( ");
				createCF.append("		  " + CassandraColumns.ASSET_ID + "  varchar,  ");
				createCF.append("		  " + CassandraColumns.TIMESTAMP + "  timestamp,  ");
				createCF.append("		  " + CassandraColumns.FROM + "  bigint,  ");
				createCF.append("		  " + CassandraColumns.TO + "    bigint,   ");
				createCF.append("		  " + CassandraColumns.STATUS + "  varchar,  ");
				createCF.append("		  " + CassandraColumns.ALARM_INFO_COUNT + "   int,  ");
				createCF.append("		  " + CassandraColumns.ALARM_WARN_COUNT + "   int,  ");
				createCF.append("		  " + CassandraColumns.ALARM_ERROR_COUNT + "  int,  ");
				createCF.append("		  PRIMARY KEY (" + CassandraColumns.ASSET_ID + ", " + CassandraColumns.TIMESTAMP + ")  ");
				createCF.append("		)  WITH CLUSTERING ORDER BY (" + CassandraColumns.TIMESTAMP + " DESC)  ");
				createCF.append("     AND compaction  = { 'class': 'TimeWindowCompactionStrategy', 'compaction_window_size': '28',  'compaction_window_unit': 'DAYS'  } ");
				createCF.append("     AND compression = { 'class' : 'ZstdCompressor',  'compression_level': '" +  ZSTD_COMPRESSION_LEVEL + "' } ");
				createCF.append("     AND default_time_to_live = 0 ");
				createCF.append("     AND gc_grace_seconds = 864000 ");
				createCF.append("     AND max_index_interval = 2048 ");
				createCF.append("     AND memtable_flush_period_in_ms = 0 ");
				createCF.append("     AND min_index_interval = 128 ");
				createCF.append("     AND speculative_retry = 'NONE';  ");
				session.execute(createCF.toString());
				

				// 
				log.info("Cassandra table [" + keyspace + "." + health_table + "] is created.");
				ObjectCreationChecker.getInstance().setObject(health_table, true);
			}
			// }
			//
		} catch (Exception ex) {
			log.error("[" + health_table + "] create error : " + ex.getMessage(), ex);
			throw ex;
		} finally {
			//
			// if (session != null && !session.isClosed())
			// session.close();
		}

	};
	
	
	@Override
	public void createTableForAssetConnectionStatus() throws Exception {

		Session session = null;
		String health_table = "";
		//
		try {

			//
			session     = CassandraSessionFixer.getSession();
			String keyspace    = CassandraSessionFixer.getKeyspace();

			health_table = StorageConstants.ASSET_CONNECTION_STATUS_TABLE;
			//
			if (!ObjectCreationChecker.getInstance().hasObject(health_table)) {
				//
				StringBuffer createCF = new StringBuffer();
				createCF.append(" CREATE TABLE IF NOT EXISTS " + keyspace + "." + health_table + " ( ");
				createCF.append("		  " + CassandraColumns.ASSET_ID + "  varchar,  ");
				createCF.append("		  " + CassandraColumns.TIMESTAMP + "  timestamp,  ");
				createCF.append("		  " + CassandraColumns.FROM + "  bigint,  ");
				createCF.append("		  " + CassandraColumns.TO + "    bigint,   ");
				createCF.append("		  " + CassandraColumns.STATUS + "  varchar, ");
				createCF.append("		  PRIMARY KEY (" + CassandraColumns.ASSET_ID + ", " + CassandraColumns.TIMESTAMP + ")  ");
				createCF.append("		)  WITH CLUSTERING ORDER BY (" + CassandraColumns.TIMESTAMP + " DESC)  ");
				createCF.append("     AND compaction = { 'class': 'TimeWindowCompactionStrategy', 'compaction_window_size': '1',  'compaction_window_unit': 'DAYS' } ");
				createCF.append("     AND compression = {  'class' : 'ZstdCompressor',  'compression_level': '" +  ZSTD_COMPRESSION_LEVEL + "' } ");
				createCF.append("     AND default_time_to_live = 0 ");
				createCF.append("     AND gc_grace_seconds = 864000 ");
				createCF.append("     AND max_index_interval = 2048 ");
				createCF.append("     AND memtable_flush_period_in_ms = 0 ");
				createCF.append("     AND min_index_interval = 128 ");
				createCF.append("     AND speculative_retry = 'NONE';  ");
				session.execute(createCF.toString());
				

				// 
				log.info("Cassandra table [" + keyspace + "." + health_table + "] is created.");
				ObjectCreationChecker.getInstance().setObject(health_table, true);
			}
			// }
			//
		} catch (Exception ex) {
			log.error("[" + health_table + "] create error : " + ex.getMessage(), ex);
			throw ex;
		} finally {
			//
			// if (session != null && !session.isClosed())
			// session.close();
		}

	};
	
	@Override
	public void createTableForAssetEvent() throws Exception {

		Session session = null;
		String table = "";
		//
		try {

			//
			session     = CassandraSessionFixer.getSession();
			String keyspace    = CassandraSessionFixer.getKeyspace();

			table = StorageConstants.ASSET_EVENT_TABLE;
			//
			if (!ObjectCreationChecker.getInstance().hasObject(table)) {
				//
				StringBuffer createCF = new StringBuffer();
				createCF.append(" CREATE TABLE IF NOT EXISTS " + keyspace + "." + table + " ( ");
				createCF.append("		  " + CassandraColumns.ASSET_ID + "  varchar,  ");
				createCF.append("		  " + CassandraColumns.TIMESTAMP + "  timestamp,  ");
				createCF.append("		  " + CassandraColumns.FROM + "  bigint,  ");
				createCF.append("		  " + CassandraColumns.TO + "    bigint,   ");
				createCF.append("		  " + CassandraColumns.EVENT + "  varchar,  ");
				createCF.append("		  " + CassandraColumns.COLOR + "  varchar,  ");
				createCF.append("		  " + CassandraColumns.DATA  + "  frozen<map<varchar, varchar>>,  ");
				createCF.append("		  " + CassandraColumns.NOTE  + "  varchar,  ");
				createCF.append("		  PRIMARY KEY (" + CassandraColumns.ASSET_ID + ", " + CassandraColumns.TIMESTAMP + ")  ");
				createCF.append("		)  WITH CLUSTERING ORDER BY (" + CassandraColumns.TIMESTAMP + " DESC)  ");
				createCF.append("     AND compaction = { 'class': 'TimeWindowCompactionStrategy', 'compaction_window_size': '1',  'compaction_window_unit': 'DAYS' } ");
				createCF.append("     AND compression = { 'class' : 'ZstdCompressor',  'compression_level': '" +  ZSTD_COMPRESSION_LEVEL + "' } ");
				createCF.append("     AND default_time_to_live = 0 ");
				createCF.append("     AND gc_grace_seconds = 864000 ");
				createCF.append("     AND max_index_interval = 2048 ");
				createCF.append("     AND memtable_flush_period_in_ms = 0 ");
				createCF.append("     AND min_index_interval = 128 ");
				createCF.append("     AND speculative_retry = 'NONE';  ");
				session.execute(createCF.toString());
				//
				try {
				session.execute(" ALTER TABLE  " + keyspace + "." + table  + " ADD  "  + CassandraColumns.STATEMENT_NAME  + "  text  ");
				session.execute(" ALTER TABLE  " + keyspace + "." + table  + " ADD  "  + CassandraColumns.STATEMENT_DESCRIPTION  + "  text  ");
				}catch(Exception ex) {
					
				};
				//
				StringBuffer createIDX = new StringBuffer();
				createIDX.append(" CREATE INDEX IF NOT EXISTS " + StorageConstants.ASSET_EVENT_TABLE  + "_T_IDX_EVENT on " + keyspace + "." + StorageConstants.ASSET_EVENT_TABLE + " (" + CassandraColumns.EVENT + ") ");
				session.execute(createIDX.toString());
				

				// 
				log.info("Cassandra table [" + keyspace + "." + table + "] is created.");
				ObjectCreationChecker.getInstance().setObject(table, true);
			}
			// }
			//
		} catch (Exception ex) {
			log.error("[" + table + "] create error : " + ex.getMessage(), ex);
			throw ex;
		} finally {
			//
			// if (session != null && !session.isClosed())
			// session.close();
		}
	}
	
	@Override
	public void createTableForAssetAggregation() throws Exception {
		Session session = null;
		String table = "";
		//
		try {

			//
			session     = CassandraSessionFixer.getSession();
			String keyspace    = CassandraSessionFixer.getKeyspace();
			//

			table = StorageConstants.ASSET_AGGREGATION_TABLE;
			//
			if (!ObjectCreationChecker.getInstance().hasObject(table)) {
				//
	
				StringBuffer createCF = new StringBuffer();
				createCF.append(" CREATE TABLE IF NOT EXISTS " + keyspace + "." + table + " ( ");
				createCF.append("		  " + CassandraColumns.ASSET_ID     + "  varchar,  ");
				createCF.append("		  " + CassandraColumns.TIMESTAMP    + "  timestamp,  ");
				createCF.append("		  " + CassandraColumns.AGGREGATION  + "  varchar,  ");
				createCF.append("		  " + CassandraColumns.KEY          + "  varchar,   ");
				createCF.append("		  " + CassandraColumns.VALUE       + "  varchar,   ");
				createCF.append("		  PRIMARY KEY ((" + CassandraColumns.ASSET_ID + "," + CassandraColumns.AGGREGATION  + "," + CassandraColumns.KEY + "), " + CassandraColumns.TIMESTAMP + ")  ");
				createCF.append("		)  WITH CLUSTERING ORDER BY (" + CassandraColumns.TIMESTAMP + " DESC)  ");
				createCF.append("     AND compaction  = { 'class' : 'TimeWindowCompactionStrategy', 'compaction_window_size': '1',  'compaction_window_unit': 'DAYS' } ");
				createCF.append("     AND compression = { 'class' : 'ZstdCompressor',  'compression_level': '" +  ZSTD_COMPRESSION_LEVEL + "' } ");
				createCF.append("     AND default_time_to_live = 0 ");
				createCF.append("     AND gc_grace_seconds = 864000 ");
				createCF.append("     AND max_index_interval = 2048 ");
				createCF.append("     AND memtable_flush_period_in_ms = 0 ");
				createCF.append("     AND min_index_interval = 128 ");
				createCF.append("     AND speculative_retry = 'NONE';  ");
				session.execute(createCF.toString());
				
				//
				try {
					session.execute(" ALTER TABLE  " + keyspace + "." + table  + " ADD  "  + CassandraColumns.STATEMENT_NAME  + "  text  ");
					session.execute(" ALTER TABLE  " + keyspace + "." + table  + " ADD  "  + CassandraColumns.STATEMENT_DESCRIPTION  + "  text  ");
				}catch(Exception ex) {
					
				}
				
				StringBuffer createIDX = new StringBuffer();
				createIDX.append(" CREATE INDEX IF NOT EXISTS " + StorageConstants.ASSET_AGGREGATION_TABLE  + "_T_IDX_AGGREGATION on " + keyspace + "." + StorageConstants.ASSET_AGGREGATION_TABLE + " (" + CassandraColumns.AGGREGATION + ") ");
				session.execute(createIDX.toString());
				

				// 
				log.info("Cassandra table [" + keyspace + "." + table + "] is created.");
				ObjectCreationChecker.getInstance().setObject(table, true);
			}
			// }
			//
		} catch (Exception ex) {
			log.error("[" + table + "] create error : " + ex.getMessage(), ex);
			throw ex;
		} finally {
			//
			// if (session != null && !session.isClosed())
			// session.close();
		}
	}
	
	@Override
	public void createTableForAssetContext() throws Exception {
		Session session = null;
		String table = "";
		//
		try {

			//
			session     = CassandraSessionFixer.getSession();
			String keyspace    = CassandraSessionFixer.getKeyspace();
			
			//

			table = StorageConstants.ASSET_CONTEXT_TABLE;
			//
			if (!ObjectCreationChecker.getInstance().hasObject(table)) {
				//
	
				StringBuffer createCF = new StringBuffer();
				createCF.append(" CREATE TABLE IF NOT EXISTS " + keyspace + "." + table + " ( ");
				createCF.append("		  " + CassandraColumns.ASSET_ID     + "  varchar,  ");
				createCF.append("		  " + CassandraColumns.TIMESTAMP    + "  timestamp,  ");
				createCF.append("		  " + CassandraColumns.CONTEXT      + "  varchar,  ");
				createCF.append("		  " + CassandraColumns.KEY          + "  varchar,   ");
				createCF.append("		  " + CassandraColumns.VALUE       + "  varchar,   ");
				createCF.append("		  PRIMARY KEY ((" + CassandraColumns.ASSET_ID + "," + CassandraColumns.CONTEXT  + "," + CassandraColumns.KEY + "), " + CassandraColumns.TIMESTAMP + ")  ");
				createCF.append("		)  WITH CLUSTERING ORDER BY (" + CassandraColumns.TIMESTAMP + " DESC)  ");
				createCF.append("     AND compaction = { 'class': 'TimeWindowCompactionStrategy', 'compaction_window_size': '1',  'compaction_window_unit': 'DAYS' } ");
				createCF.append("     AND compression = {  'class' : 'ZstdCompressor',  'compression_level': '" +  ZSTD_COMPRESSION_LEVEL + "' } ");
				createCF.append("     AND default_time_to_live = 0 ");
				createCF.append("     AND gc_grace_seconds = 864000 ");
				createCF.append("     AND max_index_interval = 2048 ");
				createCF.append("     AND memtable_flush_period_in_ms = 0 ");
				createCF.append("     AND min_index_interval = 128 ");
				createCF.append("     AND speculative_retry = 'NONE';  ");
				session.execute(createCF.toString());
				
				try {
				session.execute(" ALTER TABLE  " + keyspace + "." + table  + " ADD  "  + CassandraColumns.STATEMENT_NAME  + "  text  ");
				session.execute(" ALTER TABLE  " + keyspace + "." + table  + " ADD  "  + CassandraColumns.STATEMENT_DESCRIPTION  + "  text  ");
				}catch(Exception ex) {
					
				}
				StringBuffer createIDX = new StringBuffer();
				createIDX.append(" CREATE INDEX IF NOT EXISTS " + table + "_T_IDX_CONTEXT on " + keyspace + "." + table + " (" + CassandraColumns.CONTEXT + ") ");
				session.execute(createIDX.toString());
				

				// 
				log.info("Cassandra table [" + keyspace + "." + table + "] is created.");
				ObjectCreationChecker.getInstance().setObject(table, true);
			}
			// }
			//
		} catch (Exception ex) {
			log.error("[" + table + "] create error : " + ex.getMessage(), ex);
			throw ex;
		} finally {
			//
		}
	}

	
	@Override
	public void createTableForSystemHealthStatus() throws Exception {

		Session session = null;
		String health_table = "";
		//
		try {

			//
			session     = CassandraSessionFixer.getSession();
			String keyspace    = CassandraSessionFixer.getKeyspace();
			
			//

			health_table = StorageConstants.SYSTEM_HEALTH_STATUS_TABLE;
			//
			if (!ObjectCreationChecker.getInstance().hasObject(health_table)) {
				//
				StringBuffer createCF = new StringBuffer();
				createCF.append(" CREATE TABLE IF NOT EXISTS " + StorageConstants.SYSTEM_KEYSPACE + "." + StorageConstants.SYSTEM_HEALTH_STATUS_TABLE + " ( ");
				createCF.append("		  " + CassandraColumns.APP_NAME + "  varchar,  ");
				createCF.append("		  " + CassandraColumns.TIMESTAMP + "  timestamp,  ");
				createCF.append("		  " + CassandraColumns.FROM + "  bigint,  ");
				createCF.append("		  " + CassandraColumns.TO + "    bigint,   ");
				createCF.append("		  " + CassandraColumns.STATUS + "  varchar,  ");
				createCF.append("		  " + CassandraColumns.ALARM_INFO_COUNT + "   int,  ");
				createCF.append("		  " + CassandraColumns.ALARM_WARN_COUNT + "   int,  ");
				createCF.append("		  " + CassandraColumns.ALARM_ERROR_COUNT + "  int,  ");
				createCF.append("		  PRIMARY KEY (" + CassandraColumns.APP_NAME + ", " + CassandraColumns.TIMESTAMP + ")  ");
				createCF.append("		)  WITH CLUSTERING ORDER BY (" + CassandraColumns.TIMESTAMP + " DESC)  ");
				createCF.append("     AND compaction = { 'class': 'TimeWindowCompactionStrategy', 'compaction_window_size': '1',  'compaction_window_unit': 'DAYS'  } ");
				createCF.append("     AND compression = {  'class' : 'ZstdCompressor',  'compression_level': '" +  ZSTD_COMPRESSION_LEVEL + "' } ");
				createCF.append("     AND default_time_to_live = 0 ");
				createCF.append("     AND gc_grace_seconds = 864000 ");
				createCF.append("     AND max_index_interval = 2048 ");
				createCF.append("     AND memtable_flush_period_in_ms = 0 ");
				createCF.append("     AND min_index_interval = 128 ");
				createCF.append("     AND speculative_retry = 'NONE';  ");
				session.execute(createCF.toString());
				

				// 
				log.info("Cassandra table [" + keyspace + "." + health_table + "] is created.");
				ObjectCreationChecker.getInstance().setObject(health_table, true);
			}
			// }
			//
		} catch (Exception ex) {
			log.error("[" + health_table + "] create error : " + ex.getMessage(), ex);
			throw ex;
		} finally {
			//
			// if (session != null && !session.isClosed())
			// session.close();
		}

	}
	

	@Override
	public void createTableForTrigger(Trigger trigger) throws Exception {

		Session session = null;
		StringBuffer cql = null;
		//
		try {

			//
			session     = CassandraSessionFixer.getSession();
			String keyspace    = CassandraSessionFixer.getKeyspace();
			
			//

			//
			cql = new StringBuffer();
			cql.append(" CREATE TABLE IF NOT EXISTS " + keyspace + "." + trigger.getStorage_table() + " ( ");
			//
			for (int i = 0; trigger.getTrigger_attributes() != null && i < trigger.getTrigger_attributes().length; i++) {
				TriggerAttribute attribute = trigger.getTrigger_attributes()[i];
				cql.append(" " + attribute.getField_name() + " " + getColumnTypeForCassandra(attribute.getJava_type()) + " ");
				cql.append(", ");
			}
			//
			cql.append("		  PRIMARY KEY (" + trigger.getStorage_table_primary_keys() + ")  ");
			cql.append("	 )  ");
			if (StringUtils.isNotEmpty(trigger.getStorage_table_clustering_order_by_keys())) {
				cql.append("	WITH CLUSTERING ORDER BY (" + trigger.getStorage_table_clustering_order_by_keys() + ") ");
			}
			//
			cql.append("     AND compaction = { 'class': 'TimeWindowCompactionStrategy', 'compaction_window_size': '1',  'compaction_window_unit': 'DAYS'  } ");
			cql.append("     AND compression = {  'class' : 'ZstdCompressor',  'compression_level': '" +  ZSTD_COMPRESSION_LEVEL + "' } ");
			cql.append("     AND default_time_to_live = 0 ");
			cql.append("     AND gc_grace_seconds = 864000 ");
			cql.append("     AND max_index_interval = 2048 ");
			cql.append("     AND memtable_flush_period_in_ms = 0 ");
			cql.append("     AND min_index_interval = 128 ");
			cql.append("     AND speculative_retry = 'NONE';  ");
			
			session.execute(cql.toString());
			
			

			if (StringUtils.isNotEmpty(trigger.getStorage_table_idx_keys())) {
				cql = new StringBuffer();
				cql.append(" CREATE INDEX IF NOT EXISTS " + trigger.getStorage_table()  + "_T_IDX on " + keyspace + "." + trigger.getStorage_table() + " (" + trigger.getStorage_table_idx_keys() + ") ");
				session.execute(cql.toString());
			}

			// 
			log.info("Trigger table [" + trigger.getStorage_table() + "] is created.");
			ObjectCreationChecker.getInstance().setObject(trigger.getStorage_table(), true);

		} catch (Exception ex) {
			log.error("[" + trigger.getStorage_table() + "] create error : " + ex.getMessage(), ex);
			throw ex;
		} finally {
			//
			// if (session != null && !session.isClosed())
			// session.close();
		}

	}

	
	@Override
	public void createTableForDomainChangeHistory() throws Exception {

		try {

			//
			Session session     = CassandraSessionFixer.getSession();
			String keyspace    = CassandraSessionFixer.getKeyspace();

				//
				//
				StringBuffer createCF = new StringBuffer();
				createCF.append(" CREATE TABLE IF NOT EXISTS " + keyspace + "." + StorageConstants.DOMAIN_CHANGE_HISTORY_TABLE + " ( ");
				createCF.append("		  " + CassandraColumns.DOMAIN_TYPE + "         varchar,  ");
				createCF.append("		  " + CassandraColumns.OBJECT_ID   + "         varchar,  ");
				createCF.append("		  " + CassandraColumns.TIMESTAMP + "   timestamp,  ");
				createCF.append("		  " + CassandraColumns.JSON + "        text,   ");
				createCF.append("		  PRIMARY KEY ((" + CassandraColumns.DOMAIN_TYPE + ", " + CassandraColumns.OBJECT_ID + "), " + CassandraColumns.TIMESTAMP + ")  ");
				createCF.append("		)  WITH CLUSTERING ORDER BY (" + CassandraColumns.TIMESTAMP + " DESC)  ");
				createCF.append("     AND compaction = { 'class': 'TimeWindowCompactionStrategy', 'compaction_window_size': '1',  'compaction_window_unit': 'DAYS'  }");
				createCF.append("     AND caching= {'keys':'ALL', 'rows_per_partition':'NONE'} ");
				createCF.append("     AND compression = { 'class' : 'ZstdCompressor',  'compression_level': '" +  ZSTD_COMPRESSION_LEVEL + "' } ");
				createCF.append("     AND bloom_filter_fp_chance = 0.01    ");
				createCF.append("     AND default_time_to_live = 0 ");
				createCF.append("     AND gc_grace_seconds = 864000 ");
				createCF.append("     AND max_index_interval = 2048 ");
				createCF.append("     AND memtable_flush_period_in_ms = 0 ");
				createCF.append("     AND min_index_interval = 128 ");
				createCF.append("     AND speculative_retry = 'NONE';  ");
				
				session.execute(createCF.toString());
				
				// 
				log.info("Cassandra table [" + keyspace + "." + StorageConstants.DOMAIN_CHANGE_HISTORY_TABLE + "] is created.");
	
			
		} catch (Exception ex) {
			log.error("[" + StorageConstants.DOMAIN_CHANGE_HISTORY_TABLE + "] create error : " + ex.getMessage(), ex);
			throw ex;
		} finally {
			
		}
	}
	
	
	
	
	@Override
	public void createTableForOptions() throws Exception {

		try {

			Session session     = CassandraSessionFixer.getSession();
			String keyspace    = CassandraSessionFixer.getKeyspace();

		
			//------------------------------------------------------------------------------------------------------------------
			//옵션 리스너 결과 캐쉬 테이블
			//------------------------------------------------------------------------------------------------------------------
			StringBuffer createCF = new StringBuffer();
			createCF.append(" CREATE TABLE IF NOT EXISTS "  + keyspace + "." + StorageConstants.OPTION_LISTENER_PUSH_CACHE_TABLE + " ( ");
			createCF.append("		  " + CassandraColumns.URL + "         varchar,  ");
			createCF.append("		  " + CassandraColumns.TIMESTAMP + "   timestamp,  ");
			createCF.append("		  " + CassandraColumns.JSON + "        varchar,   ");
			createCF.append("		  PRIMARY KEY (" + CassandraColumns.URL + ", " + CassandraColumns.TIMESTAMP + ")  ");
			createCF.append("		)  WITH CLUSTERING ORDER BY (" + CassandraColumns.TIMESTAMP + " DESC)  ");
			createCF.append("     AND compaction = { 'class': 'TimeWindowCompactionStrategy', 'compaction_window_size': '1', 'compaction_window_unit': 'HOURS' }");
			createCF.append("     AND caching= { 'keys':'ALL', 'rows_per_partition':'NONE'} ");
			createCF.append("     AND compression = {  'class' : 'ZstdCompressor',  'compression_level': '" +  ZSTD_COMPRESSION_LEVEL + "' } ");
			createCF.append("     AND bloom_filter_fp_chance = 0.01    ");
			createCF.append("     AND default_time_to_live = 86500 "); //1일 저장
			createCF.append("     AND gc_grace_seconds = 10800 ");
			createCF.append("     AND max_index_interval = 2048 ");
			createCF.append("     AND memtable_flush_period_in_ms = 0 ");
			createCF.append("     AND min_index_interval = 128 ");
			createCF.append("     AND speculative_retry = 'NONE';  ");
			
			session.execute(createCF.toString());
			
			// 
			log.info("Cassandra table [" + keyspace + "." + StorageConstants.OPTION_LISTENER_PUSH_CACHE_TABLE + "] is created.");
			
			
			//------------------------------------------------------------------------------------------------------------------
			//시스템 시작 일자 테이블
			//------------------------------------------------------------------------------------------------------------------
			createCF = new StringBuffer();
			createCF.append(" CREATE TABLE IF NOT EXISTS "  + keyspace + "." + StorageConstants.OPTION_SYSTEM_START_DATE + " ( ");
			createCF.append("		  " + CassandraColumns.TIMESTAMP     + "   timestamp,   "); //시작일
			createCF.append("		  " + CassandraColumns.RUN_DURATION  + "   bigint,    ");
			createCF.append("		  PRIMARY KEY (" + CassandraColumns.TIMESTAMP + ")  ");
			createCF.append("		)  ");
			createCF.append("     WITH caching= { 'keys':'ALL', 'rows_per_partition':'NONE'} ");
			createCF.append("     AND compression = {  'class' : 'ZstdCompressor',  'compression_level': '" +  ZSTD_COMPRESSION_LEVEL + "' } ");
			createCF.append("     AND bloom_filter_fp_chance = 0.01    ");
			createCF.append("     AND default_time_to_live = 0 "); 
			createCF.append("     AND gc_grace_seconds = 10800 ");
			createCF.append("     AND max_index_interval = 2048 ");
			createCF.append("     AND memtable_flush_period_in_ms = 0 ");
			createCF.append("     AND min_index_interval = 128 ");
			createCF.append("     AND speculative_retry = 'NONE';  ");
			
			session.execute(createCF.toString());
			
			long server_start_timestamp = CEPEngineManager.getInstance().getStart_date().getTime();
			session.execute("INSERT INTO " + keyspace + "." + StorageConstants.OPTION_SYSTEM_START_DATE + "(timestamp, run_duration) values(" + server_start_timestamp + ", 0)" ) ;
			
			// 
			log.info("Cassandra table [" + keyspace + "." + StorageConstants.OPTION_SYSTEM_START_DATE + "] is created.");
			
			//------------------------------------------------------------------------------------------------------------------
			//시스템 프로퍼티 테이블
			//------------------------------------------------------------------------------------------------------------------
			createCF = new StringBuffer();
			createCF.append(" CREATE TABLE IF NOT EXISTS "  + keyspace + "." + StorageConstants.OPTION_SYSTEM_PROPERTIES + " ( ");
			createCF.append("		  " + CassandraColumns.PROPERTY   + "   varchar,  ");
			createCF.append("		  " + CassandraColumns.TIMESTAMP  + "   timestamp,  "); //시작일
			createCF.append("		  " + CassandraColumns.CONTENT    + "   text,   ");
			createCF.append("		  PRIMARY KEY (" + CassandraColumns.PROPERTY + ", " + CassandraColumns.TIMESTAMP + ")  ");
			createCF.append("		)  WITH CLUSTERING ORDER BY (" + CassandraColumns.TIMESTAMP + " DESC)  ");
			createCF.append("     AND caching= { 'keys':'ALL', 'rows_per_partition':'NONE'} ");
			createCF.append("     AND compression = {  'class' : 'ZstdCompressor',  'compression_level': '" +  ZSTD_COMPRESSION_LEVEL + "' } ");
			createCF.append("     AND bloom_filter_fp_chance = 0.01    ");
			createCF.append("     AND default_time_to_live = 0 "); 
			createCF.append("     AND gc_grace_seconds = 10800 ");
			createCF.append("     AND max_index_interval = 2048 ");
			createCF.append("     AND memtable_flush_period_in_ms = 0 ");
			createCF.append("     AND min_index_interval = 128 ");
			createCF.append("     AND speculative_retry = 'NONE';  ");
			
			session.execute(createCF.toString());
			
			session.execute("INSERT INTO " + keyspace + "." + StorageConstants.OPTION_SYSTEM_PROPERTIES + "(property, timestamp, content) VALUES ('application'," + server_start_timestamp + ", '" + StringEscapeUtils.escapeSql(PropertiesLoader.getApplication_properties().toString()) + "')" ) ;
			session.execute("INSERT INTO " + keyspace + "." + StorageConstants.OPTION_SYSTEM_PROPERTIES + "(property, timestamp, content) VALUES ('engine'," + server_start_timestamp + ", '" + StringEscapeUtils.escapeSql(PropertiesLoader.getEngine_properties().toString()) + "')" ) ;
			session.execute("INSERT INTO " + keyspace + "." + StorageConstants.OPTION_SYSTEM_PROPERTIES + "(property, timestamp, content) VALUES ('batch'," + server_start_timestamp + ", '" + StringEscapeUtils.escapeSql(PropertiesLoader.getBatch_properties().toString()) + "')" ) ;
			session.execute("INSERT INTO " + keyspace + "." + StorageConstants.OPTION_SYSTEM_PROPERTIES + "(property, timestamp, content) VALUES ('db'," + server_start_timestamp + ", '" + StringEscapeUtils.escapeSql(PropertiesLoader.getDb_properties().toString()) + "')" ) ;
			session.execute("INSERT INTO " + keyspace + "." + StorageConstants.OPTION_SYSTEM_PROPERTIES + "(property, timestamp, content) VALUES ('mq'," + server_start_timestamp + ", '" + StringEscapeUtils.escapeSql(PropertiesLoader.getMq_properties().toString()) + "')" ) ;
			session.execute("INSERT INTO " + keyspace + "." + StorageConstants.OPTION_SYSTEM_PROPERTIES + "(property, timestamp, content) VALUES ('storage'," + server_start_timestamp + ", '" + StringEscapeUtils.escapeSql(PropertiesLoader.getStorage_properties().toString()) + "')" ) ;
			
			
			// 
			log.info("Cassandra table [" + keyspace + "." + StorageConstants.OPTION_SYSTEM_PROPERTIES + "] is created.");
			
			
		} catch (Exception ex) {
			log.error("Cassandra table for options create error : " + ex.getMessage(), ex);
			throw ex;
		} finally {
			
		}
	}
	
	@Override
	public void createMaterializedView() throws Exception {

		try {

			Session session     = CassandraSessionFixer.getSession();
			String keyspace    = CassandraSessionFixer.getKeyspace();

			StringBuffer cql = new StringBuffer();
			
			/* --------------------------------------------------------------------- */
			/* 메타 테이블 */
			/* --------------------------------------------------------------------- */
			
			/* MM_에셋 */
			cql = new StringBuffer();
			cql.append("     CREATE MATERIALIZED VIEW IF NOT EXISTS " + keyspace + "." + "MM_ASSET_MV_BY_SITE_ID  AS  ");
			cql.append("               SELECT * FROM " + keyspace + ".MM_ASSET ");
			cql.append("                WHERE site_id IS NOT NULL  ");
			cql.append("        		  AND asset_id IS NOT NULL  ");
			cql.append("               PRIMARY KEY (site_id, asset_id)  ");
			cql.append("      WITH CLUSTERING ORDER BY (asset_id ASC);  ");
			session.execute(cql.toString());
			
			cql = new StringBuffer();
			cql.append("     CREATE MATERIALIZED VIEW IF NOT EXISTS " + keyspace + "." + "MM_ASSET_MV_BY_TABLE_TYPE  AS  ");
			cql.append("               SELECT * FROM " + keyspace + ".MM_ASSET ");
			cql.append("                WHERE table_type IS NOT NULL  ");
			cql.append("        		  AND asset_id IS NOT NULL  ");
			cql.append("               PRIMARY KEY (table_type, asset_id)  ");
			cql.append("      WITH CLUSTERING ORDER BY (asset_id ASC);  ");
			session.execute(cql.toString());
			
			cql = new StringBuffer();
			cql.append("     CREATE MATERIALIZED VIEW IF NOT EXISTS " + keyspace + "." + "MM_ASSET_MV_BY_ASSET_TYPE  AS  ");
			cql.append("               SELECT * FROM " + keyspace + ".MM_ASSET ");
			cql.append("                WHERE asset_type IS NOT NULL  ");
			cql.append("        		  AND asset_id IS NOT NULL  ");
			cql.append("               PRIMARY KEY (asset_type, asset_id)  ");
			cql.append("      WITH CLUSTERING ORDER BY (asset_id ASC);  ");
			session.execute(cql.toString());
			
			cql = new StringBuffer();
			cql.append("     CREATE MATERIALIZED VIEW IF NOT EXISTS " + keyspace + "." + "MM_ASSET_MV_BY_ASSET_NAME  AS  ");
			cql.append("               SELECT * FROM " + keyspace + ".MM_ASSET ");
			cql.append("                WHERE asset_name IS NOT NULL  ");
			cql.append("        		  AND asset_id IS NOT NULL  ");
			cql.append("               PRIMARY KEY (asset_name, asset_id)  ");
			cql.append("      WITH CLUSTERING ORDER BY (asset_id ASC);  ");
			session.execute(cql.toString());
			
			/* MM_OPC */
			cql = new StringBuffer();
			cql.append("     CREATE MATERIALIZED VIEW IF NOT EXISTS " + keyspace + "." + "MM_OPC_MV_BY_OPC_SERVER_IP  AS  ");
			cql.append("               SELECT * FROM " + keyspace + ".MM_OPC ");
			cql.append("                WHERE opc_server_ip IS NOT NULL  ");
			cql.append("        		  AND opc_id IS NOT NULL  ");
			cql.append("               PRIMARY KEY (opc_server_ip, opc_id)  ");
			cql.append("      WITH CLUSTERING ORDER BY (opc_id ASC);  ");
			session.execute(cql.toString());
			
			cql = new StringBuffer();
			cql.append("     CREATE MATERIALIZED VIEW IF NOT EXISTS " + keyspace + "." + "MM_OPC_MV_BY_OPC_TYPE  AS  ");
			cql.append("               SELECT * FROM " + keyspace + ".MM_OPC ");
			cql.append("                WHERE opc_type IS NOT NULL  ");
			cql.append("        		  AND opc_id IS NOT NULL  ");
			cql.append("               PRIMARY KEY (opc_type, opc_id)  ");
			cql.append("      WITH CLUSTERING ORDER BY (opc_id ASC);  ");
			session.execute(cql.toString());
			
			cql = new StringBuffer();
			cql.append("     CREATE MATERIALIZED VIEW IF NOT EXISTS " + keyspace + "." + "MM_OPC_MV_BY_SITE_ID  AS  ");
			cql.append("               SELECT * FROM " + keyspace + ".MM_OPC ");
			cql.append("                WHERE site_id IS NOT NULL  ");
			cql.append("        		  AND opc_id IS NOT NULL  ");
			cql.append("               PRIMARY KEY (site_id, opc_id)  ");
			cql.append("      WITH CLUSTERING ORDER BY (opc_id ASC);  ");
			session.execute(cql.toString());
			
			/* MM_알람 */
			
			cql = new StringBuffer();
			cql.append("     CREATE MATERIALIZED VIEW IF NOT EXISTS " + keyspace + "." + "MM_ALARM_MV_BY_ALARM_TYPE  AS  ");
			cql.append("               SELECT * FROM " + keyspace + ".MM_ALARM ");
			cql.append("                WHERE alarm_type IS NOT NULL  ");
			cql.append("        		  AND tag_id IS NOT NULL  ");
			cql.append("               PRIMARY KEY (alarm_type, tag_id)  ");
			cql.append("      WITH CLUSTERING ORDER BY (tag_id ASC);  ");
			session.execute(cql.toString());
			

			cql = new StringBuffer();
			cql.append("     CREATE MATERIALIZED VIEW IF NOT EXISTS " + keyspace + "." + "MM_ALARM_MV_BY_ALARM_CONFIG_PRIORITY  AS  ");
			cql.append("               SELECT * FROM " + keyspace + ".MM_ALARM ");
			cql.append("                WHERE alarm_config_priority IS NOT NULL  ");
			cql.append("        		  AND tag_id IS NOT NULL  ");
			cql.append("               PRIMARY KEY (alarm_config_priority, tag_id)  ");
			cql.append("      WITH CLUSTERING ORDER BY (tag_id ASC);  ");
			session.execute(cql.toString());
			
			
			/* MM_메타데이터 */
			
			cql = new StringBuffer();
			cql.append("     CREATE MATERIALIZED VIEW IF NOT EXISTS " + keyspace + "." + "MM_METADATA_MV_BY_KEY  AS  ");
			cql.append("               SELECT * FROM " + keyspace + ".MM_METADATA ");
			cql.append("                WHERE key IS NOT NULL  ");
			cql.append("        		  AND object_id IS NOT NULL  ");
			cql.append("               PRIMARY KEY (key, object_id)  ");
			cql.append("      WITH CLUSTERING ORDER BY (object_id ASC);  ");
			session.execute(cql.toString());
			
			
			/* MM_태그 */
			
			cql = new StringBuffer();
			cql.append("     CREATE MATERIALIZED VIEW IF NOT EXISTS " + keyspace + ".MM_TAG_MV_BY_TAG_NAME  AS  ");
			cql.append("               SELECT * FROM " + keyspace + ".MM_TAG ");
			cql.append("               WHERE tag_name IS NOT NULL  ");
			cql.append("        		  AND tag_id IS NOT NULL  ");
			cql.append("               PRIMARY KEY (tag_name, tag_id)  ");
			cql.append("      WITH CLUSTERING ORDER BY (tag_id ASC);  ");
			session.execute(cql.toString());
			
			cql = new StringBuffer();
			cql.append("     CREATE MATERIALIZED VIEW IF NOT EXISTS " + keyspace + ".MM_TAG_MV_BY_LINKED_ASSET_ID  AS  ");
			cql.append("               SELECT * FROM " + keyspace + ".MM_TAG ");
			cql.append("               WHERE linked_asset_id IS NOT NULL  ");
			cql.append("        		  AND tag_id IS NOT NULL  ");
			cql.append("               PRIMARY KEY (linked_asset_id, tag_id)  ");
			cql.append("      WITH CLUSTERING ORDER BY (tag_id ASC);  ");
			session.execute(cql.toString());
			
			
			cql = new StringBuffer();
			cql.append("     CREATE MATERIALIZED VIEW IF NOT EXISTS " + keyspace + ".MM_TAG_MV_BY_SITE_ID  AS  ");
			cql.append("               SELECT * FROM " + keyspace + ".MM_TAG ");
			cql.append("               WHERE site_id IS NOT NULL  ");
			cql.append("        		  AND tag_id IS NOT NULL  ");
			cql.append("               PRIMARY KEY (site_id, tag_id)  ");
			cql.append("      WITH CLUSTERING ORDER BY (tag_id ASC);  ");
			session.execute(cql.toString());
			
			
			cql = new StringBuffer();
			cql.append("     CREATE MATERIALIZED VIEW IF NOT EXISTS " + keyspace + ".MM_TAG_MV_BY_OPC_ID  AS  ");
			cql.append("               SELECT * FROM " + keyspace + ".MM_TAG ");
			cql.append("               WHERE opc_id IS NOT NULL  ");
			cql.append("        		  AND tag_id IS NOT NULL  ");
			cql.append("               PRIMARY KEY (opc_id, tag_id)  ");
			cql.append("      WITH CLUSTERING ORDER BY (tag_id ASC);  ");
			session.execute(cql.toString());
			
		
			cql = new StringBuffer();
			cql.append("     CREATE MATERIALIZED VIEW IF NOT EXISTS " + keyspace + ".MM_TAG_MV_BY_JAVA_TYPE  AS  ");
			cql.append("               SELECT * FROM " + keyspace + ".MM_TAG ");
			cql.append("               WHERE java_type IS NOT NULL  ");
			cql.append("        		  AND tag_id IS NOT NULL  ");
			cql.append("               PRIMARY KEY (java_type, tag_id)  ");
			cql.append("      WITH CLUSTERING ORDER BY (tag_id ASC);  ");
			session.execute(cql.toString());
			
			cql = new StringBuffer();
			cql.append("     CREATE MATERIALIZED VIEW IF NOT EXISTS " + keyspace + ".MM_TAG_MV_BY_UNIT  AS  ");
			cql.append("               SELECT * FROM " + keyspace + ".MM_TAG ");
			cql.append("               WHERE unit IS NOT NULL  ");
			cql.append("        		  AND tag_id IS NOT NULL  ");
			cql.append("               PRIMARY KEY (unit, tag_id)  ");
			cql.append("      WITH CLUSTERING ORDER BY (tag_id ASC);  ");
			session.execute(cql.toString());
			
			cql = new StringBuffer();
			cql.append("     CREATE MATERIALIZED VIEW IF NOT EXISTS " + keyspace + ".MM_TAG_MV_BY_ALIAS_NAME  AS  ");
			cql.append("               SELECT * FROM " + keyspace + ".MM_TAG ");
			cql.append("               WHERE alias_name IS NOT NULL  ");
			cql.append("        		  AND tag_id IS NOT NULL  ");
			cql.append("               PRIMARY KEY (alias_name, tag_id)  ");
			cql.append("      WITH CLUSTERING ORDER BY (tag_id ASC);  ");
			session.execute(cql.toString());
			
			
			
			/* --------------------------------------------------------------------- */
			/* 에셋 테이블 */
			/* --------------------------------------------------------------------- */
			
			/* 에셋 데이터 */
			/*
			cql = new StringBuffer();
			cql.append("      CREATE MATERIALIZED VIEW IF NOT EXISTS    ");
			cql.append("        " + keyspace + "." + StorageConstants.ASSET_DATA_TABLE + "_MV_BY_ASSET_TYPE  AS    ");
			cql.append("     			          SELECT * FROM " + keyspace + "." + StorageConstants.ASSET_DATA_TABLE + " ");
			cql.append("     			           WHERE asset_id IS NOT NULL   ");
			cql.append("     		   		         AND asset_type IS NOT NULL   ");
			cql.append("     						 AND date IS NOT NULL          ");
			cql.append("     						 AND hour IS NOT NULL          ");
			cql.append("     			     		 AND timestamp IS NOT NULL      ");
			cql.append("     			            PRIMARY KEY ((asset_type, date, hour), asset_id,  timestamp)   "); //HOUR를 파티션 키로 줘야, 라지 파티션이 안생김!
			cql.append("     			          WITH CLUSTERING ORDER BY (asset_id ASC, timestamp DESC); ");
			session.execute(cql.toString());
			*/
						
			/* 에셋 알람 */
			cql = new StringBuffer();
			cql.append("     CREATE MATERIALIZED VIEW IF NOT EXISTS " + keyspace + "." + StorageConstants.ASSET_ALARM_TABLE + "_MV_BY_ASSET_ID  AS  ");
			cql.append("            SELECT * FROM "  + keyspace + "." + StorageConstants.ASSET_ALARM_TABLE +  "  ");
			cql.append("            WHERE asset_id IS NOT NULL   ");
			cql.append("     		  AND alias_name IS NOT NULL ");
			cql.append("     		  AND timestamp IS NOT NULL ");
			cql.append("            PRIMARY KEY (asset_id, timestamp, alias_name) ");
			cql.append("            WITH CLUSTERING ORDER BY (timestamp DESC, alias_name ASC); ");
			session.execute(cql.toString());
			//
			
			cql = new StringBuffer();
			cql.append("     CREATE MATERIALIZED VIEW IF NOT EXISTS " + keyspace + "." + StorageConstants.ASSET_ALARM_TABLE + "_MV_BY_ALIAS_NAME  AS ");
			cql.append("            SELECT * FROM "  + keyspace + "." + StorageConstants.ASSET_ALARM_TABLE +  " ");
			cql.append("            WHERE asset_id IS NOT NULL  ");
			cql.append("     		  AND alias_name IS NOT NULL ");
			cql.append("     		  AND timestamp IS NOT NULL ");
			cql.append("            PRIMARY KEY (alias_name, timestamp, asset_id) ");
			cql.append("            WITH CLUSTERING ORDER BY (timestamp DESC, asset_id ASC)  ");
			session.execute(cql.toString());
			//
				
			cql = new StringBuffer();
			cql.append("     CREATE MATERIALIZED VIEW IF NOT EXISTS "+ keyspace + "." + StorageConstants.ASSET_ALARM_TABLE + "_MV_BY_SITE_ID  AS  ");
			cql.append("            SELECT * FROM "  + keyspace + "." + StorageConstants.ASSET_ALARM_TABLE +  "    ");
			cql.append("            WHERE asset_id IS NOT NULL     ");
			cql.append("     		  and alias_name is not null   ");
			cql.append("     		  and timestamp is not null    ");
			cql.append("     		  and site_id  is not null     ");
			cql.append("            PRIMARY KEY (site_id, timestamp,  asset_id, alias_name)    ");
			cql.append("            WITH CLUSTERING ORDER BY (timestamp DESC,  asset_id ASC, alias_name ASC);    ");
			session.execute(cql.toString());
			//
			
			cql = new StringBuffer();	   
			cql.append("     CREATE MATERIALIZED VIEW IF NOT EXISTS " + keyspace + "." + StorageConstants.ASSET_ALARM_TABLE + "_MV_BY_AREA_ID  AS ");
			cql.append("            SELECT * FROM "  + keyspace + "." + StorageConstants.ASSET_ALARM_TABLE +  " ");
			cql.append("            WHERE asset_id IS NOT NULL  ");
			cql.append("     		  AND alias_name IS NOT NULL ");
			cql.append("     		  AND timestamp IS NOT NULL ");
			cql.append("     		  AND area_id  IS NOT NULL ");
			cql.append("            PRIMARY KEY (area_id, timestamp,  asset_id, alias_name) ");
			cql.append("            WITH CLUSTERING ORDER BY (timestamp DESC,  asset_id ASC,  alias_name ASC); ");
			session.execute(cql.toString());
			//
		   
			
			cql = new StringBuffer();
			cql.append("     CREATE MATERIALIZED VIEW IF NOT EXISTS " + keyspace + "." + StorageConstants.ASSET_ALARM_TABLE + "_MV_BY_ALARM_CONFIG_ID  AS    ");
			cql.append("            SELECT * FROM "  + keyspace + "." + StorageConstants.ASSET_ALARM_TABLE +  "    ");
			cql.append("            WHERE asset_id IS NOT NULL     ");
			cql.append("     		  and alias_name is not null    ");
			cql.append("     		  and timestamp is not null    ");
			cql.append("     		  and alarm_config_id is not null    ");
			cql.append("            PRIMARY KEY (alarm_config_id, timestamp, asset_id, alias_name)    ");
			cql.append("            WITH CLUSTERING ORDER BY (timestamp desc, asset_id asc, alias_name asc);    ");
			session.execute(cql.toString());
			//
				   
			cql = new StringBuffer();
			cql.append("     CREATE MATERIALIZED VIEW IF NOT EXISTS " + keyspace + "." + StorageConstants.ASSET_ALARM_TABLE + "_MV_BY_PRIORITY  AS    ");
			cql.append("            SELECT * FROM " + keyspace + "." + StorageConstants.ASSET_ALARM_TABLE + "    ");
			cql.append("            WHERE asset_id IS NOT NULL     ");
			cql.append("     		  AND alias_name IS NOT NULL    ");
			cql.append("     		  AND timestamp IS NOT NULL    ");
			cql.append("     		  AND priority IS NOT NULL    ");
			cql.append("            PRIMARY KEY (priority, timestamp, asset_id, alias_name)    ");
			cql.append("            WITH CLUSTERING ORDER BY (timestamp DESC, asset_id ASC,  alias_name ASC);    ");
			session.execute(cql.toString());
			
			/* 에셋 이벤트  */
			cql = new StringBuffer();
			cql.append("     CREATE MATERIALIZED VIEW IF NOT EXISTS " + keyspace + "." + StorageConstants.ASSET_EVENT_TABLE + "_MV_BY_EVENT  AS    ");
			cql.append("            SELECT * FROM " + keyspace + "." + StorageConstants.ASSET_EVENT_TABLE + "    ");
			cql.append("            WHERE asset_id IS NOT NULL     ");
			cql.append("     		  AND timestamp IS NOT NULL    ");
			cql.append("     		  AND event IS NOT NULL    ");
			cql.append("            PRIMARY KEY (event, timestamp, asset_id)    ");
			cql.append("            WITH CLUSTERING ORDER BY (timestamp DESC, asset_id ASC);    ");
			session.execute(cql.toString());
			
			cql = new StringBuffer();
			cql.append("     CREATE MATERIALIZED VIEW IF NOT EXISTS " + keyspace + "." + StorageConstants.ASSET_EVENT_TABLE + "_MV_BY_STATEMENT_NAME  AS    ");
			cql.append("            SELECT * FROM " + keyspace + "." + StorageConstants.ASSET_EVENT_TABLE + "    ");
			cql.append("            WHERE asset_id IS NOT NULL     ");
			cql.append("     		  AND timestamp IS NOT NULL    ");
			cql.append("     		  AND statement_name IS NOT NULL    ");
			cql.append("            PRIMARY KEY (statement_name, timestamp, asset_id)    ");
			cql.append("            WITH CLUSTERING ORDER BY (timestamp DESC, asset_id ASC);    ");
			session.execute(cql.toString());
			
			cql = new StringBuffer();
			cql.append("     CREATE MATERIALIZED VIEW IF NOT EXISTS " + keyspace + "." + StorageConstants.ASSET_EVENT_TABLE + "_MV_BY_COLOR  AS    ");
			cql.append("            SELECT * FROM " + keyspace + "." + StorageConstants.ASSET_EVENT_TABLE + "    ");
			cql.append("            WHERE asset_id IS NOT NULL     ");
			cql.append("     		  AND timestamp IS NOT NULL    ");
			cql.append("     		  AND color IS NOT NULL    ");
			cql.append("            PRIMARY KEY (color, timestamp, asset_id)    ");
			cql.append("            WITH CLUSTERING ORDER BY (timestamp DESC, asset_id ASC);    ");
			session.execute(cql.toString());
			
			/* 에셋 상태  */
			cql = new StringBuffer();
			cql.append("     CREATE MATERIALIZED VIEW IF NOT EXISTS " + keyspace + "." + StorageConstants.ASSET_CONTEXT_TABLE + "_MV_BY_CONTEXT  AS    ");
			cql.append("            SELECT * FROM " + keyspace + "." + StorageConstants.ASSET_CONTEXT_TABLE + "    ");
			cql.append("            WHERE asset_id IS NOT NULL     ");
			cql.append("     		  AND timestamp IS NOT NULL    ");
			cql.append("     		  AND context IS NOT NULL    ");
			cql.append("     		  AND key IS NOT NULL    ");
			cql.append("            PRIMARY KEY (context, timestamp, asset_id, key)    ");
			cql.append("            WITH CLUSTERING ORDER BY (timestamp DESC, asset_id ASC, key ASC);    ");
			session.execute(cql.toString());
			
			cql = new StringBuffer();
			cql.append("     CREATE MATERIALIZED VIEW IF NOT EXISTS " + keyspace + "." + StorageConstants.ASSET_CONTEXT_TABLE + "_MV_BY_STATEMENT_NAME  AS    ");
			cql.append("            SELECT * FROM " + keyspace + "." + StorageConstants.ASSET_CONTEXT_TABLE + "    ");
			cql.append("            WHERE asset_id IS NOT NULL     ");
			cql.append("     		  AND timestamp IS NOT NULL    ");
			cql.append("     		  AND statement_name IS NOT NULL    ");
			cql.append("     		  AND context IS NOT NULL    ");
			cql.append("     		  AND key IS NOT NULL    ");
			cql.append("            PRIMARY KEY (statement_name, timestamp, asset_id, context, key)    ");
			cql.append("            WITH CLUSTERING ORDER BY (timestamp DESC, asset_id ASC, context ASC, key ASC);    ");
			session.execute(cql.toString());
			
			cql = new StringBuffer();
			cql.append("     CREATE MATERIALIZED VIEW IF NOT EXISTS " + keyspace + "." + StorageConstants.ASSET_CONTEXT_TABLE + "_MV_BY_KEY  AS    ");
			cql.append("            SELECT * FROM " + keyspace + "." + StorageConstants.ASSET_CONTEXT_TABLE + "    ");
			cql.append("            WHERE asset_id IS NOT NULL     ");
			cql.append("     		  AND timestamp IS NOT NULL    ");
			cql.append("     		  AND context IS NOT NULL    ");
			cql.append("     		  AND key IS NOT NULL    ");
			cql.append("            PRIMARY KEY (key, timestamp, asset_id, context)    ");
			cql.append("            WITH CLUSTERING ORDER BY (timestamp DESC, asset_id ASC, context ASC);    ");
			session.execute(cql.toString());
			
			/* 에셋 집계  */
			cql = new StringBuffer();
			cql.append("     CREATE MATERIALIZED VIEW IF NOT EXISTS " + keyspace + "." + StorageConstants.ASSET_AGGREGATION_TABLE + "_MV_BY_AGGREGATION  AS    ");
			cql.append("            SELECT * FROM " + keyspace + "." + StorageConstants.ASSET_AGGREGATION_TABLE + "    ");
			cql.append("            WHERE asset_id IS NOT NULL     ");
			cql.append("     		  AND timestamp IS NOT NULL    ");
			cql.append("     		  AND aggregation IS NOT NULL    ");
			cql.append("     		  AND key IS NOT NULL    ");
			cql.append("            PRIMARY KEY (aggregation, timestamp, asset_id, key)    ");
			cql.append("            WITH CLUSTERING ORDER BY (timestamp DESC, asset_id ASC, key ASC);    ");
			session.execute(cql.toString());
			
			cql = new StringBuffer();
			cql.append("     CREATE MATERIALIZED VIEW IF NOT EXISTS " + keyspace + "." + StorageConstants.ASSET_AGGREGATION_TABLE + "_MV_BY_STATEMENT_NAME  AS    ");
			cql.append("            SELECT * FROM " + keyspace + "." + StorageConstants.ASSET_AGGREGATION_TABLE + "    ");
			cql.append("            WHERE asset_id IS NOT NULL     ");
			cql.append("     		  AND timestamp IS NOT NULL    ");
			cql.append("     		  AND statement_name IS NOT NULL    ");
			cql.append("     		  AND aggregation IS NOT NULL    ");
			cql.append("     		  AND key IS NOT NULL    ");
			cql.append("            PRIMARY KEY (statement_name, timestamp, asset_id, aggregation, key)    ");
			cql.append("            WITH CLUSTERING ORDER BY (timestamp DESC, asset_id ASC, aggregation ASC, key ASC);    ");
			session.execute(cql.toString());
			
			cql = new StringBuffer();
			cql.append("     CREATE MATERIALIZED VIEW IF NOT EXISTS " + keyspace + "." + StorageConstants.ASSET_AGGREGATION_TABLE + "_MV_BY_KEY  AS    ");
			cql.append("            SELECT * FROM " + keyspace + "." + StorageConstants.ASSET_AGGREGATION_TABLE + "    ");
			cql.append("            WHERE asset_id IS NOT NULL     ");
			cql.append("     		  AND timestamp IS NOT NULL    ");
			cql.append("     		  AND aggregation IS NOT NULL    ");
			cql.append("     		  AND key IS NOT NULL    ");
			cql.append("            PRIMARY KEY (key, timestamp, asset_id, aggregation)    ");
			cql.append("            WITH CLUSTERING ORDER BY (timestamp DESC, asset_id ASC, aggregation ASC);    ");
			session.execute(cql.toString());
			
			/* 에셋 타임라인  */
			cql = new StringBuffer();
			cql.append("     CREATE MATERIALIZED VIEW IF NOT EXISTS " + keyspace + "." + StorageConstants.ASSET_TIMELINE_TABLE + "_MV_BY_STATEMENT_NAME  AS    ");
			cql.append("            SELECT * FROM " + keyspace + "." + StorageConstants.ASSET_TIMELINE_TABLE + "    ");
			cql.append("            WHERE asset_id IS NOT NULL     ");
			cql.append("     		  AND timestamp IS NOT NULL    ");
			cql.append("     		  AND statement_name IS NOT NULL    ");
			cql.append("            PRIMARY KEY (statement_name, timestamp, asset_id)    ");
			cql.append("            WITH CLUSTERING ORDER BY (timestamp DESC, asset_id ASC);    ");
			session.execute(cql.toString());
			
			cql = new StringBuffer();
			cql.append("     CREATE MATERIALIZED VIEW IF NOT EXISTS " + keyspace + "." + StorageConstants.ASSET_TIMELINE_TABLE + "_MV_BY_TIMELINE_TYPE   AS    ");
			cql.append("            SELECT * FROM " + keyspace + "." + StorageConstants.ASSET_TIMELINE_TABLE + "    ");
			cql.append("            WHERE asset_id IS NOT NULL     ");
			cql.append("     		  AND timestamp IS NOT NULL    ");
			cql.append("     		  AND timeline_type  IS NOT NULL    ");
			cql.append("            PRIMARY KEY (timeline_type , timestamp, asset_id)    ");
			cql.append("            WITH CLUSTERING ORDER BY (timestamp DESC, asset_id ASC);    ");
			session.execute(cql.toString());
			
			
			// 
			log.info("Cassandra materialized view [" + keyspace + ".TM_ASSET_*_MV_BY_*" + "] is created.");
				
		} catch (Exception ex) {
			log.error("Cassandra materialized view create error : " + ex.getMessage(), ex);
			throw ex;
		} finally {
			
		}
	};
	
	
	public void setupTableCompactionStrategy() throws Exception{
		try {

			Session session    = CassandraSessionFixer.getSession();
			String keyspace    = CassandraSessionFixer.getKeyspace();
			//
			Properties s_props = PropertiesLoader.getStorage_properties();
			String compaction_strategy = s_props.getProperty("storage.table.compaction.strategy", "TWCS");
			
			if(compaction_strategy.equals("TWCS")) { //TWCS
				session.execute("ALTER TABLE " + keyspace + "." + StorageConstants.TAG_POINT_TABLE  
						+ " WITH compaction = { 'class': 'TimeWindowCompactionStrategy', 'compaction_window_size': '24', 'compaction_window_unit': 'HOURS', 'min_threshold': '4', 'max_threshold': '32', 'unchecked_tombstone_compaction' : 'true' }  ");
				log.info("Cassandra table compaction strategy setting completed : compaction=[" + compaction_strategy + "]");
			};
			
			
			if(compaction_strategy.equals("STCS")) { //일경우 변경
				session.execute("ALTER TABLE " + keyspace + "." + StorageConstants.TAG_POINT_TABLE 
						+ " WITH compaction = { 'class': 'SizeTieredCompactionStrategy', 'min_threshold': '4', 'max_threshold': '32', 'unchecked_tombstone_compaction' : 'true' } ");
				log.info("Cassandra table compaction strategy setting completed : compaction=[" + compaction_strategy + "]");
			};
			// 
		
				
		} catch (Exception ex) {
			log.error("Cassandra table compaction strategy setting error : " + ex.getMessage(), ex);
			throw ex;
		} finally {
			
		}
	}
	
	
	
	public void setupTableComression() throws Exception{
		try {

			Session session    = CassandraSessionFixer.getSession();
			String keyspace    = CassandraSessionFixer.getKeyspace();
			
			//큰 테이블 압축 옵션 변경
			session.execute("ALTER TABLE " + keyspace + "." + StorageConstants.TAG_POINT_TABLE            + " WITH compression = { 'class' : 'ZstdCompressor', 'chunk_length_in_kb': '4', 'compression_level': '" + ZSTD_COMPRESSION_LEVEL + "' } ");
			session.execute("ALTER TABLE " + keyspace + "." + StorageConstants.TAG_BLOB_OBJECT_TABLE      + " WITH compression = { 'class' : 'ZstdCompressor', 'chunk_length_in_kb': '4', 'compression_level': '" + ZSTD_COMPRESSION_LEVEL + "' } ");
			session.execute("ALTER TABLE " + keyspace + "." + StorageConstants.TAG_POINT_SNAPSHOT_TABLE   + " WITH compression = { 'class' : 'ZstdCompressor', 'chunk_length_in_kb': '4', 'compression_level': '" + ZSTD_COMPRESSION_LEVEL + "' } ");
			session.execute("ALTER TABLE " + keyspace + "." + StorageConstants.ASSET_DATA_TABLE           + " WITH compression = { 'class' : 'ZstdCompressor', 'chunk_length_in_kb': '4', 'compression_level': '" + ZSTD_COMPRESSION_LEVEL + "' } ");
			//
			log.info("Cassandra table compression setting completed : compressor=[ZSTD], level=[" + ZSTD_COMPRESSION_LEVEL + "]");
				
		} catch (Exception ex) {

			log.error("Cassandra table compression level setting error : " + ex.getMessage(), ex);

			throw ex;
		} finally {
			
		}
	}
	
	public void setupTableCacheAndTTL() throws Exception{
		try {

			Session session    = CassandraSessionFixer.getSession();
			String keyspace    = CassandraSessionFixer.getKeyspace();
			
			
			Properties s_props = PropertiesLoader.getStorage_properties();
			
			// 테이블 ROW-캐시 설정
			session.execute("ALTER TABLE " + keyspace + "." + StorageConstants.TAG_POINT_TABLE  + " WITH caching = { 'keys' : 'ALL', 'rows_per_partition' : '" +  (s_props.getProperty("storage.tag.point.row.cache.size",  "NONE")) + "' }");
			session.execute("ALTER TABLE " + keyspace + "." + StorageConstants.ASSET_DATA_TABLE + " WITH caching = { 'keys' : 'ALL', 'rows_per_partition' : '" +  (s_props.getProperty("storage.asset.data.row.cache.size", "NONE")) + "'  }");
			
	
		    // 테이블 TTL 설정
			session.execute("ALTER TABLE " + keyspace + "."  + StorageConstants.TAG_POINT_TABLE           + " WITH  default_time_to_live = " +  ((Integer.parseInt(s_props.getProperty("storage.tag.point.ttl", "0"))) * 86400)  + " ");
			session.execute("ALTER TABLE " + keyspace + "."  + StorageConstants.TAG_POINT_MAP_TABLE       + " WITH  default_time_to_live = " +  ((Integer.parseInt(s_props.getProperty("storage.tag.point.map.ttl", "0"))) * 86400)  + " ");
			
			session.execute("ALTER TABLE " + keyspace + "."  + StorageConstants.TAG_POINT_SNAPSHOT_TABLE  + " WITH  default_time_to_live = " +  ((Integer.parseInt(s_props.getProperty("storage.tag.point.snapshot.ttl", "0"))) * 86400)  + " ");
			session.execute("ALTER TABLE " + keyspace + "."  + StorageConstants.TAG_POINT_ARCHIVE_TABLE   + " WITH  default_time_to_live = " +  ((Integer.parseInt(s_props.getProperty("storage.tag.point.archive.ttl", "0"))) * 86400)   + " ");
			
			for (int i = 0; i < StorageConstants.SAMPLING_TERMS_ARRAY.length; i++) {
				 session.execute("ALTER TABLE "  + keyspace + "." + (StorageConstants.TAG_POINT_SAMPLING_TABLE  + "_" + StorageConstants.SAMPLING_TERMS_ARRAY[i].replace(" ", "_")) + " WITH  default_time_to_live = " +  ((Integer.parseInt(s_props.getProperty("storage.tag.point.sampling.ttl", "0"))) * 86400)  + " ");
			};
			for (int i = 0; i < StorageConstants.AGGREGATION_TERMS_ARRAY.length; i++) {
				 session.execute("ALTER TABLE "  + keyspace + "."  + (StorageConstants.TAG_POINT_AGGREGATION_TABLE  + "_" + StorageConstants.AGGREGATION_TERMS_ARRAY[i].replace(" ", "_")) + " WITH  default_time_to_live = " +  ((Integer.parseInt(s_props.getProperty("storage.tag.point.aggregation.ttl", "0"))) * 86400)  + " ");
			};
			session.execute("ALTER TABLE "  + keyspace + "."  + StorageConstants.ASSET_DATA_TABLE  + " WITH  default_time_to_live = " +  ((Integer.parseInt(s_props.getProperty("storage.asset.data.ttl", "0"))) * 86400)  + " ");
			
			session.execute("ALTER TABLE " + keyspace + "."  + StorageConstants.TAG_BLOB_TABLE     + " WITH  default_time_to_live = " +  ((Integer.parseInt(s_props.getProperty("storage.tag.blob.ttl", "0"))) * 86400)  + " ");
			
			// 
			log.info("Cassandra table CACHE and TTL setting completed : properties=[" + s_props.toString() + "]");
				
		} catch (Exception ex) {
			log.error("Cassandra table CACHE and TTL setting error : " + ex.getMessage(), ex);
			throw ex;
		} finally {
			
		}
	};
	
	
	public void setupReplicationFactor(int factor) throws Exception{
		try {

			Session session    = CassandraSessionFixer.getSession();
			String  keyspace    = CassandraSessionFixer.getKeyspace();

			session.execute("ALTER KEYSPACE " + keyspace + "           WITH replication = { 'class': 'NetworkTopologyStrategy','datacenter1': " + factor + " } ;");
			session.execute("ALTER KEYSPACE " + keyspace + "_el        WITH replication = { 'class': 'NetworkTopologyStrategy','datacenter1': " + factor + "  } ;");
			session.execute("ALTER KEYSPACE " + keyspace + "_mon       WITH replication = { 'class': 'NetworkTopologyStrategy','datacenter1': " + factor + "  } ;");
			session.execute("ALTER KEYSPACE " + keyspace + "_system    WITH replication = { 'class': 'NetworkTopologyStrategy','datacenter1': " + factor + "  } ;");
			session.execute("ALTER KEYSPACE pt           WITH replication = { 'class': 'NetworkTopologyStrategy','datacenter1': " + factor + "  } ;");
			session.execute("ALTER KEYSPACE system_auth  WITH replication = { 'class': 'NetworkTopologyStrategy','datacenter1': " + factor + "  } ;");
		
			// 
			log.info("Cassandra replication_factor change completed : factor=[" + factor + "]");
				
		} catch (Exception ex) {
			log.error("Cassandra replication_factor change error : " + ex.getMessage(), ex);
			throw ex;
		} finally {
			
		}
	};
	
	
	
	/*
	 * 
	 */
	@Override
	public void updateSystemRunDuration() throws Exception {
		//

		Session session = null;
		StringBuffer cql = new StringBuffer();
		//
		try {

			session     = CassandraSessionFixer.getSession();
			String keyspace    = CassandraSessionFixer.getKeyspace();

			long server_start_timestamp = CEPEngineManager.getInstance().getStart_date().getTime();
			long current_timestamp = System.currentTimeMillis();
			long running_seconds = TimeUnit.MILLISECONDS.toSeconds((current_timestamp - server_start_timestamp));
			session.executeAsync("UPDATE " + keyspace + "." + StorageConstants.OPTION_SYSTEM_START_DATE + " SET run_duration  = " + running_seconds + " WHERE timestamp = "  + server_start_timestamp ) ;
			

		} catch (Exception ex) {
			log.error("Update system run duration error : CQL=[" + cql.toString() + "], " + ex.getMessage(), ex);
			throw ex;
		} finally {
			//
			// if (session != null && !session.isClosed())
			// session.close();
		}
	}
	
	
	
	/**
	 * 메터데이터 업데이트
	 */
	public void updateMetadata(MetaModel meta) throws Exception {

		Session session = null;
		//
		try {

			session     = CassandraSessionFixer.getSession();
			String keyspace    = CassandraSessionFixer.getKeyspace();
			
			//
			StringBuffer cql = new StringBuffer();
	
			
			//MM_SITE
			session.execute("TRUNCATE  " + keyspace + "." + StorageConstants.SITE_TABLE);
			for( Map.Entry<String, Site> sites : meta.getSite_map().entrySet() ){
				
	            Site site = sites.getValue();
	            cql = new StringBuffer();
	 			cql.append(" INSERT INTO " + keyspace + "." + StorageConstants.SITE_TABLE + " ( ");
	 			cql.append("		  " + CassandraColumns.SITE_ID   + "     ,  ");
	 			cql.append("		  " + CassandraColumns.SITE_NAME + "     ,  ");
	 			cql.append("		  " + CassandraColumns.DESCRIPTION + "   ,  ");
	 			cql.append("		  " + CassandraColumns.LAT + "     ,  ");
	 			cql.append("		  " + CassandraColumns.LNG + "     ,  ");
	 			cql.append("		  " + CassandraColumns.INSERT_DATE + "     ,  ");
	 			cql.append("		  " + CassandraColumns.UPDATE_DATE + "      ");
	 			cql.append(" ) VALUES (   ");
	 			cql.append(" ?,   ");
	 			cql.append(" ?,   ");
	 			cql.append(" ?,   ");
	 			cql.append(" ?,   ");
	 			cql.append(" ?,   ");
	 			cql.append(" ?,   ");
	 			cql.append(" ?    ");
	 			cql.append(" );   ");
	 			
	 			PreparedStatement ps = PreparedStatementCacheFactory.getInstance().get(session, cql.toString(), ConsistencyLevel.ONE);

				BoundStatement query = new BoundStatement(ps);
				query.setString(CassandraColumns.SITE_ID, site.getSite_id());
				query.setString(CassandraColumns.SITE_NAME, site.getSite_name());
				query.setString(CassandraColumns.DESCRIPTION, site.getDescription());
				query.setString(CassandraColumns.LAT, (site.getLat()));
				query.setString(CassandraColumns.LNG, (site.getLng()));
				query.setTimestamp(CassandraColumns.INSERT_DATE,  DateUtils.stringToDate("yyyy-MM-dd", site.getInsert_date()));
				query.setTimestamp(CassandraColumns.UPDATE_DATE,  DateUtils.stringToDate("yyyy-MM-dd", site.getUpdate_date()));
				
				session.executeAsync(query);
	        };
			
	
	      //MM_OPC
	      session.execute("TRUNCATE  " + keyspace + "." + StorageConstants.OPC_TABLE);
		  for( Map.Entry<String, OPC> opcs : meta.getOpc_map().entrySet() ){
				
	            OPC opc = opcs.getValue();
	            cql = new StringBuffer();
	 			cql.append(" INSERT INTO " + keyspace + "." + StorageConstants.OPC_TABLE + " ( ");
	 			cql.append("		  " + CassandraColumns.OPC_ID   + "        ,  ");
				cql.append("		  " + CassandraColumns.SITE_ID   + "       ,  ");
				cql.append("		  " + CassandraColumns.OPC_NAME + "        ,  ");
				cql.append("		  " + CassandraColumns.OPC_TYPE + "        ,  ");
				cql.append("		  " + CassandraColumns.OPC_SERVER_IP + "   ,  ");
				cql.append("		  " + CassandraColumns.DESCRIPTION + "     ,  ");
				cql.append("		  " + CassandraColumns.INSERT_DATE + "     ,  ");
				cql.append("		  " + CassandraColumns.UPDATE_DATE + "        ");
	 			cql.append(" ) VALUES (   ");
	 			cql.append(" ?,   ");
	 			cql.append(" ?,   ");
	 			cql.append(" ?,   ");
	 			cql.append(" ?,   ");
	 			cql.append(" ?,   ");
	 			cql.append(" ?,   ");
	 			cql.append(" ?,   ");
	 			cql.append(" ?    ");
	 			cql.append(" );   ");
	 			
	 			PreparedStatement ps = PreparedStatementCacheFactory.getInstance().get(session, cql.toString(), ConsistencyLevel.ONE);

				BoundStatement query = new BoundStatement(ps);
				query.setString(CassandraColumns.OPC_ID, opc.getOpc_id());
				query.setString(CassandraColumns.SITE_ID, opc.getSite_id());
				query.setString(CassandraColumns.OPC_NAME, opc.getOpc_name());
				query.setString(CassandraColumns.OPC_TYPE, opc.getOpc_type());
				query.setString(CassandraColumns.OPC_SERVER_IP, opc.getOpc_server_ip());
				query.setString(CassandraColumns.DESCRIPTION, opc.getDescription());
				query.setTimestamp(CassandraColumns.INSERT_DATE,  DateUtils.stringToDate("yyyy-MM-dd", opc.getInsert_date()));
				query.setTimestamp(CassandraColumns.UPDATE_DATE,  DateUtils.stringToDate("yyyy-MM-dd", opc.getUpdate_date()));
				
				session.executeAsync(query);
	        };
	        
	        
	      //에셋 설정
	        session.execute("TRUNCATE  " + keyspace + "." + StorageConstants.ASSET_TABLE);
		    for( Map.Entry<String, Asset> assets : meta.getAsset_map().entrySet() ){
				
	            Asset asset = assets.getValue();
	            cql = new StringBuffer();
	 			cql.append(" INSERT INTO " + keyspace + "." + StorageConstants.ASSET_TABLE + " ( ");
	 			cql.append("		  " + CassandraColumns.ASSET_ID   + "     ,  ");
				cql.append("		  " + CassandraColumns.PARENT_ASSET_ID   + "     ,  ");
				cql.append("		  " + CassandraColumns.SITE_ID   + "       ,  ");
				cql.append("		  " + CassandraColumns.ASSET_NAME + "      ,  ");
				cql.append("		  " + CassandraColumns.ASSET_TYPE + "      ,  ");
				cql.append("		  " + CassandraColumns.ASSET_ORDER + "     ,  ");
				cql.append("		  " + CassandraColumns.ASSET_SVG_IMG  + "  ,  ");
				cql.append("		  " + CassandraColumns.TABLE_TYPE     + "  ,  ");
				cql.append("		  " + CassandraColumns.DESCRIPTION     + "  ,  ");
				cql.append("		  " + CassandraColumns.INSERT_DATE + "     ,  ");
				cql.append("		  " + CassandraColumns.UPDATE_DATE + "       ");
	 			cql.append(" ) VALUES (   ");
	 			cql.append(" ?,   ");
	 			cql.append(" ?,   ");
	 			cql.append(" ?,   ");
	 			cql.append(" ?,   ");
	 			cql.append(" ?,   ");
	 			cql.append(" ?,   ");
	 			cql.append(" ?,   ");
	 			cql.append(" ?,   ");
	 			cql.append(" ?,   ");
	 			cql.append(" ?,   ");
	 			cql.append(" ?    ");
	 			cql.append(" );   ");
	 			
	 			PreparedStatement ps = PreparedStatementCacheFactory.getInstance().get(session, cql.toString(), ConsistencyLevel.ONE);

				BoundStatement query = new BoundStatement(ps);
				query.setString(CassandraColumns.ASSET_ID, asset.getAsset_id());
				query.setString(CassandraColumns.PARENT_ASSET_ID, asset.getParent_asset_id());
				query.setString(CassandraColumns.SITE_ID, asset.getSite_id());
				query.setString(CassandraColumns.ASSET_NAME, asset.getAsset_name());
				query.setString(CassandraColumns.ASSET_TYPE, asset.getAsset_type());
				query.setInt(CassandraColumns.ASSET_ORDER,  NumberUtils.toInt(asset.getAsset_order(), 0));
				query.setString(CassandraColumns.ASSET_SVG_IMG, asset.getAsset_svg_img());
				query.setString(CassandraColumns.TABLE_TYPE, asset.getTable_type());
				query.setString(CassandraColumns.DESCRIPTION,    asset.getDescription());
				query.setTimestamp(CassandraColumns.INSERT_DATE,  DateUtils.stringToDate("yyyy-MM-dd", asset.getInsert_date()));
				query.setTimestamp(CassandraColumns.UPDATE_DATE,  DateUtils.stringToDate("yyyy-MM-dd", asset.getUpdate_date()));
				
				session.executeAsync(query);
				
	        };
	
	        //TAG_TABLE
	        session.execute("TRUNCATE  " + keyspace + "." + StorageConstants.TAG_TABLE);
			for( Map.Entry<String, Tag> tags : meta.getTag_map().entrySet() ){
					
		            Tag tag = tags.getValue();
		            cql = new StringBuffer();
		 			cql.append(" INSERT INTO " + keyspace + "." + StorageConstants.TAG_TABLE + " ( ");
		 			cql.append("		  " + CassandraColumns.TAG_ID   + "     ,  ");
					cql.append("		  " + CassandraColumns.OPC_ID   + "     ,  ");
					cql.append("		  " + CassandraColumns.SITE_ID   + "    ,  ");
					cql.append("		  " + CassandraColumns.TAG_NAME + "     ,  ");
					cql.append("		  " + CassandraColumns.TAG_SOURCE + "   ,  ");
					cql.append("		  " + CassandraColumns.JAVA_TYPE + "    ,  ");
					cql.append("		  " + CassandraColumns.ALIAS_NAME + "   ,  ");
					cql.append("		  " + CassandraColumns.IMPORTANCE + "   ,  ");
					cql.append("		  " + CassandraColumns.INTERVAL + "     ,  ");
					cql.append("		  " + CassandraColumns.UNIT + "      ,  ");
					cql.append("		  " + CassandraColumns.TRIP_HI + "   ,  ");
					cql.append("		  " + CassandraColumns.HI + "        ,  ");
					cql.append("		  " + CassandraColumns.HI_HI + "     ,  ");
					cql.append("		  " + CassandraColumns.LO + "        ,  ");
					cql.append("		  " + CassandraColumns.LO_LO + "     ,  ");
					cql.append("		  " + CassandraColumns.TRIP_LO + "     ,  ");
					cql.append("		  " + CassandraColumns.MIN_VALUE + "     ,  ");
					cql.append("		  " + CassandraColumns.MAX_VALUE + "     ,  ");
					cql.append("		  " + CassandraColumns.DISPLAY_FORMAT + "     ,  ");
					cql.append("		  " + CassandraColumns.LAT + "     ,  ");
					cql.append("		  " + CassandraColumns.LNG + "     ,  ");
					cql.append("		  " + CassandraColumns.LINKED_ASSET_ID + "     ,  ");
					cql.append("		  " + CassandraColumns.DESCRIPTION + "   ,  ");
					cql.append("		  " + CassandraColumns.INSERT_DATE + "     ,  ");
					cql.append("		  " + CassandraColumns.UPDATE_DATE + "        ");
		 			cql.append(" ) VALUES (   ");
		 			cql.append(" ?,   ");
		 			cql.append(" ?,   ");
		 			cql.append(" ?,   ");
		 			cql.append(" ?,   ");
		 			cql.append(" ?,   ");
		 			cql.append(" ?,   ");
		 			cql.append(" ?,   ");
		 			cql.append(" ?,   ");
		 			cql.append(" ?,   ");
		 			cql.append(" ?,   ");
		 			cql.append(" ?,   ");
		 			cql.append(" ?,   ");
		 			cql.append(" ?,   ");
		 			cql.append(" ?,   ");
					cql.append(" ?,   ");
		 			cql.append(" ?,   ");
		 			cql.append(" ?,   ");
		 			cql.append(" ?,   ");
		 			cql.append(" ?,   ");
		 			cql.append(" ?,   ");
		 			cql.append(" ?,   ");
		 			cql.append(" ?,   ");
		 			cql.append(" ?,   ");
		 			cql.append(" ?,   ");
		 			cql.append(" ?    ");
		 			cql.append(" );   ");
		 			
		 			PreparedStatement ps = PreparedStatementCacheFactory.getInstance().get(session, cql.toString(), ConsistencyLevel.ONE);
					
					BoundStatement query = new BoundStatement(ps);
					query.setString(CassandraColumns.TAG_ID, tag.getTag_id());
					query.setString(CassandraColumns.OPC_ID, tag.getOpc_id());
					query.setString(CassandraColumns.SITE_ID, tag.getSite_id());
					query.setString(CassandraColumns.TAG_NAME, tag.getTag_name());
					query.setString(CassandraColumns.TAG_SOURCE, tag.getTag_source());
					query.setString(CassandraColumns.JAVA_TYPE, tag.getJava_type());
					query.setString(CassandraColumns.ALIAS_NAME, tag.getAlias_name());
					query.setInt(CassandraColumns.IMPORTANCE,   tag.getImportance());
					query.setString(CassandraColumns.INTERVAL, tag.getInterval());
					query.setString(CassandraColumns.UNIT, tag.getUnit());
					query.setString(CassandraColumns.TRIP_HI, tag.getTrip_hi());
					query.setString(CassandraColumns.HI, tag.getHi());
					query.setString(CassandraColumns.HI_HI, tag.getHi_hi());
					query.setString(CassandraColumns.LO, tag.getLo());
					query.setString(CassandraColumns.LO_LO, tag.getLo_lo());
					query.setString(CassandraColumns.TRIP_LO, tag.getTrip_lo());
					query.setString(CassandraColumns.MIN_VALUE, tag.getMin_value());
					query.setString(CassandraColumns.MAX_VALUE, tag.getMax_value());
					query.setString(CassandraColumns.DISPLAY_FORMAT, tag.getOpc_name());
					query.setString(CassandraColumns.LAT, tag.getLat());
					query.setString(CassandraColumns.LNG, tag.getLng());
					query.setString(CassandraColumns.LINKED_ASSET_ID, tag.getLinked_asset_id());
					query.setString(CassandraColumns.DESCRIPTION, tag.getDescription());
					query.setTimestamp(CassandraColumns.INSERT_DATE,  DateUtils.stringToDate("yyyy-MM-dd", tag.getInsert_date()));
					query.setTimestamp(CassandraColumns.UPDATE_DATE,  DateUtils.stringToDate("yyyy-MM-dd", tag.getUpdate_date()));
					
					session.executeAsync(query);
		    };

			
		    //알람 설정
		    session.execute("TRUNCATE  " + keyspace + "." + StorageConstants.ALARM_TABLE);
		    for( Map.Entry<String, AlarmConfig> alarms : meta.getAlarm_map().entrySet() ){
				
	            AlarmConfig alarm = alarms.getValue();
	            cql = new StringBuffer();
	 			cql.append(" INSERT INTO " + keyspace + "." + StorageConstants.ALARM_TABLE + " ( ");
				cql.append("		  " + CassandraColumns.ALARM_CONFIG_ID   + "     ,  ");
				cql.append("		  " + CassandraColumns.TAG_ID   + "     ,  ");
				cql.append("		  " + CassandraColumns.ALARM_CONFIG_NAME + "     ,  ");
				cql.append("		  " + CassandraColumns.ALARM_CONFIG_PRIORITY + "     ,  ");
				cql.append("		  " + CassandraColumns.ALARM_CONFIG_DESC + "     ,  ");
				cql.append("		  " + CassandraColumns.ALARM_TYPE + "     ,  ");
				cql.append("		  " + CassandraColumns.EPL + "     ,  ");
				cql.append("		  " + CassandraColumns.CONDITION + "     ,  ");
				cql.append("		  " + CassandraColumns.MESSAGE   + "     ,  ");
				cql.append("		  " + CassandraColumns.RECIEVE_ME + "    ,  ");
				cql.append("		  " + CassandraColumns.RECIEVE_OTHERS + "      ,  ");
				cql.append("		  " + CassandraColumns.SEND_EMAIL + "    ,  ");
				cql.append("		  " + CassandraColumns.SEND_SMS + "      ,  ");
				cql.append("		  " + CassandraColumns.DUPLICATE_CHECK + "   ,  ");
				cql.append("		  " + CassandraColumns.DUPLICATE_CHECK_TIME + "   ,  ");
				cql.append("		  " + CassandraColumns.INSERT_DATE + "     ,  ");
				cql.append("		  " + CassandraColumns.UPDATE_DATE + "       ");
	 			cql.append(" ) VALUES (   ");
	 			cql.append(" ?,   ");
	 			cql.append(" ?,   ");
	 			cql.append(" ?,   ");
	 			cql.append(" ?,   ");
	 			cql.append(" ?,   ");
	 			cql.append(" ?,   ");
	 			cql.append(" ?,   ");
	 			cql.append(" ?,   ");
	 			cql.append(" ?,   ");
	 			cql.append(" ?,   ");
	 			cql.append(" ?,   ");
	 			cql.append(" ?,   ");
	 			cql.append(" ?,   ");
	 			cql.append(" ?,   ");
	 			cql.append(" ?,   ");
	 			cql.append(" ?,   ");
	 			cql.append(" ?   ");
	 			cql.append(" );   ");
	 			
	 			PreparedStatement ps = PreparedStatementCacheFactory.getInstance().get(session, cql.toString(), ConsistencyLevel.ONE);

				BoundStatement query = new BoundStatement(ps);
				query.setString(CassandraColumns.ALARM_CONFIG_ID, alarm.getAlarm_config_id());
				query.setString(CassandraColumns.TAG_ID, alarm.getTag_id());
				query.setString(CassandraColumns.ALARM_CONFIG_NAME, alarm.getAlarm_config_name());
				query.setString(CassandraColumns.ALARM_CONFIG_PRIORITY, alarm.getAlarm_config_priority());
				query.setString(CassandraColumns.ALARM_CONFIG_DESC, alarm.getAlarm_config_desc());
				query.setString(CassandraColumns.ALARM_TYPE, alarm.getAlarm_type());
				query.setString(CassandraColumns.EPL, alarm.getEpl());
				query.setString(CassandraColumns.CONDITION, alarm.getCondition());
				query.setString(CassandraColumns.MESSAGE, alarm.getMessage());
				query.setBool(CassandraColumns.RECIEVE_ME,   alarm.isRecieve_me());
				query.setString(CassandraColumns.RECIEVE_OTHERS, alarm.getRecieve_others());
				query.setBool(CassandraColumns.SEND_EMAIL,   alarm.isSend_email());
				query.setBool(CassandraColumns.SEND_SMS,     alarm.isSend_sms());
				query.setBool(CassandraColumns.DUPLICATE_CHECK, alarm.isDuplicate_check());
				query.setInt(CassandraColumns.DUPLICATE_CHECK_TIME, alarm.getDuplicate_check_time());
				query.setTimestamp(CassandraColumns.INSERT_DATE,  (alarm.getInsert_date()));
				query.setTimestamp(CassandraColumns.UPDATE_DATE,  (alarm.getLast_update_date()));
				
				session.executeAsync(query);
	        };
	        
	        
	        
			
		


			//USER_TABLE
	        session.execute("TRUNCATE  " + keyspace + "." + StorageConstants.USER_TABLE);
		    for( Map.Entry<String, User> users : meta.getUser_map().entrySet() ){
				
		    	User user = users.getValue();
	            cql = new StringBuffer();
	 			cql.append(" INSERT INTO " + keyspace + "." + StorageConstants.USER_TABLE + " ( ");
				cql.append("		  " + CassandraColumns.USER_ID   + "     ,  ");
				cql.append("		  " + CassandraColumns.SECURITY_ID   + "     ,  ");
				cql.append("		  " + CassandraColumns.PASSWORD   + "     ,  ");
				cql.append("		  " + CassandraColumns.ROLE   + "       ,  ");
				cql.append("		  " + CassandraColumns.NAME + "      ,  ");
				cql.append("		  " + CassandraColumns.EMAIL + "      ,  ");
				cql.append("		  " + CassandraColumns.PHONE + "     ,  ");
				cql.append("		  " + CassandraColumns.INSERT_DATE + "     ,  ");
				cql.append("		  " + CassandraColumns.UPDATE_DATE + "       ");
	 			cql.append(" ) VALUES (   ");
	 			cql.append(" ?,   ");
	 			cql.append(" ?,   ");
	 			cql.append(" ?,   ");
	 			cql.append(" ?,   ");
	 			cql.append(" ?,   ");
	 			cql.append(" ?,   ");
	 			cql.append(" ?,   ");
	 			cql.append(" ?,   ");
	 			cql.append(" ?    ");
	 			cql.append(" );   ");
	 			
	 			PreparedStatement ps = PreparedStatementCacheFactory.getInstance().get(session, cql.toString(), ConsistencyLevel.ONE);

				BoundStatement query = new BoundStatement(ps);
				query.setString(CassandraColumns.USER_ID, user.getUser_id());
				query.setString(CassandraColumns.SECURITY_ID, user.getSecurity_id());
				query.setString(CassandraColumns.PASSWORD, user.getPassword());
				query.setString(CassandraColumns.ROLE, user.getRole());
				query.setString(CassandraColumns.NAME, user.getName());
				query.setString(CassandraColumns.EMAIL, user.getEmail());
				query.setString(CassandraColumns.PHONE, user.getPhone());
				query.setTimestamp(CassandraColumns.INSERT_DATE,  (user.getInsert_date()));
				query.setTimestamp(CassandraColumns.UPDATE_DATE,  (user.getLast_update_date()));
				
				session.executeAsync(query);
				
	        };

			
			
	        //SECURITY_TABLE		
	        session.execute("TRUNCATE  " + keyspace + "." + StorageConstants.SECURITY_TABLE);
            for( Map.Entry<String, Security> securites : meta.getSeucrity_map().entrySet() ){
           	 Security security = securites.getValue();
	            cql = new StringBuffer();
	 			cql.append(" INSERT INTO " + keyspace + "." + StorageConstants.SECURITY_TABLE + " ( ");
	 			cql.append("		  " + CassandraColumns.SECURITY_ID   + "     ,  ");
				cql.append("		  " + CassandraColumns.SECURITY_DESC   + "     ,  ");
				cql.append("		  " + CassandraColumns.OBJECT_PERMISSION_ARRAY   + "     ,  ");
				cql.append("		  " + CassandraColumns.INSERT_DATE + "     ,  ");
				cql.append("		  " + CassandraColumns.UPDATE_DATE + "        ");
	 			cql.append(" ) VALUES (   ");
	 			cql.append(" ?,   ");
	 			cql.append(" ?,   ");
	 			cql.append(" ?,   ");
	 			cql.append(" ?,   ");
	 			cql.append(" ?    ");
	 			cql.append(" );   ");
	 			
	 			PreparedStatement ps = PreparedStatementCacheFactory.getInstance().get(session, cql.toString(), ConsistencyLevel.ONE);

				BoundStatement query = new BoundStatement(ps);
				query.setString(CassandraColumns.SECURITY_ID, security.getSecurity_id());
				query.setString(CassandraColumns.SECURITY_DESC, security.getSecurity_desc());
				query.setString(CassandraColumns.OBJECT_PERMISSION_ARRAY, security.getObject_permission_array());
				query.setTimestamp(CassandraColumns.INSERT_DATE,  (security.getInsert_date()));
				query.setTimestamp(CassandraColumns.UPDATE_DATE,  (security.getLast_update_date()));
				
				session.executeAsync(query);
				
	        };
	        
	        
	      //MM_METADATA
			session.execute("TRUNCATE  " + keyspace + "." + StorageConstants.METADATA_TABLE);
			for( Map.Entry<String, Metadata> matadatas : meta.getMetadata_map().entrySet() ){
				
	            Metadata metadata = matadatas.getValue();
	            cql = new StringBuffer();
	 			cql.append(" INSERT INTO " + keyspace + "." + StorageConstants.METADATA_TABLE + " ( ");
	 			cql.append("		  " + CassandraColumns.OBJECT_ID   + "     ,  ");
	 			cql.append("		  " + CassandraColumns.KEY + "     ,  ");
	 			cql.append("		  " + CassandraColumns.VALUE + "   ,  ");
	 			cql.append("		  " + CassandraColumns.DESCRIPTION + "   ,  ");
	 			cql.append("		  " + CassandraColumns.INSERT_DATE + "    ");
	 			cql.append(" ) VALUES (   ");
	 			cql.append(" ?,   ");
	 			cql.append(" ?,   ");
	 			cql.append(" ?,   ");
	 			cql.append(" ?,   ");
	 			cql.append(" ?    ");
	 			cql.append(" );   ");
	 			
	 			PreparedStatement ps = PreparedStatementCacheFactory.getInstance().get(session, cql.toString(), ConsistencyLevel.ONE);

				BoundStatement query = new BoundStatement(ps);
				query.setString(CassandraColumns.OBJECT_ID, metadata.getObject_id());
				query.setString(CassandraColumns.KEY, metadata.getKey());
				query.setString(CassandraColumns.VALUE, metadata.getValue());
				query.setString(CassandraColumns.DESCRIPTION, metadata.getDescription());
				query.setTimestamp(CassandraColumns.INSERT_DATE,  DateUtils.stringToDate("yyyy-MM-dd", metadata.getInsert_date()));
				
				session.executeAsync(query);
	        };
			
	        
	        //JSON_MODEL_TABLE	
	        session.execute("TRUNCATE  " + keyspace + "." + StorageConstants.JSON_MODEL_TABLE);
            for( MetaModel.JSONModel json : meta.getJson_model()){
            	//
	            cql = new StringBuffer();
	 			cql.append(" INSERT INTO " + keyspace + "." + StorageConstants.JSON_MODEL_TABLE + " ( ");
	 			cql.append("		  " + CassandraColumns.PATH   + "     ,  ");
				cql.append("		  " + CassandraColumns.PAYLOAD    + "        ");
	 			cql.append(" ) VALUES (   ");
	 			cql.append(" ?,   ");
	 			cql.append(" ?    ");
	 			cql.append(" );   ");
	 			//
	 			PreparedStatement ps = PreparedStatementCacheFactory.getInstance().get(session, cql.toString(), ConsistencyLevel.ONE);
                BoundStatement query = new BoundStatement(ps);
				query.setString(CassandraColumns.PATH, json.getModel());
				query.setString(CassandraColumns.PAYLOAD,  json.getPayload());
				//
				session.executeAsync(query);
	        };
	        
			
			//
		} catch (Exception ex) {
			log.error("Metadata update error : " + ex.getMessage(), ex);
			throw ex;
		} finally {
			//
			// if (session != null && !session.isClosed())
			// session.close();
		}

	}
	

	/**
	 * 
	 * insertTrigger
	 * @param trigger
	 * @param timestamp
	 * @param data
	 * @throws Exception
	 */
	@Override
	public void insertTrigger(Trigger trigger, long timestamp, JSONObject data) throws Exception {
		//

		Session session = null;
		StringBuffer cql = new StringBuffer();
		//
		try {

			//
			long start = System.currentTimeMillis();

			session     = CassandraSessionFixer.getSession();
			String keyspace    = CassandraSessionFixer.getKeyspace();

			//
			if (!ObjectCreationChecker.getInstance().hasObject(trigger.getStorage_table())) {
				createTableForTrigger(trigger);
			}

			try {
				//
				cql = new StringBuffer();
				cql.append(" INSERT INTO  " + keyspace + "." + trigger.getStorage_table() + " (");
				for (int i1 = 0; trigger.getTrigger_attributes() != null && i1 < trigger.getTrigger_attributes().length; i1++) {
					TriggerAttribute attribute = trigger.getTrigger_attributes()[i1];
					cql.append("	" + attribute.getField_name() + " ");
					if (i1 != trigger.getTrigger_attributes().length - 1) {
						cql.append(", ");
					}
				}
				cql.append(" ) ");
				cql.append(" VALUES ");
				cql.append(" ( ");
				//
				for (int i2 = 0; trigger.getTrigger_attributes() != null && i2 < trigger.getTrigger_attributes().length; i2++) {
					TriggerAttribute attribute = trigger.getTrigger_attributes()[i2];
					cql.append("	" + getReplacedValue(data.getString(attribute.getField_name()), attribute.getJava_type()) + " ");
					if (i2 != trigger.getTrigger_attributes().length - 1) {
						cql.append(", ");
					}
				}
				cql.append(" ); ");
				session.executeAsync(cql.toString());

				long end = System.currentTimeMillis() - start;

				log.debug("Cassandra insert trigger data : exec_time=[" + end + "]");

				//
			} catch (Exception ex) {
				throw new Exception("Cassandra CQL execute failed : CQL=[" + cql.toString() + "], " + ex.getMessage(), ex);
			}

		} catch (Exception ex) {
			DataFlowErrorCount.ERROR_STORE_TRIGGER_INSERT.incrementAndGet();
			log.error("[" + trigger.getStorage_table() + "] trigger insert error : CQL=[" + cql.toString() + "], " + ex.getMessage(), ex);
			throw ex;
		} finally {
			//
			// if (session != null && !session.isClosed())
			// session.close();
		}
	}
	
	@Override
	public void insertPointSampling(String term, long timestamp, List<Point> point_list) throws Exception {
		//

		Session session = null;
		StringBuffer cql = new StringBuffer();
		String sampling_table = "";
		//
		try {

			//
			long start = System.currentTimeMillis();

			//
			session     = CassandraSessionFixer.getSession();
			String keyspace    = CassandraSessionFixer.getKeyspace();

			//
			sampling_table = StorageConstants.TAG_POINT_SAMPLING_TABLE + "_" + term.replace(" ", "_").toLowerCase();

			cql = new StringBuffer();
			cql.append(" INSERT INTO  " + keyspace + "." + sampling_table + " ( ");
			cql.append(" " + CassandraColumns.TAG_ID + ", ");
			cql.append(" " + CassandraColumns.TIMESTAMP + ", ");
			cql.append(" " + CassandraColumns.VALUE + " ");
			cql.append(" ) ");
			cql.append(" VALUES ");
			cql.append(" ( ");
			cql.append(" ");
			cql.append(" ?,");
			cql.append(" ?,");
			cql.append(" ? ");
			cql.append(" ) USING TIMESTAMP ? ; ");

			PreparedStatement ps = PreparedStatementCacheFactory.getInstance().get(session, cql.toString(), ConsistencyLevel.ONE);

			
			int batchSize = 1000;
			int count = 0;
			
			BatchStatement batch = new BatchStatement(Type.UNLOGGED);
			for(int i=0; i < point_list.size(); i++) {
				Point point = point_list.get(i);
				BoundStatement query = new BoundStatement(ps);
				query.setIdempotent(true);
				query.setString(CassandraColumns.TAG_ID, point.getTag_id());
				query.setTimestamp(CassandraColumns.TIMESTAMP, new Date(timestamp));
	            query.setString(CassandraColumns.VALUE, point.getValue());
	            query.setLong(3, timestamp);
	            batch.add(query);
	            if(++count % batchSize == 0) {
	        		session.executeAsync(batch);
	        		batch = new BatchStatement(Type.UNLOGGED);
	        	}
			};
            session.executeAsync(batch);
			
			long end = System.currentTimeMillis() - start;
			log.debug("Cassandra insert sampling data : table=[" + sampling_table + "], point_list=[" + point_list.size() + "] : exec_time=[" + end + "]");

		} catch (Exception ex) {
			DataFlowErrorCount.ERROR_STORE_SAMPLING_INSERT.incrementAndGet();
			log.error("[" + sampling_table + "] sampling insert error : CQL=[" + cql.toString() + "], " + ex.getMessage(), ex);
			throw ex;
		} finally {
			//
			// if (session != null && !session.isClosed())
			// session.close();
		}

	}
	
	
	@Override
	public void insertBLOB(BLOB blob) throws Exception {
		//

		Session session = null;
		StringBuffer cql = new StringBuffer();
	
		//
		try {

			//
			long start = System.currentTimeMillis();

			//
			session     = CassandraSessionFixer.getSession();
			String keyspace    = CassandraSessionFixer.getKeyspace();

			//
			UUID uuid = UUIDs.random();
			
			//
			cql = new StringBuffer();
			cql.append(" INSERT INTO  " + keyspace + "." + StorageConstants.TAG_BLOB_TABLE + " ( ");
			cql.append(" " + CassandraColumns.TAG_ID + ", ");
			cql.append(" " + CassandraColumns.TIMESTAMP + ", ");
			cql.append(" " + CassandraColumns.OBJECT_ID + ", ");
			cql.append(" " + CassandraColumns.FILE_PATH + ", ");
			cql.append(" " + CassandraColumns.FILE_SIZE + ", ");
			cql.append(" " + CassandraColumns.MIME_TYPE + ", ");
			cql.append(" " + CassandraColumns.ATTRIBUTE + " ");
			cql.append(" ) ");
			cql.append(" VALUES ");
			cql.append(" ( ");
			cql.append(" ");
			cql.append(" ?,");
			cql.append(" ?,");
			cql.append(" ?,");
			cql.append(" ?,");
			cql.append(" ?,");
			cql.append(" ?,");
			cql.append(" ? ");
			cql.append(" ) USING TIMESTAMP ? ; ");

			PreparedStatement ps = PreparedStatementCacheFactory.getInstance().get(session, cql.toString(), ConsistencyLevel.ONE);

			//기본 정보 저장
			BoundStatement query = new BoundStatement(ps);
			query.setString(CassandraColumns.TAG_ID, blob.getTag_id());
			query.setTimestamp(CassandraColumns.TIMESTAMP, new Date(blob.getTimestamp()));
			query.setUUID(CassandraColumns.OBJECT_ID, uuid);
			query.setString(CassandraColumns.FILE_PATH, blob.getFile_path());
			query.setLong(CassandraColumns.FILE_SIZE, blob.getFile_size());
			query.setString(CassandraColumns.MIME_TYPE, blob.getMime_type());
			query.setMap(CassandraColumns.ATTRIBUTE, blob.getAttribute());
			query.setLong(7, blob.getTimestamp());
			
			session.executeAsync(query);
			
			//BLOB 오브젝트 저장
			cql = new StringBuffer();
			cql.append(" INSERT INTO  " + keyspace + "." + StorageConstants.TAG_BLOB_OBJECT_TABLE + " ( ");
			cql.append(" " + CassandraColumns.OBJECT_ID + ", ");
			cql.append(" " + CassandraColumns.OBJECT + " ");
			cql.append(" ) ");
			cql.append(" VALUES ");
			cql.append(" ( ");
			cql.append(" ");
			cql.append(" ?,");
			cql.append(" ? ");
			cql.append(" ); ");

			PreparedStatement ps_object = PreparedStatementCacheFactory.getInstance().get(session, cql.toString(), ConsistencyLevel.ONE);

			BoundStatement query_object = new BoundStatement(ps_object);
			query_object.setUUID(CassandraColumns.OBJECT_ID,   uuid);
			query_object.setBytes(CassandraColumns.OBJECT,     ByteBuffer.wrap(blob.getBlob()));;
			
			session.executeAsync(query_object);
			
			

			long end = System.currentTimeMillis() - start;
			log.debug("Cassandra insert BLOB data : table=[" + StorageConstants.TAG_BLOB_TABLE + "] : exec_time=[" + end + "]");

		} catch (Exception ex) {
			DataFlowErrorCount.ERROR_STORE_BLOB_INSERT.incrementAndGet();
			log.error("[" + StorageConstants.TAG_BLOB_TABLE + "] BLOB insert error : CQL=[" + cql.toString() + "], " + ex.getMessage(), ex);
			throw ex;
		} finally {
			//
			// if (session != null && !session.isClosed())
			// session.close();
		}

	}

	@Override
	public void insertPointAggregation(Tag tag, String term, long timestamp, JSONObject data) throws Exception {
		//

		Session session = null;
		StringBuffer cql = new StringBuffer();
		String agg_table = "";
		//
		try {

			//
			long start = System.currentTimeMillis();

			//
			session     = CassandraSessionFixer.getSession();
			String keyspace    = CassandraSessionFixer.getKeyspace();

			//
			agg_table = StorageConstants.TAG_POINT_AGGREGATION_TABLE + "_" + term.replace(" ", "_").toLowerCase();

			cql = new StringBuffer();
			cql.append(" INSERT INTO  " + keyspace + "." + agg_table + " ( ");
			cql.append(" " + CassandraColumns.TAG_ID + ", ");
			cql.append(" " + CassandraColumns.TIMESTAMP + ", ");
			cql.append(" " + CassandraColumns.COUNT + ", ");
			cql.append(" " + CassandraColumns.FIRST + ", ");
			cql.append(" " + CassandraColumns.LAST + ", ");
			cql.append(" " + CassandraColumns.AVG + ", ");
			cql.append(" " + CassandraColumns.MIN + ", ");
			cql.append(" " + CassandraColumns.MAX + ", ");
			cql.append(" " + CassandraColumns.SUM + ", ");
			cql.append(" " + CassandraColumns.STDDEV + ", ");
			cql.append(" " + CassandraColumns.MEDIAN + "  ");
			cql.append(" ) ");
			cql.append(" VALUES ");
			cql.append(" ( ");
			cql.append(" ");
			cql.append(" ?,");
			cql.append(" ?,");
			cql.append(" ?,");
			cql.append(" ?,");
			cql.append(" ?,");
			cql.append(" ?,");
			cql.append(" ?,");
			cql.append(" ?,");
			cql.append(" ?,");
			cql.append(" ?,");
			cql.append(" ? ");
			cql.append(" ) USING TIMESTAMP ? ; ");

			PreparedStatement ps = PreparedStatementCacheFactory.getInstance().get(session, cql.toString(), ConsistencyLevel.ONE);

			BoundStatement query = new BoundStatement(ps);
			query.setIdempotent(true);
			
			query.setString(CassandraColumns.TAG_ID, tag.getTag_id());
			query.setTimestamp(CassandraColumns.TIMESTAMP, new Date(timestamp));
			query.setInt(CassandraColumns.COUNT, data.getInt("count"));
			query.setString(CassandraColumns.FIRST, data.getString("first"));
            query.setString(CassandraColumns.LAST, data.getString("last"));
			
			
			//移댁슫�듃媛� 0 �씠�긽�씪 寃쎌슦 吏묎퀎媛� 議댁옱�븿.
			if (data.getInt("count") > 0) {
				
				if (!StorageUtils.isNull(data, "avg")) {
					query.setDouble(CassandraColumns.AVG, data.getDouble("avg"));
				} else {
					query.setToNull(CassandraColumns.AVG);
				}
				;
				if (!StorageUtils.isNull(data, "min")) {
					query.setDouble(CassandraColumns.MIN, data.getDouble("min"));
				} else {
					query.setToNull(CassandraColumns.MIN);
				}
				;
				if (!StorageUtils.isNull(data, "max")) {
					query.setDouble(CassandraColumns.MAX, data.getDouble("max"));
				} else {
					query.setToNull(CassandraColumns.MAX);
				}
				if (!StorageUtils.isNull(data, "sum")) {
					query.setDouble(CassandraColumns.SUM, data.getDouble("sum"));
				} else {
					query.setToNull(CassandraColumns.SUM);
				}
				;
				if (!StorageUtils.isNull(data, "stddev")) {
					query.setDouble(CassandraColumns.STDDEV, data.getDouble("stddev"));
				} else {
					query.setToNull(CassandraColumns.STDDEV);
				}
				;
				if (!StorageUtils.isNull(data, "median")) {
					query.setDouble(CassandraColumns.MEDIAN, data.getDouble("median"));
				} else {
					query.setToNull(CassandraColumns.MEDIAN);
				}
				;

			};
			
			query.setLong(11, timestamp);

			session.executeAsync(query);

			long end = System.currentTimeMillis() - start;
			log.debug("Cassandra insert aggregation data : table=[" + agg_table + "], size=[" + data.toString() + "] : exec_time=[" + end + "]");

		} catch (Exception ex) {
			DataFlowErrorCount.ERROR_STORE_AGGREGATION_INSERT.incrementAndGet();
			log.error("[" + agg_table + "] aggregation insert error : CQL=[" + cql.toString() + "], " + ex.getMessage(), ex);
			throw ex;
		} finally {
			//
			// if (session != null && !session.isClosed())
			// session.close();
		}

	}

	@Override
	public void insertPointSnapshot(String site_id, String term, long timestamp, JSONObject data_map, JSONObject timestamp_map, JSONObject alarm_map) throws Exception {
		//

		Session session = null;
		StringBuffer cql = new StringBuffer();
		String snapshot_table = "";
		//
		try {

			//
			long start = System.currentTimeMillis();

			//
			session     = CassandraSessionFixer.getSession();
			String keyspace    = CassandraSessionFixer.getKeyspace();

			//
			// snapshot_table = StorageConstants.TAG_POINT_SNAPSHOT_TABLE + "_"
			// + term.replace(" ", "_");
			snapshot_table = StorageConstants.TAG_POINT_SNAPSHOT_TABLE;

			cql = new StringBuffer();
			cql.append(" INSERT INTO  " + keyspace + "." + snapshot_table + " ( ");
			cql.append(" " + CassandraColumns.SITE_ID + ", ");
			cql.append(" " + CassandraColumns.SNAPSHOT_ID + ", ");
			cql.append(" " + CassandraColumns.DATE + ", ");
			cql.append(" " + CassandraColumns.HOUR + ", ");
			cql.append(" " + CassandraColumns.TIMESTAMP + ", ");
			cql.append(" " + CassandraColumns.DATA_MAP + ",  ");
			cql.append(" " + CassandraColumns.TIMESTAMP_MAP + ",  ");
			cql.append(" " + CassandraColumns.ALARM_MAP + " ");
			cql.append(" ) ");
			cql.append(" VALUES ");
			cql.append(" ( ");
			cql.append(" ");
			cql.append(" :" + CassandraColumns.SITE_ID + ", ");
			cql.append(" :" + CassandraColumns.SNAPSHOT_ID + ", ");
			cql.append(" :" + CassandraColumns.DATE + ", ");
			cql.append(" :" + CassandraColumns.HOUR + ", ");
			cql.append(" :" + CassandraColumns.TIMESTAMP + ", ");
			cql.append(" :" + CassandraColumns.DATA_MAP + ",  ");
			cql.append(" :" + CassandraColumns.TIMESTAMP_MAP + ",  ");
			cql.append(" :" + CassandraColumns.ALARM_MAP + " ");
			cql.append(" ) USING TIMESTAMP ? ; ");

			PreparedStatement ps = PreparedStatementCacheFactory.getInstance().get(session, cql.toString(), ConsistencyLevel.ONE);
			
			SimpleDateFormat formatted = new SimpleDateFormat("yyyyMMdd");
			int date = Integer.parseInt(formatted.format(new Date(timestamp)));
			Map<String, Long> p_timestamp_map = new HashMap<String,Long>();
			Iterator<?> keys = timestamp_map.keys();
			while( keys.hasNext() ) {
				String key = (String)keys.next();
	            p_timestamp_map.put(key, timestamp_map.getLong(key));
	        };
			
			BoundStatement query = new BoundStatement(ps);
			query.setIdempotent(true);
			
			// 
			query.setString(CassandraColumns.SITE_ID, site_id);
			query.setString(CassandraColumns.SNAPSHOT_ID, "SNAPSHOT_" + term.replace(" ", "_"));
			query.setInt(CassandraColumns.DATE,   TimestampUtils.timestampToYYYYMMDD(timestamp));
			query.setInt(CassandraColumns.HOUR,   TimestampUtils.getHH(timestamp));
			query.setTimestamp(CassandraColumns.TIMESTAMP, new Date(timestamp));
			query.setMap(CassandraColumns.DATA_MAP, (Map<String, String>) data_map);
			query.setMap(CassandraColumns.TIMESTAMP_MAP, (Map<String, Long>)  p_timestamp_map);
			query.setMap(CassandraColumns.ALARM_MAP, (Map<String, Integer>) alarm_map);
			query.setLong(8, timestamp);
			
			session.executeAsync(query);

			long end = System.currentTimeMillis() - start;
			log.debug("Cassandra insert snapshot data : table=[" + snapshot_table + "], size=[" + data_map.size() + "] : exec_time=[" + end + "]");

		} catch (Exception ex) {
			DataFlowErrorCount.ERROR_STORE_SNAPSHOT_INSERT.incrementAndGet();
			log.error("[" + snapshot_table + "] snapshot insert error : CQL=[" + cql.toString() + "], " + ex.getMessage(), ex);
			throw ex;
		} finally {
			//
			// if (session != null && !session.isClosed())
			// session.close();
		}
	}


	@Override
	public void insertPointArchive(Tag tag, int date, int hour, int point_count, Map<String, List<String>> map) throws Exception {
		//

		Session session = null;
		StringBuffer cql = new StringBuffer();
		//
		try {

			//
			long start = System.currentTimeMillis();

			//
			session     = CassandraSessionFixer.getSession();
			String keyspace    = CassandraSessionFixer.getKeyspace();

			long timestamp = (System.currentTimeMillis() / 1000) * 1000;
			///
			///
			///
			cql = new StringBuffer();
			cql.append(" INSERT INTO  " + keyspace + "." + StorageConstants.TAG_POINT_ARCHIVE_TABLE + " ( ");
			cql.append(" " + CassandraColumns.TAG_ID + ", ");
			cql.append(" " + CassandraColumns.DATE + ", ");
			cql.append(" " + CassandraColumns.HOUR + ", ");
			for(int i=0;i<60;i++) {
				cql.append(" " + "m_" + (String.format("%02d", i))  + ", ");
			};
			cql.append(" " + CassandraColumns.ATTRIBUTE + " ");
			cql.append(" ) ");
			cql.append(" VALUES ");
			cql.append(" ( ");
			cql.append(" ");
			cql.append(" :" + CassandraColumns.TAG_ID + ", ");
			cql.append(" :" + CassandraColumns.DATE + ", ");
			cql.append(" :" + CassandraColumns.HOUR + ", ");
			for(int i=0;i<60;i++) {
				cql.append(" " + ":m_" + (String.format("%02d", i))  + ", ");
			};
			cql.append(" :" + CassandraColumns.ATTRIBUTE + " ");
			cql.append(" ) USING TIMESTAMP ? ; ");

			PreparedStatement ps = PreparedStatementCacheFactory.getInstance().get(session, cql.toString(), ConsistencyLevel.ONE);

			BoundStatement query = new BoundStatement(ps);
			query.setIdempotent(true);
			query.setString(CassandraColumns.TAG_ID, tag.getTag_id());
			query.setInt(CassandraColumns.DATE,      date);
			query.setInt(CassandraColumns.HOUR,      hour);
			for(int i=0;i<60;i++) {
				query.setList("m_" + (String.format("%02d", i)), map.get( "M_" + (String.format("%02d", i))));
			};
			query.setMap(CassandraColumns.ATTRIBUTE, new HashMap<String,String>());
			query.setLong(65, timestamp);
			session.executeAsync(query);
			
			///
			
			
			cql = new StringBuffer();
			cql.append(" UPDATE " +  keyspace + "." + StorageConstants.TAG_POINT_ARCHIVE_TABLE  + "_count SET count = count + :count "
					+ " WHERE tag_id = :tag_id AND date = :date AND hour = :hour ");

			PreparedStatement ps_3 = PreparedStatementCacheFactory.getInstance().get(session, cql.toString(), ConsistencyLevel.ONE);

			BoundStatement query_3 = new BoundStatement(ps_3);
			query_3.setString(CassandraColumns.TAG_ID, tag.getTag_id());
			query_3.setInt(CassandraColumns.DATE,      date);
			query_3.setInt(CassandraColumns.HOUR,      hour);
			query_3.setLong(CassandraColumns.COUNT,    point_count);
			session.executeAsync(query_3);

			//

			//
			//
			/*
			List<String> hour_all_list = new ArrayList<String>();
			for(int i=0;i<60;i++) {
				hour_all_list.addAll(map.get( "M_" + (String.format("%02d", i))));
			};
			cql = new StringBuffer();
			cql.append(" UPDATE " +  keyspace + "." + StorageConstants.TAG_POINT_ARCHIVE_TABLE + "_DAILY SET " + "h_" +  (String.format("%02d", hour)) + " = :h_" +  (String.format("%02d", hour))  + "  "
					+ " WHERE tag_id = :tag_id AND date = :date ");

			PreparedStatement ps_2 = PreparedStatementCacheFactory.getInstance().get(session, cql.toString(), ConsistencyLevel.ONE);

			BoundStatement query_2 = new BoundStatement(ps_2);
			query_2.setString(CassandraColumns.TAG_ID, tag.getTag_id());
			query_2.setInt(CassandraColumns.DATE,      date);
			query_2.setList("h_" + (String.format("%02d", hour)) , hour_all_list);
			session.executeAsync(query_2);
			*/
			//
			//
			//
			
			
			

			long end = System.currentTimeMillis() - start;
			log.debug("Cassandra insert archive data : table=[" + StorageConstants.TAG_POINT_ARCHIVE_TABLE  + "_*] : exec_time=[" + end + "]");

		} catch (Exception ex) {
			DataFlowErrorCount.ERROR_STORE_SNAPSHOT_INSERT.incrementAndGet();
			log.error("[" + StorageConstants.TAG_POINT_ARCHIVE_TABLE  + "_*] archive insert error : CQL=[" + cql.toString() + "], " + ex.getMessage(), ex);
			throw ex;
		} finally {
			//
			// if (session != null && !session.isClosed())
			// session.close();
		}
	}

	
	@Override
	public void insertPointMap(long timestamp, Map<String, Point> map) throws Exception {
		//

		Session session = null;
		StringBuffer cql = new StringBuffer();
		
		//
		try {

			//
			long start = System.currentTimeMillis();

			//
			session     = CassandraSessionFixer.getSession();
			String keyspace    = CassandraSessionFixer.getKeyspace();
			
			
			//2.CQL �깮�꽦
			cql = new StringBuffer();
			cql.append(" INSERT INTO  " + keyspace + "." + StorageConstants.TAG_POINT_MAP_TABLE + " ( ");
			cql.append(" " + CassandraColumns.YEAR + ",  ");
			cql.append(" " + CassandraColumns.MONTH + ",  ");
			cql.append(" " + CassandraColumns.DAY + ",  ");
			cql.append(" " + CassandraColumns.HOUR + ",  ");
			cql.append(" " + CassandraColumns.MINUTE + ",  ");
			cql.append(" " + CassandraColumns.TIMESTAMP + ",  ");
			cql.append(" " + CassandraColumns.MAP + "  ");
			cql.append(" )  ");
			cql.append(" VALUES ");
			cql.append(" ( ");
			cql.append(" :" + CassandraColumns.YEAR + ",  ");
			cql.append(" :" + CassandraColumns.MONTH + ",  ");
			cql.append(" :" + CassandraColumns.DAY + ",  ");
			cql.append(" :" + CassandraColumns.HOUR + ",  ");
			cql.append(" :" + CassandraColumns.MINUTE + ",  ");
			cql.append(" :" + CassandraColumns.TIMESTAMP + ",  ");
			cql.append(" :" + CassandraColumns.MAP + "  ");
			cql.append(" ) USING TIMESTAMP ? ; ");

		
			PreparedStatement ps = PreparedStatementCacheFactory.getInstance().get(session, cql.toString(), ConsistencyLevel.ONE);
			BoundStatement query = new BoundStatement(ps);
			query.setIdempotent(true);
			
			Date date = new Date(timestamp);
			LocalDateTime local_datetime = date.toInstant().atZone(ZoneId.systemDefault()).toLocalDateTime();
			int year  = local_datetime.getYear();
			int month = local_datetime.getMonthValue();
			int day   = local_datetime.getDayOfMonth();
			int hour   = local_datetime.getHour();
			int minute = local_datetime.getMinute();
			
			query.setInt(CassandraColumns.YEAR,     year);
			query.setInt(CassandraColumns.MONTH,    month);
			query.setInt(CassandraColumns.DAY,      day);
			query.setInt(CassandraColumns.HOUR,     hour);
			query.setInt(CassandraColumns.MINUTE,   minute);
			query.setTimestamp(CassandraColumns.TIMESTAMP, new Date(timestamp));
			query.setString(CassandraColumns.MAP,   JSONObject.fromObject(map).toString());
			//IF BLOB
			//query.setBytes(CassandraColumns.MAP, ByteBuffer.wrap(JSONObject.fromObject(map).toString().getBytes()));
			
			query.setLong(7, timestamp);
			
			session.executeAsync(query);

			long end = System.currentTimeMillis() - start;
			log.debug("Cassandra insert tag point map data : exec_time=[" + end + "]");

		} catch (Exception ex) {
			DataFlowErrorCount.ERROR_STORE_POINT_MAP_INSERT.incrementAndGet();
			log.error("[" + StorageConstants.TAG_POINT_MAP_TABLE + "] insert point map error : CQL=[" + cql.toString() + "], " + ex.getMessage(), ex);
			throw ex;
		} finally {
			//
			// if (session != null && !session.isClosed())
			// session.close();
		}
	}
	
	
	@Override
	public void insertPointMatrix(int date, long timestamp, long from, long to, Map<String, List<String>> map) throws Exception {
		//

		Session session = null;
		StringBuffer cql = new StringBuffer();
		
		//
		try {

			//
			long start = System.currentTimeMillis();

			//
			session     = CassandraSessionFixer.getSession();
			String keyspace    = CassandraSessionFixer.getKeyspace();
			
			
			//2.CQL
			cql = new StringBuffer();
			cql.append(" INSERT INTO  " + keyspace + "." + StorageConstants.TAG_POINT_MATRIX_TABLE + " ( ");
			cql.append(" " + CassandraColumns.DATE + ",  ");
			cql.append(" " + CassandraColumns.TIMESTAMP + ",  ");
			cql.append(" " + CassandraColumns.FROM + ",  ");
			cql.append(" " + CassandraColumns.TO + ",  ");
			cql.append(" " + CassandraColumns.POINT_MAP + "  ");
			cql.append(" )  ");
			cql.append(" VALUES ");
			cql.append(" ( ");
			cql.append(" :" + CassandraColumns.DATE + ",  ");
			cql.append(" :" + CassandraColumns.TIMESTAMP + ",  ");
			cql.append(" :" + CassandraColumns.FROM + ",  ");
			cql.append(" :" + CassandraColumns.TO + ",  ");
			cql.append(" :" + CassandraColumns.POINT_MAP + "  ");
			cql.append(" ) USING TIMESTAMP ? ; ");

			PreparedStatement ps = PreparedStatementCacheFactory.getInstance().get(session, cql.toString(), ConsistencyLevel.ONE);
			BoundStatement query = new BoundStatement(ps);
			query.setIdempotent(true);
			
			query.setInt(CassandraColumns.DATE, date);
			query.setTimestamp(CassandraColumns.TIMESTAMP, new Timestamp(timestamp));
			query.setTimestamp(CassandraColumns.FROM, new Timestamp(from));
			query.setTimestamp(CassandraColumns.TO,   new Timestamp(to));
			query.setMap(CassandraColumns.POINT_MAP, map, TypeToken.of(String.class), TypeTokens.listOf(String.class));
			query.setLong(5, timestamp);
			session.executeAsync(query);

			long end = System.currentTimeMillis() - start;
			log.debug("Cassandra insert tag point matrix data : exec_time=[" + end + "]");

		} catch (Exception ex) {
			DataFlowErrorCount.ERROR_STORE_POINT_MAP_INSERT.incrementAndGet();
			log.error("[" + StorageConstants.TAG_POINT_MATRIX_TABLE + "] insert point matrix error : CQL=[" + cql.toString() + "], " + ex.getMessage(), ex);
			throw ex;
		} finally {
			//
			// if (session != null && !session.isClosed())
			// session.close();
		}
	};

	
	

	@Override
	public void insertAlarm(plantpulse.event.opc.Alarm alarm, JSONObject data) throws Exception {
		//

		Session session = null;
		StringBuffer cql = new StringBuffer();
		//
		try {

			//
			long start = System.currentTimeMillis();

			//
			session     = CassandraSessionFixer.getSession();
			String keyspace    = CassandraSessionFixer.getKeyspace();

	
			// 태그 알람 저장
			cql = new StringBuffer();
			cql.append(" INSERT INTO  " + keyspace + "." + StorageConstants.TAG_ARLAM_TABLE + " ( ");
			cql.append(" " + CassandraColumns.TAG_ID + ",        ");
			cql.append(" " + CassandraColumns.ASSET_ID + ",        ");
			cql.append(" " + CassandraColumns.TIMESTAMP + "     , ");
			cql.append(" " + CassandraColumns.PRIORITY + "      ,   ");
			cql.append(" " + CassandraColumns.DESCRIPTION + "  ,     ");
			cql.append(" " + CassandraColumns.ALARM_SEQ + ",   ");
			cql.append(" " + CassandraColumns.ALARM_CONFIG_ID + ",  ");
			
			//22 추가
			cql.append(" " + "site_id" + ",   ");
			cql.append(" " + "site_name" + ",   ");
			cql.append(" " + "site_description" + ",   ");
			cql.append(" " + "opc_id" + ",   ");
			cql.append(" " + "opc_name" + ",   ");
			cql.append(" " + "opc_description" + ",   ");
			cql.append(" " + "tag_name" + ",   ");
			cql.append(" " + "tag_description" + ",   ");
			cql.append(" " + "asset_name" + ",   ");
			cql.append(" " + "asset_description" + ",   ");
			
			cql.append(" " + "tag_java_type" + ",   ");
			cql.append(" " + "tag_interval" + ",   "); //int
			cql.append(" " + "tag_alias_name" + ",   ");
			cql.append(" " + "tag_unit" + ",   ");
			cql.append(" " + "tag_display_format" + ",   ");
			cql.append(" " + "tag_importance" + ",   ");
			cql.append(" " + "tag_lat" + ",   ");
			cql.append(" " + "tag_lng" + ",   ");
			cql.append(" " + "tag_min_value" + ",   ");
			cql.append(" " + "tag_max_value" + ",   ");
			cql.append(" " + "tag_note" + ",   ");
			cql.append(" " + "tag_location" + ",   ");
			
			cql.append(" " + "alarm_config_name" + ",   ");;
			cql.append(" " + "alarm_config_description" + ",   ");
			cql.append(" " + "alarm_config_epl" + ",   ");
			cql.append(" " + "alarm_config_recieve_me" + ",   ");
			cql.append(" " + "alarm_config_recieve_others" + ",   ");
			cql.append(" " + "alarm_config_insert_user_id" + "   ");
			
			//
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
			cql.append(" ?, ");
			cql.append(" ?, ");
			cql.append(" ?, ");
			cql.append(" ?, ");
			cql.append(" ?, ");
			cql.append(" ?, ");
			cql.append(" ?, ");
			cql.append(" ?, ");
			cql.append(" ?, ");
			cql.append(" ?, ");
			cql.append(" ?, ");
			cql.append(" ?, ");
			cql.append(" ?, ");
			cql.append(" ?, ");
			cql.append(" ?, ");
			cql.append(" ?, ");
			cql.append(" ?, ");
			cql.append(" ?, ");
			cql.append(" ?, ");
			cql.append(" ?, ");
			cql.append(" ?, ");
			cql.append(" ?, ");
			cql.append(" ?, ");
			cql.append(" ?, ");
			cql.append(" ?, ");
			cql.append(" ?, ");
			cql.append(" ?  ");
			
			cql.append(" ) USING TIMESTAMP ? ; ");
			
			
			PreparedStatement ps = PreparedStatementCacheFactory.getInstance().get(session, cql.toString(), ConsistencyLevel.ONE);
			BoundStatement query = new BoundStatement(ps);
			query.setString(CassandraColumns.TAG_ID, data.getString("tag_id"));
			if(data.containsKey("asset_id") && StringUtils.isNotEmpty(data.getString("asset_id"))){
				query.setString(CassandraColumns.ASSET_ID, data.getString("asset_id"));
			}else{
				query.setToNull(CassandraColumns.ASSET_ID);
			}
			query.setTimestamp(CassandraColumns.TIMESTAMP, new Date(Long.parseLong(data.getString("timestamp"))));
			query.setString(CassandraColumns.PRIORITY, data.getString("priority"));
			query.setString(CassandraColumns.DESCRIPTION, data.getString("description"));
			query.setInt(CassandraColumns.ALARM_SEQ, data.getInt("alarm_seq"));
			query.setString(CassandraColumns.ALARM_CONFIG_ID, data.getString("alarm_config_id"));
			//
			
			//26 컬럼 추가
			query.setString("SITE_ID",            data.getString("site_id"));
			query.setString("SITE_NAME",          data.getString("site_name"));
			query.setString("SITE_DESCRIPTION",   data.getString("site_description"));
			query.setString("OPC_ID",             data.getString("opc_id"));
			query.setString("OPC_NAME",           data.getString("opc_name"));
			query.setString("OPC_DESCRIPTION",    data.getString("opc_description"));
			query.setString("TAG_NAME",           data.getString("tag_name"));
			query.setString("TAG_DESCRIPTION",    data.getString("tag_description"));
			query.setString("ASSET_NAME",         data.getString("asset_name"));
			query.setString("ASSET_DESCRIPTION",  data.getString("asset_description"));
			
			query.setString("TAG_JAVA_TYPE",      data.getString("tag_java_type"));
			query.setInt("TAG_INTERVAL"     ,     data.getInt("tag_interval"));
			query.setString("TAG_ALIAS_NAME",     data.getString("tag_alias_name"));
			query.setString("TAG_UNIT",           data.getString("tag_unit"));
			query.setString("TAG_DISPLAY_FORMAT", data.getString("tag_display_format"));
			query.setInt("TAG_IMPORTANCE",        data.getInt("tag_importance"));
			query.setString("TAG_LAT",            data.getString("tag_lat"));
			query.setString("TAG_LNG",            data.getString("tag_lng"));
			query.setString("TAG_MIN_VALUE",      data.getString("tag_min_value"));
			query.setString("TAG_MAX_VALUE",      data.getString("tag_max_value"));
			query.setString("TAG_NOTE",           data.getString("tag_note"));
			query.setString("TAG_LOCATION",       data.getString("tag_location"));
			
			query.setString("ALARM_CONFIG_NAME",           data.getString("alarm_config_name"));
			query.setString("ALARM_CONFIG_DESCRIPTION",    data.getString("alarm_config_description"));
			query.setString("ALARM_CONFIG_EPL",            data.getString("alarm_config_epl"));
			query.setBool("ALARM_CONFIG_RECIEVE_ME",       data.getBoolean("alarm_config_recieve_me"));
			query.setString("ALARM_CONFIG_RECIEVE_OTHERS", data.getString("alarm_config_recieve_others"));
			query.setString("ALARM_CONFIG_INSERT_USER_ID", data.getString("alarm_config_insert_user_id"));
			
			query.setLong(35, Long.parseLong(data.getString("timestamp")));
			//
			session.executeAsync(query);
			
			
			// 태그 알람 온 테이블 등록
			cql = new StringBuffer();
			cql.append(" INSERT INTO  " + keyspace + "." + StorageConstants.TAG_ARLAM_ON_TABLE + " ( ");
			cql.append(" " + CassandraColumns.TAG_ID + ",        ");
			cql.append(" " + CassandraColumns.ASSET_ID + ",        ");
			cql.append(" " + CassandraColumns.TIMESTAMP + "     , ");
			cql.append(" " + CassandraColumns.PRIORITY + "      ,   ");
			cql.append(" " + CassandraColumns.DESCRIPTION + "  ,     ");
			cql.append(" " + CassandraColumns.ALARM_SEQ + ",   ");
			cql.append(" " + CassandraColumns.ALARM_CONFIG_ID + "   ");
			cql.append(" )  ");
			cql.append(" VALUES ");
			cql.append(" ( ");
			cql.append(" ?, ");
			cql.append(" ?, ");
			cql.append(" ?, ");
			cql.append(" ?, ");
			cql.append(" ?, ");
			cql.append(" ?,  ");
			cql.append(" ?  ");
			cql.append(" )  USING TTL ? AND TIMESTAMP ? ; ");

			
			PreparedStatement ps_active = PreparedStatementCacheFactory.getInstance().get(session, cql.toString(), ConsistencyLevel.ONE);
			BoundStatement query_active = new BoundStatement(ps_active);
			query_active.setString(CassandraColumns.TAG_ID, data.getString("tag_id"));
			if(data.containsKey("asset_id") && StringUtils.isNotEmpty(data.getString("asset_id"))){
				query_active.setString(CassandraColumns.ASSET_ID, data.getString("asset_id"));
			}else{
				query_active.setToNull(CassandraColumns.ASSET_ID);
			}
			query_active.setTimestamp(CassandraColumns.TIMESTAMP, new Date(Long.parseLong(data.getString("timestamp"))));
			query_active.setString(CassandraColumns.PRIORITY, data.getString("priority"));
			query_active.setString(CassandraColumns.DESCRIPTION, data.getString("description"));
			query_active.setInt(CassandraColumns.ALARM_SEQ, data.getInt("alarm_seq"));
			query_active.setString(CassandraColumns.ALARM_CONFIG_ID, data.getString("alarm_config_id"));
			
			//
			int alarm_on_ttl =
			Integer.parseInt(
					plantpulse.cep.engine.properties.PropertiesLoader.getEngine_properties().getProperty("alarm.duplicate.check.minutes", "600"));
			query_active.setInt(7, alarm_on_ttl); //
			query_active.setLong(8, Long.parseLong(data.getString("timestamp")));
			//
			session.executeAsync(query_active);
			
			
			// 에셋 알람 등록
			if(data.containsKey("asset_id") && StringUtils.isNotEmpty(data.getString("asset_id"))){
				
				
				//
				cql = new StringBuffer();
				cql.append(" INSERT INTO  " + keyspace + "." + StorageConstants.ASSET_ALARM_TABLE + " ( ");
				cql.append(" " + CassandraColumns.ASSET_ID + ",           ");
				cql.append(" " + CassandraColumns.ALIAS_NAME + ",           ");
				cql.append(" " + CassandraColumns.TAG_ID + ",           ");
				cql.append(" " + CassandraColumns.TIMESTAMP + "     ,   ");
				cql.append(" " + CassandraColumns.PRIORITY + "      ,   ");
				cql.append(" " + CassandraColumns.DESCRIPTION + "  ,     ");
				cql.append(" " + CassandraColumns.ALARM_SEQ + ",   ");
				cql.append(" " + CassandraColumns.ALARM_CONFIG_ID + ",   ");
				
				cql.append(" site_id,   ");
				cql.append(" site_name,   ");
				cql.append(" site_description,   ");
				cql.append(" tag_name,   ");
				cql.append(" tag_description,   ");
				cql.append(" tag_location,   ");
				cql.append(" asset_name,   ");
				cql.append(" asset_description,   ");
				cql.append(" area_id,   ");
				cql.append(" area_name,   ");
				cql.append(" area_description   ");
				
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
				cql.append(" ?, ");
				cql.append(" ?, ");
				cql.append(" ?, ");
				cql.append(" ?, ");
				cql.append(" ?, ");
				cql.append(" ?, ");
				cql.append(" ?, ");
				cql.append(" ?, ");
				cql.append(" ?, ");
				cql.append(" ?, ");
				cql.append(" ?  ");
				cql.append(" ) USING TIMESTAMP ? ; ");
	
				//
				PreparedStatement aps = PreparedStatementCacheFactory.getInstance().get(session, cql.toString(), ConsistencyLevel.ONE);
				BoundStatement aquery = new BoundStatement(aps);
				aquery.setString(CassandraColumns.ASSET_ID,   data.getString("asset_id"));
				aquery.setString(CassandraColumns.ALIAS_NAME, data.getString("alias_name"));
				aquery.setString(CassandraColumns.TAG_ID, data.getString("tag_id"));
				aquery.setTimestamp(CassandraColumns.TIMESTAMP, new Date(Long.parseLong(data.getString("timestamp"))));
				aquery.setString(CassandraColumns.PRIORITY, data.getString("priority"));
				aquery.setString(CassandraColumns.DESCRIPTION, data.getString("description"));
				aquery.setInt(CassandraColumns.ALARM_SEQ, data.getInt("alarm_seq"));
				aquery.setString(CassandraColumns.ALARM_CONFIG_ID, data.getString("alarm_config_id"));
				
				aquery.setString("SITE_ID",           data.getString("site_id"));
				aquery.setString("SITE_NAME",         data.getString("site_name"));
				aquery.setString("SITE_DESCRIPTION",  data.getString("site_description"));
				aquery.setString("TAG_NAME",          data.getString("tag_name"));
				aquery.setString("TAG_DESCRIPTION",   data.getString("tag_description"));
				aquery.setString("TAG_LOCATION",      data.getString("tag_location"));
				aquery.setString("ASSET_NAME",        data.getString("asset_name"));
				aquery.setString("ASSET_DESCRIPTION", data.getString("asset_description"));
				
				aquery.setString("AREA_ID",          data.getString("area_id"));
				aquery.setString("AREA_NAME",        data.getString("area_name"));
				aquery.setString("AREA_DESCRIPTION", data.getString("area_description"));
				
				aquery.setLong(19, Long.parseLong(data.getString("timestamp")));
				
				//
				session.executeAsync(aquery);
			}
			
			// 업데이트 태그 알람 건수 카운트
			StringBuffer cql_ac = new StringBuffer();
			cql_ac.append(" UPDATE   " + keyspace + "." + StorageConstants.TAG_ARLAM_COUNT_TABLE + " ");
			cql_ac.append("    SET   " + CassandraColumns.ALARM_COUNT + " = " + CassandraColumns.ALARM_COUNT + " + ? ");
			if (data.getString("priority").equals("INFO")) {
				cql_ac.append("    ,   " + CassandraColumns.ALARM_COUNT_BY_INFO + " = " + CassandraColumns.ALARM_COUNT_BY_INFO + " + ? ");
			} else if (data.getString("priority").equals("WARN")) {
				cql_ac.append("    ,   " + CassandraColumns.ALARM_COUNT_BY_WARN + " = " + CassandraColumns.ALARM_COUNT_BY_WARN + " + ? ");
			} else if (data.getString("priority").equals("ERROR")) {
				cql_ac.append("    ,   " + CassandraColumns.ALARM_COUNT_BY_ERROR + " = " + CassandraColumns.ALARM_COUNT_BY_ERROR + " + ? ");
			}
			cql_ac.append("  WHERE   " + CassandraColumns.TAG_ID + " = ? ");
			cql_ac.append("    AND   " + CassandraColumns.YEAR + " = ? ");
			cql_ac.append("    AND   " + CassandraColumns.MONTH + " = ? ");
			cql_ac.append("    AND   " + CassandraColumns.DAY + " = ? ");
			cql_ac.append("    AND   " + CassandraColumns.HOUR + " = ? ");
			cql_ac.append("    AND   " + CassandraColumns.MINUTE + " = ? ");

			PreparedStatement ps_alarm_count = PreparedStatementCacheFactory.getInstance().get(session, cql_ac.toString(), ConsistencyLevel.ONE);

			Date timestamp = new Date(Long.parseLong(data.getString("timestamp")));
			LocalDateTime local_datetime = timestamp.toInstant().atZone(ZoneId.systemDefault()).toLocalDateTime();
			
			//
			BoundStatement query_alarm_county = new BoundStatement(ps_alarm_count);
			query_alarm_county.setString(CassandraColumns.TAG_ID, data.getString("tag_id"));

			query_alarm_county.setLong(CassandraColumns.ALARM_COUNT, 1);

			if (data.getString("priority").equals("INFO")) {
				query_alarm_county.setLong(CassandraColumns.ALARM_COUNT_BY_INFO, 1);
			} else if (data.getString("priority").equals("WARN")) {
				query_alarm_county.setLong(CassandraColumns.ALARM_COUNT_BY_WARN, 1);
			} else if (data.getString("priority").equals("ERROR")) {
				query_alarm_county.setLong(CassandraColumns.ALARM_COUNT_BY_ERROR, 1);
			}

			query_alarm_county.setInt(CassandraColumns.YEAR,   local_datetime.getYear());
			query_alarm_county.setInt(CassandraColumns.MONTH,  local_datetime.getMonthValue());
			query_alarm_county.setInt(CassandraColumns.DAY,    local_datetime.getDayOfMonth());
			query_alarm_county.setInt(CassandraColumns.HOUR,   local_datetime.getHour());
			query_alarm_county.setInt(CassandraColumns.MINUTE, local_datetime.getMinute());

			session.executeAsync(query_alarm_county);


			// 사이트 요약 알람 건수 업데이트
			StringBuffer cql_site_summary = new StringBuffer();
			cql_site_summary.append(" UPDATE   " + keyspace + "." + StorageConstants.SITE_SUMMARY_TABLE + " ");
			cql_site_summary.append("    SET   " + CassandraColumns.ALARM_COUNT + " = " + CassandraColumns.ALARM_COUNT + " + ? ");
			if (data.getString("priority").equals("INFO")) {
				cql_site_summary.append("    ,   " + CassandraColumns.ALARM_COUNT_BY_INFO + " = " + CassandraColumns.ALARM_COUNT_BY_INFO + " + ? ");
			} else if (data.getString("priority").equals("WARN")) {
				cql_site_summary.append("    ,   " + CassandraColumns.ALARM_COUNT_BY_WARN + " = " + CassandraColumns.ALARM_COUNT_BY_WARN + " + ? ");
			} else if (data.getString("priority").equals("ERROR")) {
				cql_site_summary.append("    ,   " + CassandraColumns.ALARM_COUNT_BY_ERROR + " = " + CassandraColumns.ALARM_COUNT_BY_ERROR + " + ? ");
			}
			cql_site_summary.append("  WHERE   " + CassandraColumns.SITE_ID + " = ? ");
			cql_site_summary.append("    AND   " + CassandraColumns.YEAR + " = ? ");
			cql_site_summary.append("    AND   " + CassandraColumns.MONTH + " = ? ");
			cql_site_summary.append("    AND   " + CassandraColumns.DAY + " = ? ");
			cql_site_summary.append("    AND   " + CassandraColumns.HOUR + " = ? ");
			cql_site_summary.append("    AND   " + CassandraColumns.MINUTE + " = ? ");

			PreparedStatement ps_site_summary = PreparedStatementCacheFactory.getInstance().get(session, cql_site_summary.toString(), ConsistencyLevel.ONE);

			//
			BoundStatement query_site_summary = new BoundStatement(ps_site_summary);
			query_site_summary.setString(CassandraColumns.SITE_ID, data.getString("site_id"));

			query_site_summary.setLong(CassandraColumns.ALARM_COUNT, 1);

			if (data.getString("priority").equals("INFO")) {
				query_site_summary.setLong(CassandraColumns.ALARM_COUNT_BY_INFO, 1);
			} else if (data.getString("priority").equals("WARN")) {
				query_site_summary.setLong(CassandraColumns.ALARM_COUNT_BY_WARN, 1);
			} else if (data.getString("priority").equals("ERROR")) {
				query_site_summary.setLong(CassandraColumns.ALARM_COUNT_BY_ERROR, 1);
			}

			query_site_summary.setInt(CassandraColumns.YEAR,   local_datetime.getYear());
			query_site_summary.setInt(CassandraColumns.MONTH,  local_datetime.getMonthValue());
			query_site_summary.setInt(CassandraColumns.DAY,    local_datetime.getDayOfMonth());
			query_site_summary.setInt(CassandraColumns.HOUR,   local_datetime.getHour());
			query_site_summary.setInt(CassandraColumns.MINUTE, local_datetime.getMinute());
			

			session.executeAsync(query_site_summary);
			
			//타임시리즈 데이터베이스 동기화
			CEPEngineManager.getInstance().getTimeseries_database().syncAlarm(alarm, data);
			//

			long end = System.currentTimeMillis() - start;
			log.debug("Cassandra insert alarm data : exec_time=[" + end + "]");

		} catch (Exception ex) {
			DataFlowErrorCount.ERROR_STORE_ALARM_INSERT.incrementAndGet();
			log.error("[PP_TM_ALARM_TABLES*] insert alarm error : CQL=[" + cql.toString() + "], DATA=[" + data.toString() + "], ERROR=[" + ex.getMessage() + "]", ex);
			throw ex;
		} finally {
			//
			// if (session != null && !session.isClosed())
			// session.close();
		}
	}
	
	@Override
	public void insertAssetTimeline(AssetStatement stmt, long timestamp, Asset asset, String timeline_type, String payload) throws Exception {
		//
		Session session = null;
		StringBuffer cql = new StringBuffer();
		
		//
		try {

			//
			long start = System.currentTimeMillis();

			//
			session     = CassandraSessionFixer.getSession();
			String keyspace    = CassandraSessionFixer.getKeyspace();
			
			//2. 
			cql = new StringBuffer();
			cql.append(" INSERT INTO  " + keyspace + "." + StorageConstants.ASSET_TIMELINE_TABLE + " ( ");
			cql.append(" " + CassandraColumns.ASSET_ID  + ",  ");
			cql.append(" " + CassandraColumns.STATEMENT_NAME  + ",  ");
			cql.append(" " + CassandraColumns.STATEMENT_DESCRIPTION  + ",  ");
			cql.append(" " + CassandraColumns.TIMESTAMP + ",  ");
			cql.append(" " + CassandraColumns.TIMELINE_TYPE  + ",  ");
			cql.append(" " + CassandraColumns.PAYLOAD  + "  ");
			cql.append(" )  ");
			cql.append(" VALUES ");
			cql.append(" ( ");
			cql.append(" :" + CassandraColumns.ASSET_ID + ",  ");
			cql.append(" :" + CassandraColumns.STATEMENT_NAME  + ",  ");
			cql.append(" :" + CassandraColumns.STATEMENT_DESCRIPTION  + ",  ");
			cql.append(" :" + CassandraColumns.TIMESTAMP  + ",  ");
			cql.append(" :" + CassandraColumns.TIMELINE_TYPE  + ",  ");
			cql.append(" :" + CassandraColumns.PAYLOAD  + "  ");
			cql.append(" ) USING TIMESTAMP ? ; ");
			
			
			//2. 
			PreparedStatement ps = PreparedStatementCacheFactory.getInstance().get(session, cql.toString(), ConsistencyLevel.ONE);
			BoundStatement query = new BoundStatement(ps);
			query.setString(CassandraColumns.ASSET_ID, asset.getAsset_id());
			query.setString(CassandraColumns.STATEMENT_NAME, stmt.getStatement_name());
			query.setString(CassandraColumns.STATEMENT_DESCRIPTION, stmt.getDescription());
			query.setTimestamp(CassandraColumns.TIMESTAMP, new Date(timestamp));
			query.setString(CassandraColumns.TIMELINE_TYPE, timeline_type);
			query.setString(CassandraColumns.PAYLOAD, payload);
			query.setLong(6, timestamp);
			
			session.executeAsync(query);

			long end = System.currentTimeMillis() - start;
			log.debug("Cassandra insert asset time line : exec_time=[" + end + "]");

		} catch (Exception ex) {
			DataFlowErrorCount.ERROR_STORE_ASSET_INSERT.incrementAndGet();
			log.error("Insert asset time line error : CQL=[" + cql.toString() + "], " + ex.getMessage(), ex);
			throw ex;
		} finally {
			//
			// if (session != null && !session.isClosed())
			// session.close();
		}
	}
	
	@Override
	public void insertAssetData(long timestamp, Asset asset, Map<String, Tag> tag_map, Map<String, Point> point_map) throws Exception {
		//
		Session session = null;
		StringBuffer cql = new StringBuffer();
		
		//
		try {

			//
			long start = System.currentTimeMillis();

			//
			session     = CassandraSessionFixer.getSession();
			String keyspace    = CassandraSessionFixer.getKeyspace();
			
	        //1.
			Map<String,String> data_map = new HashMap<String,String>();
			Map<String,Long> timestamp_map = new HashMap<String,Long>();
			Iterator<String> titer = tag_map.keySet().iterator();
	        while( titer.hasNext() ){
	        	Tag tag = tag_map.get(titer.next());
	        	String filed_name = AliasUtils.getAliasName(tag);
				Point point = point_map.get(tag.getTag_id());
				if(point != null){
					data_map.put(filed_name, point.getValue());
					timestamp_map.put(filed_name, point.getTimestamp());
				}else{
					data_map.put(filed_name, "");
					timestamp_map.put(filed_name, new Long(0));
				}
			};

			//2. CQL 
			cql = new StringBuffer();
			cql.append(" INSERT INTO  " + keyspace + "." + StorageConstants.ASSET_DATA_TABLE + " ( ");
			cql.append(" " + CassandraColumns.TIMESTAMP + ",  ");
			cql.append(" " + CassandraColumns.DATE  + ",  ");
			cql.append(" " + CassandraColumns.HOUR  + ",  ");
			cql.append(" " + CassandraColumns.MINUTE  + ",  ");
			cql.append(" " + CassandraColumns.ASSET_ID  + ",  ");
			cql.append(" " + CassandraColumns.ASSET_TYPE  + ",  ");
			cql.append(" " + CassandraColumns.DATA_MAP  + ",  ");
			cql.append(" " + CassandraColumns.TIMESTAMP_MAP  + "  ");
			cql.append(" )  ");
			cql.append(" VALUES ");
			cql.append(" ( ");
			cql.append(" :" + CassandraColumns.TIMESTAMP + ",  ");
			cql.append(" :" + CassandraColumns.DATE  + ",  ");
			cql.append(" :" + CassandraColumns.HOUR  + ",  ");
			cql.append(" :" + CassandraColumns.MINUTE  + ",  ");
			cql.append(" :" + CassandraColumns.ASSET_ID  + ",  ");
			cql.append(" :" + CassandraColumns.ASSET_TYPE  + ",  ");
			cql.append(" :" + CassandraColumns.DATA_MAP  + ",  ");
			cql.append(" :" + CassandraColumns.TIMESTAMP_MAP  + "  ");
			cql.append(" )  USING TIMESTAMP ? ; ");

			//2. 
			PreparedStatement ps = PreparedStatementCacheFactory.getInstance().get(session, cql.toString(), ConsistencyLevel.ONE);
			BoundStatement query = new BoundStatement(ps);
			query.setIdempotent(true);
			
			query.setTimestamp(CassandraColumns.TIMESTAMP, new Date(timestamp));
			query.setInt(CassandraColumns.DATE,   TimestampUtils.timestampToYYYYMMDD(timestamp));
			query.setInt(CassandraColumns.HOUR,   TimestampUtils.getHH(timestamp));
			query.setInt(CassandraColumns.MINUTE, TimestampUtils.getMM(timestamp));
			query.setString(CassandraColumns.ASSET_ID, asset.getAsset_id());
			query.setString(CassandraColumns.ASSET_TYPE, asset.getTable_type());
			query.setMap(CassandraColumns.DATA_MAP, data_map);
			query.setMap(CassandraColumns.TIMESTAMP_MAP, timestamp_map);
			query.setLong(8, timestamp);
			session.executeAsync(query);
			
			long end = System.currentTimeMillis() - start;
			log.debug("Cassandra insert asset point data : exec_time=[" + end + "]");

		} catch (Exception ex) {
			DataFlowErrorCount.ERROR_STORE_ASSET_INSERT.incrementAndGet();
			log.error("Insert asset point data error : CQL=[" + cql.toString() + "], " + ex.getMessage(), ex);
			throw ex;
		} finally {
			//
			// if (session != null && !session.isClosed())
			// session.close();
		}
	}
	
	
	@Override
	public void insertAssetDataSampling(String term, long timestamp, Asset asset, Map<String, Tag> tag_map, Map<String, Point> point_map) throws Exception {
		//
		Session session = null;
		StringBuffer cql = new StringBuffer();
		
		//
		try {

			//
			long start = System.currentTimeMillis();

			//
			session     = CassandraSessionFixer.getSession();
			String keyspace    = CassandraSessionFixer.getKeyspace();
			
	        //1. �뜲�씠�꽣 留� 留뚮벉
			Map<String,String> data_map = new HashMap<String,String>();
			Map<String,Long> timestamp_map = new HashMap<String,Long>();
			Iterator<String> titer = tag_map.keySet().iterator();
	        while( titer.hasNext() ){
	        	Tag tag = tag_map.get(titer.next());
	        	String filed_name = AliasUtils.getAliasName(tag);
				Point point = point_map.get(tag.getTag_id());
				if(point != null){
					data_map.put(filed_name, point.getValue());
					timestamp_map.put(filed_name, point.getTimestamp());
				}else{
					data_map.put(filed_name, "");
					timestamp_map.put(filed_name, new Long(0));
				}
			};

			//2. CQL 
			cql = new StringBuffer();
			cql.append(" INSERT INTO  " + keyspace + "." + StorageConstants.ASSET_DATA_TABLE + "_sampling_" + (term.replace(" ", "_").toLowerCase())  + " ( ");
			cql.append(" " + CassandraColumns.TIMESTAMP + ",  ");
			cql.append(" " + CassandraColumns.DATE  + ",  ");
			cql.append(" " + CassandraColumns.HOUR  + ",  ");
			cql.append(" " + CassandraColumns.MINUTE  + ",  ");
			cql.append(" " + CassandraColumns.ASSET_ID  + ",  ");
			cql.append(" " + CassandraColumns.ASSET_TYPE  + ",  ");
			cql.append(" " + CassandraColumns.DATA_MAP  + ",  ");
			cql.append(" " + CassandraColumns.TIMESTAMP_MAP  + "  ");
			cql.append(" )  ");
			cql.append(" VALUES ");
			cql.append(" ( ");
			cql.append(" :" + CassandraColumns.TIMESTAMP + ",  ");
			cql.append(" :" + CassandraColumns.DATE  + ",  ");
			cql.append(" :" + CassandraColumns.HOUR  + ",  ");
			cql.append(" :" + CassandraColumns.MINUTE  + ",  ");
			cql.append(" :" + CassandraColumns.ASSET_ID  + ",  ");
			cql.append(" :" + CassandraColumns.ASSET_TYPE  + ",  ");
			cql.append(" :" + CassandraColumns.DATA_MAP  + ",  ");
			cql.append(" :" + CassandraColumns.TIMESTAMP_MAP  + "  ");
			cql.append(" )  USING TIMESTAMP ? ; ");

			//2. 
			PreparedStatement ps = PreparedStatementCacheFactory.getInstance().get(session, cql.toString(), ConsistencyLevel.ONE);
			BoundStatement query = new BoundStatement(ps);
			query.setIdempotent(true);
			
			query.setTimestamp(CassandraColumns.TIMESTAMP, new Date(timestamp));
			query.setInt(CassandraColumns.DATE,   TimestampUtils.timestampToYYYYMMDD(timestamp));
			query.setInt(CassandraColumns.HOUR,   TimestampUtils.getHH(timestamp));
			query.setInt(CassandraColumns.MINUTE, TimestampUtils.getMM(timestamp));
			query.setString(CassandraColumns.ASSET_ID, asset.getAsset_id());
			query.setString(CassandraColumns.ASSET_TYPE, asset.getTable_type());
			query.setMap(CassandraColumns.DATA_MAP, data_map);
			query.setMap(CassandraColumns.TIMESTAMP_MAP, timestamp_map);
			query.setLong(8, timestamp);
			session.executeAsync(query);
			
			long end = System.currentTimeMillis() - start;
			log.debug("Cassandra insert asset point data sampling : term=[" + term + "], exec_time=[" + end + "]");

		} catch (Exception ex) {
			DataFlowErrorCount.ERROR_STORE_ASSET_INSERT.incrementAndGet();
			log.error("Insert asset point data sampling error : CQL=[" + cql.toString() + "], " + ex.getMessage(), ex);
			throw ex;
		} finally {
			//
			// if (session != null && !session.isClosed())
			// session.close();
		}
	}

	@Override
	public void insertAssetHealthStatus(long current_timestamp, Asset model, long from_timestamp, long to_timestamp, String status, int info_count, int warn_count, int error_count) throws Exception {
		//

		Session session = null;
		StringBuffer cql = new StringBuffer();
		String table = "";
		//
		try {

			//
			long start = System.currentTimeMillis();

			session     = CassandraSessionFixer.getSession();
			String keyspace    = CassandraSessionFixer.getKeyspace();

			//
			table = StorageConstants.ASSET_HEALTH_STATUS_TABLE;
			

			cql = new StringBuffer();
			cql.append(" INSERT INTO  " + keyspace + "." + table + " ( ");
			cql.append(" " + CassandraColumns.ASSET_ID + ", ");
			cql.append(" " + CassandraColumns.TIMESTAMP + ", ");
			cql.append(" " + CassandraColumns.FROM + ",  ");
			cql.append(" " + CassandraColumns.TO + ", ");
			cql.append(" " + CassandraColumns.STATUS + ", ");
			cql.append(" " + CassandraColumns.ALARM_INFO_COUNT + ", ");
			cql.append(" " + CassandraColumns.ALARM_WARN_COUNT + ", ");
			cql.append(" " + CassandraColumns.ALARM_ERROR_COUNT + " ");
			cql.append(" ) ");
			cql.append(" VALUES ");
			cql.append(" ( ");
			cql.append(" ?,");
			cql.append(" ?,");
			cql.append(" ?,");
			cql.append(" ?,");
			cql.append(" ?,");
			cql.append(" ?,");
			cql.append(" ?,");
			cql.append(" ? ");
			cql.append(" ) USING TIMESTAMP ? ; ");

			PreparedStatement ps = PreparedStatementCacheFactory.getInstance().get(session, cql.toString(), ConsistencyLevel.ONE);

			BoundStatement query = new BoundStatement(ps);
			// EX) SNAPSHOT_10_MINUTES
			query.setString(CassandraColumns.ASSET_ID,  model.getAsset_id());
			query.setTimestamp(CassandraColumns.TIMESTAMP, new Date(current_timestamp));
			query.setLong(CassandraColumns.FROM, from_timestamp);
			query.setLong(CassandraColumns.TO ,  to_timestamp);
			query.setString(CassandraColumns.STATUS, status);
			query.setInt(CassandraColumns.ALARM_INFO_COUNT ,  info_count);
			query.setInt(CassandraColumns.ALARM_WARN_COUNT ,  warn_count);
			query.setInt(CassandraColumns.ALARM_ERROR_COUNT , error_count);
			query.setLong(8, current_timestamp);
			session.executeAsync(query);

			long end = System.currentTimeMillis() - start;
			log.debug("Cassandra insert asset health status data : table=[" + table + "] : exec_time=[" + end + "]");

		} catch (Exception ex) {
			DataFlowErrorCount.ERROR_STORE_ASSET_HEALTH_INSERT.incrementAndGet();
			log.error("[" + table + "] asset health status insert error : CQL=[" + cql.toString() + "], " + ex.getMessage(), ex);
			throw ex;
		} finally {
			//
			// if (session != null && !session.isClosed())
			// session.close();
		}
	};
	
	@Override
	public void insertAssetConnectionStatus(long current_timestamp, Asset model, long from_timestamp, long to_timestamp, String status) throws Exception {
		//

		Session session = null;
		StringBuffer cql = new StringBuffer();
		String table = "";
		//
		try {

			//
			long start = System.currentTimeMillis();

			//

			session     = CassandraSessionFixer.getSession();
			String keyspace    = CassandraSessionFixer.getKeyspace();

			//
			table = StorageConstants.ASSET_CONNECTION_STATUS_TABLE;
			

			cql = new StringBuffer();
			cql.append(" INSERT INTO  " + keyspace + "." + table + " ( ");
			cql.append(" " + CassandraColumns.ASSET_ID + ", ");
			cql.append(" " + CassandraColumns.TIMESTAMP + ", ");
			cql.append(" " + CassandraColumns.FROM + ",  ");
			cql.append(" " + CassandraColumns.TO + ", ");
			cql.append(" " + CassandraColumns.STATUS + " ");
			cql.append(" ) ");
			cql.append(" VALUES ");
			cql.append(" ( ");
			cql.append(" ");
			cql.append(" ?,");
			cql.append(" ?,");
			cql.append(" ?,");
			cql.append(" ?,");
			cql.append(" ?");
			cql.append(" ) USING TIMESTAMP ? ; ");

			PreparedStatement ps = PreparedStatementCacheFactory.getInstance().get(session, cql.toString(), ConsistencyLevel.ONE);

			BoundStatement query = new BoundStatement(ps);
			// EX) SNAPSHOT_10_MINUTES
			query.setString(CassandraColumns.ASSET_ID,  model.getAsset_id());
			query.setTimestamp(CassandraColumns.TIMESTAMP, new Date(current_timestamp));
			query.setLong(CassandraColumns.FROM, from_timestamp);
			query.setLong(CassandraColumns.TO ,  to_timestamp);
			query.setString(CassandraColumns.STATUS, status);
			query.setLong(5, current_timestamp);
			session.executeAsync(query);

			long end = System.currentTimeMillis() - start;
			log.debug("Cassandra insert asset connection status data : table=[" + table + "] : exec_time=[" + end + "]");

		} catch (Exception ex) {
			DataFlowErrorCount.ERROR_STORE_ASSET_HEALTH_INSERT.incrementAndGet();
			log.error("[" + table + "] asset connection status insert error : CQL=[" + cql.toString() + "], " + ex.getMessage(), ex);
			throw ex;
		} finally {
			//
			// if (session != null && !session.isClosed())
			// session.close();
		}
	};
	
	
	@Override
	public void insertAssetEvent(AssetStatement stmt, long current_timestamp, Asset model, String event, long from_timestamp, long to_timestamp,  String color, Map<String,String> data, String note) throws Exception {
		//

		Session session = null;
		StringBuffer cql = new StringBuffer();
		String table = "";
		//
		try {

			//
			long start = System.currentTimeMillis();

			//

			session     = CassandraSessionFixer.getSession();
			String keyspace    = CassandraSessionFixer.getKeyspace();

			//
			table = StorageConstants.ASSET_EVENT_TABLE;
			

			cql = new StringBuffer();
			cql.append(" INSERT INTO  " + keyspace + "." + table + " ( ");
			cql.append(" " + CassandraColumns.ASSET_ID + ", ");
			cql.append(" " + CassandraColumns.STATEMENT_NAME  + ",  ");
			cql.append(" " + CassandraColumns.STATEMENT_DESCRIPTION  + ",  ");
			cql.append(" " + CassandraColumns.TIMESTAMP + ", ");
			cql.append(" " + CassandraColumns.FROM + ",  ");
			cql.append(" " + CassandraColumns.TO + ", ");
			cql.append(" " + CassandraColumns.EVENT + ", ");
			cql.append(" " + CassandraColumns.COLOR + ", ");
			cql.append(" " + CassandraColumns.DATA + ", ");
			cql.append(" " + CassandraColumns.NOTE + "  ");
			cql.append(" ) ");
			cql.append(" VALUES ");
			cql.append(" ( ");
			cql.append(" ");
			cql.append(" ?,");
			cql.append(" ?,");
			cql.append(" ?,");
			cql.append(" ?,");
			cql.append(" ?,");
			cql.append(" ?,");
			cql.append(" ?,");
			cql.append(" ?,");
			cql.append(" ?,");
			cql.append(" ? ");
			cql.append(" ) USING TIMESTAMP ? ; ");

			PreparedStatement ps = PreparedStatementCacheFactory.getInstance().get(session, cql.toString(), ConsistencyLevel.ONE);

			BoundStatement query = new BoundStatement(ps);
			query.setString(CassandraColumns.ASSET_ID,  model.getAsset_id());
			query.setString(CassandraColumns.STATEMENT_NAME, stmt.getStatement_name());
			query.setString(CassandraColumns.STATEMENT_DESCRIPTION, stmt.getDescription());
			query.setTimestamp(CassandraColumns.TIMESTAMP, new Date(current_timestamp));
			query.setLong(CassandraColumns.FROM, from_timestamp);
			query.setLong(CassandraColumns.TO ,  to_timestamp);
			query.setString(CassandraColumns.EVENT, event);
			query.setString(CassandraColumns.COLOR, color);
			query.setMap(CassandraColumns.DATA, data);
			query.setString(CassandraColumns.NOTE, note);
			query.setLong(10, current_timestamp);
			session.executeAsync(query);

			long end = System.currentTimeMillis() - start;
			log.debug("Cassandra insert asset event data : table=[" + table + "] : exec_time=[" + end + "]");

		} catch (Exception ex) {
			DataFlowErrorCount.ERROR_STORE_ASSET_EVENT_INSERT.incrementAndGet();
			log.error("[" + table + "] asset event insert error : CQL=[" + cql.toString() + "], " + ex.getMessage(), ex);
			throw ex;
		} finally {
			//
			// if (session != null && !session.isClosed())
			// session.close();
		}
	}

	
	@Override
	public void insertAssetAggregation(AssetStatement stmt,long current_timestamp, Asset model, String aggregation, String key, String value) throws Exception {
		//

		Session session = null;
		StringBuffer cql = new StringBuffer();
		String table = "";
		//
		try {

			//
			long start = System.currentTimeMillis();

			//

			session     = CassandraSessionFixer.getSession();
			String keyspace    = CassandraSessionFixer.getKeyspace();

			//
			table = StorageConstants.ASSET_AGGREGATION_TABLE;
			

			cql = new StringBuffer();
			cql.append(" INSERT INTO  " + keyspace + "." + table + " ( ");
			cql.append(" " + CassandraColumns.ASSET_ID + ", ");
			cql.append(" " + CassandraColumns.STATEMENT_NAME  + ",  ");
			cql.append(" " + CassandraColumns.STATEMENT_DESCRIPTION  + ",  ");
			cql.append(" " + CassandraColumns.TIMESTAMP + ", ");
			cql.append(" " + CassandraColumns.AGGREGATION + ",  ");
			cql.append(" " + CassandraColumns.KEY   + ",  ");
			cql.append(" " + CassandraColumns.VALUE + "  ");
			cql.append(" ) ");
			cql.append(" VALUES ");
			cql.append(" ( ");
			cql.append(" ?,");
			cql.append(" ?,");
			cql.append(" ?,");
			cql.append(" ?,");
			cql.append(" ?,");
			cql.append(" ?,");
			cql.append(" ? ");
			cql.append(" ) USING TIMESTAMP ? ; ");

			PreparedStatement ps = PreparedStatementCacheFactory.getInstance().get(session, cql.toString(), ConsistencyLevel.ONE);

			BoundStatement query = new BoundStatement(ps);
			// EX) SNAPSHOT_10_MINUTES
			query.setString(CassandraColumns.ASSET_ID,  model.getAsset_id());
			query.setString(CassandraColumns.STATEMENT_NAME, stmt.getStatement_name());
			query.setString(CassandraColumns.STATEMENT_DESCRIPTION, stmt.getDescription());
			query.setTimestamp(CassandraColumns.TIMESTAMP, new Date(current_timestamp));
			query.setString(CassandraColumns.AGGREGATION, aggregation);
			query.setString(CassandraColumns.KEY ,  key);
			query.setString(CassandraColumns.VALUE ,  value);
			query.setLong(7, current_timestamp);
			session.executeAsync(query);

			long end = System.currentTimeMillis() - start;
			log.debug("Cassandra insert asset aggregation data : table=[" + table + "] : exec_time=[" + end + "]");

		} catch (Exception ex) {
			DataFlowErrorCount.ERROR_STORE_ASSET_AGGREGATION_INSERT.incrementAndGet();
			log.error("[" + table + "] asset aggregation insert error : CQL=[" + cql.toString() + "], " + ex.getMessage(), ex);
			throw ex;
		} finally {
			//
			// if (session != null && !session.isClosed())
			// session.close();
		}
	}

	
	@Override
	public void insertAssetContext(AssetStatement stmt, long current_timestamp, Asset model, String context, String key, String value) throws Exception {
		//

		Session session = null;
		StringBuffer cql = new StringBuffer();
		String table = "";
		//
		try {

			//
			long start = System.currentTimeMillis();

			//

			session     = CassandraSessionFixer.getSession();
			String keyspace    = CassandraSessionFixer.getKeyspace();

			//
			table = StorageConstants.ASSET_CONTEXT_TABLE;
			

			cql = new StringBuffer();
			cql.append(" INSERT INTO  " + keyspace + "." + table + " ( ");
			cql.append(" " + CassandraColumns.ASSET_ID + ", ");
			cql.append(" " + CassandraColumns.STATEMENT_NAME  + ",  ");
			cql.append(" " + CassandraColumns.STATEMENT_DESCRIPTION  + ",  ");
			cql.append(" " + CassandraColumns.TIMESTAMP + ", ");
			cql.append(" " + CassandraColumns.CONTEXT + ",  ");
			cql.append(" " + CassandraColumns.KEY + ",  ");
			cql.append(" " + CassandraColumns.VALUE + "  ");
			cql.append(" ) ");
			cql.append(" VALUES ");
			cql.append(" ( ");
			cql.append(" ?,");
			cql.append(" ?,");
			cql.append(" ?,");
			cql.append(" ?,");
			cql.append(" ?,");
			cql.append(" ?,");
			cql.append(" ? ");
			cql.append(" ) USING TIMESTAMP ? ; ");

			PreparedStatement ps = PreparedStatementCacheFactory.getInstance().get(session, cql.toString(), ConsistencyLevel.ONE);

			BoundStatement query = new BoundStatement(ps);
			// EX) SNAPSHOT_10_MINUTES
			query.setString(CassandraColumns.ASSET_ID,  model.getAsset_id());
			query.setString(CassandraColumns.STATEMENT_NAME, stmt.getStatement_name());
			query.setString(CassandraColumns.STATEMENT_DESCRIPTION, stmt.getDescription());
			query.setTimestamp(CassandraColumns.TIMESTAMP, new Date(current_timestamp));
			query.setString(CassandraColumns.CONTEXT, context);
			query.setString(CassandraColumns.KEY ,  key);
			query.setString(CassandraColumns.VALUE ,  value);
			query.setLong(7, current_timestamp);
			session.executeAsync(query);

			long end = System.currentTimeMillis() - start;
			log.debug("Cassandra insert asset context data : table=[" + table + "] : exec_time=[" + end + "]");

		} catch (Exception ex) {
			DataFlowErrorCount.ERROR_STORE_ASSET_AGGREGATION_INSERT.incrementAndGet();
			log.error("[" + table + "] asset context insert error : CQL=[" + cql.toString() + "], " + ex.getMessage(), ex);
			throw ex;
		} finally {
			//
			// if (session != null && !session.isClosed())
			// session.close();
		}
	}

	
	
	@Override
	public void insertSystemHealthStatus(long current_timestamp, long from_timestamp, long to_timestamp, String status, int info_count, int warn_count, int error_count) throws Exception {
		//

		Session session = null;
		StringBuffer cql = new StringBuffer();
		String table = "";
		//
		try {

			//
			long start = System.currentTimeMillis();

			//

			session     = CassandraSessionFixer.getSession();

			//
			table = StorageConstants.SYSTEM_HEALTH_STATUS_TABLE;
			

			cql = new StringBuffer();
			cql.append(" INSERT INTO  " + StorageConstants.SYSTEM_KEYSPACE + "." + StorageConstants.SYSTEM_HEALTH_STATUS_TABLE + " ( ");
			cql.append(" " + CassandraColumns.APP_NAME + ", ");
			cql.append(" " + CassandraColumns.TIMESTAMP + ", ");
			cql.append(" " + CassandraColumns.FROM + ",  ");
			cql.append(" " + CassandraColumns.TO + ", ");
			cql.append(" " + CassandraColumns.STATUS + ", ");
			cql.append(" " + CassandraColumns.ALARM_INFO_COUNT + ", ");
			cql.append(" " + CassandraColumns.ALARM_WARN_COUNT + ", ");
			cql.append(" " + CassandraColumns.ALARM_ERROR_COUNT + " ");
			cql.append(" ) ");
			cql.append(" VALUES ");
			cql.append(" ( ");
			cql.append(" ");
			cql.append(" ?,");
			cql.append(" ?,");
			cql.append(" ?,");
			cql.append(" ?,");
			cql.append(" ?,");
			cql.append(" ?,");
			cql.append(" ?,");
			cql.append(" ? ");
			cql.append(" ) USING TIMESTAMP ?; ");

			PreparedStatement ps = PreparedStatementCacheFactory.getInstance().get(session, cql.toString(), ConsistencyLevel.ONE);

			BoundStatement query = new BoundStatement(ps);
			// EX) SNAPSHOT_10_MINUTES
			query.setString(CassandraColumns.APP_NAME,  StorageConstants.SYSTEM_APP_NAME);
			query.setTimestamp(CassandraColumns.TIMESTAMP, new Date(current_timestamp));
			query.setLong(CassandraColumns.FROM, from_timestamp);
			query.setLong(CassandraColumns.TO ,  to_timestamp);
			query.setString(CassandraColumns.STATUS, status);
			query.setInt(CassandraColumns.ALARM_INFO_COUNT ,  info_count);
			query.setInt(CassandraColumns.ALARM_WARN_COUNT ,  warn_count);
			query.setInt(CassandraColumns.ALARM_ERROR_COUNT , error_count);
			query.setLong(8, current_timestamp);
			session.executeAsync(query);

			long end = System.currentTimeMillis() - start;
			log.debug("Cassandra insert system health status data : table=[" + table + "] : exec_time=[" + end + "]");

		} catch (Exception ex) {
			log.error("[" + table + "] system health status insert error : CQL=[" + cql.toString() + "], " + ex.getMessage(), ex);
			throw ex;
		} finally {
			//
			// if (session != null && !session.isClosed())
			// session.close();
		}
	};
	

	@Override
	public void insertDomainChangeHistory(long current_timestamp, String domain_type, String object_id, String json) throws Exception {
		//

		Session session = null;
		StringBuffer cql = new StringBuffer();
		String table = "";
		//
		try {

			//
			long start = System.currentTimeMillis();

			//

			session     = CassandraSessionFixer.getSession();
			String keyspace    = CassandraSessionFixer.getKeyspace();

			//
			table = StorageConstants.DOMAIN_CHANGE_HISTORY_TABLE;
			

			cql = new StringBuffer();
			cql.append(" INSERT INTO  " + keyspace + "." + table + " ( ");
			cql.append(" " + CassandraColumns.DOMAIN_TYPE + ", ");
			cql.append(" " + CassandraColumns.OBJECT_ID + ", ");
			cql.append(" " + CassandraColumns.TIMESTAMP + ", ");
			cql.append(" " + CassandraColumns.JSON + "  ");
			cql.append(" ) ");
			cql.append(" VALUES ");
			cql.append(" ( ");
			cql.append(" ");
			cql.append(" ?,");
			cql.append(" ?,");
			cql.append(" ?,");
			cql.append(" ? ");
			cql.append(" ) USING TIMESTAMP ? ; ");

			PreparedStatement ps = PreparedStatementCacheFactory.getInstance().get(session, cql.toString(), ConsistencyLevel.ONE);

			BoundStatement query = new BoundStatement(ps);
			query.setString(CassandraColumns.DOMAIN_TYPE,  domain_type);
			query.setString(CassandraColumns.OBJECT_ID,  object_id);
			query.setTimestamp(CassandraColumns.TIMESTAMP, new Date(current_timestamp));
			query.setString(CassandraColumns.JSON, json);
			query.setLong(4, current_timestamp);
			session.executeAsync(query);

			long end = System.currentTimeMillis() - start;
			log.debug("Cassandra insert domain change history data : table=[" + table + "] : exec_time=[" + end + "]");

		} catch (Exception ex) {
			log.error("[" + table + "] domain change history insert error : CQL=[" + cql.toString() + "], " + ex.getMessage(), ex);
			throw ex;
		} finally {
			//
		}
	};
	
	
	
	@Override
	public void insertListenerPushCache(long current_timestamp, String url, String json) throws Exception {
		//

		Session session = null;
		StringBuffer cql = new StringBuffer();
		String table = "";
		//
		try {

			//
			long start = System.currentTimeMillis();

			//

			session     = CassandraSessionFixer.getSession();
			String keyspace    = CassandraSessionFixer.getKeyspace();

			//
			table = StorageConstants.OPTION_LISTENER_PUSH_CACHE_TABLE;
			

			cql = new StringBuffer();
			cql.append(" INSERT INTO  " + keyspace + "." + table + " ( ");
			cql.append(" " + CassandraColumns.URL + ", ");
			cql.append(" " + CassandraColumns.TIMESTAMP + ", ");
			cql.append(" " + CassandraColumns.JSON + "  ");
			cql.append(" ) ");
			cql.append(" VALUES ");
			cql.append(" ( ");
			cql.append(" ");
			cql.append(" ?,");
			cql.append(" ?,");
			cql.append(" ? ");
			cql.append(" ) USING TIMESTAMP ?; ");

			PreparedStatement ps = PreparedStatementCacheFactory.getInstance().get(session, cql.toString(), ConsistencyLevel.ONE);

			BoundStatement query = new BoundStatement(ps);
			query.setString(CassandraColumns.URL,  url);
			query.setTimestamp(CassandraColumns.TIMESTAMP, new Date(current_timestamp));
			query.setString(CassandraColumns.JSON, json);
			query.setLong(3, current_timestamp);
			session.executeAsync(query);

			long end = System.currentTimeMillis() - start;
			log.debug("Cassandra insert option listener push json status data : table=[" + table + "] : exec_time=[" + end + "]");

		} catch (Exception ex) {
			log.error("[" + table + "] listener push json insert error : CQL=[" + cql.toString() + "], " + ex.getMessage(), ex);
			throw ex;
		} finally {
			//
			// if (session != null && !session.isClosed())
			// session.close();
		}
	};

	private void executeBatch(Session session, final BatchStatement batch_statement) throws Exception {
		final Semaphore semaphore = new Semaphore(15000);
		try {
			boolean trace = false;
			if (trace) {
				ListeningExecutorService executor =
						MoreExecutors.listeningDecorator(Executors.newFixedThreadPool(2));
				semaphore.acquire();
				batch_statement.setConsistencyLevel(ConsistencyLevel.ONE);
				batch_statement.enableTracing();
				ResultSetFuture resultSetFuture = session.executeAsync(batch_statement);
				
				Futures.addCallback(resultSetFuture, new FutureCallback<ResultSet>() {
					@Override
					public void onSuccess(com.datastax.driver.core.ResultSet rs) {
						List<ExecutionInfo> executionInfoList = rs.getAllExecutionInfo();
						for (int i = 0; i < executionInfoList.size(); i++) {
							ExecutionInfo executionInfo = executionInfoList.get(i);
							log.info(String.format("Batch target host (queried): %s", executionInfo.getQueriedHost().toString()));
							for (Host host : executionInfo.getTriedHosts()) {
								log.info(String.format("Batch target host (tried): %s", host.toString()));
							}
							;

							QueryTrace queryTrace = executionInfo.getQueryTrace();
							if (queryTrace != null) {
								log.info(String.format("Trace id: %s", queryTrace.getTraceId()));
								log.info(String.format("Duration micros: %d", queryTrace.getDurationMicros()));
								log.info("---------------------------------------+--------------+------------+--------------");
								log.info(String.format("%-38s | %-12s | %-10s | %-12s", "activity", "timestamp", "source", "source_elapsed"));
								log.info("---------------------------------------+--------------+------------+--------------");
								for (QueryTrace.Event event : queryTrace.getEvents()) {
									log.info(String.format("%38s | %12s | %10s | %12s", event.getDescription(), StorageUtils.millis2Date(event.getTimestamp()), event.getSource(), event.getSourceElapsedMicros()));
								}
								log.info("---------------------------------------+--------------+------------+--------------");
								// batch_statement.disableTracing();
							}

						}
						semaphore.release();
					}

					@Override
					public void onFailure(Throwable throwable) {
						log.error(throwable.toString(), throwable);
						semaphore.release();
					}
				}, executor);

			} else {
				// batch_statement.setConsistencyLevel(ConsistencyLevel.ONE);
				// ResultSetFuture future =
				// session.executeAsync(batch_statement);
				// future.getUninterruptibly();
				session.executeAsync(batch_statement);

			}

		} catch (NoHostAvailableException e) {
			log.error("No host in the " + session.getCluster() + " cluster can be contacted to execute the query.", e);
			throw e;
		} catch (QueryExecutionException e) {
			log.error("An exception was thrown by Cassandra because it cannot " + "successfully execute the query with the specified consistency level.", e);
			throw e;
		} catch (QueryValidationException e) {
			log.error("The query is not valid, for example, incorrect syntax.", e);
			throw e;
		} catch (IllegalStateException e) {
			log.error("The BatchStatement is not ready.", e);
			throw e;
		}
	}




}
