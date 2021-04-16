package plantpulse.cep.version.update;

import java.sql.Connection;
import java.util.ArrayList;
import java.util.List;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import plantpulse.cep.db.ConnectionUtils;
import plantpulse.cep.version.VersionUpdate;
import plantpulse.dbutils.QueryRunner;

public class VersionUpdate_V7_1_0_20201208 implements VersionUpdate {

	private static final Log log = LogFactory.getLog(VersionUpdate_V7_1_0_20201208.class);
	
	@Override
	public long getVersion() {
		return 20201208L; //반드시 바꿔야 업데이트 됩니다.
	}
	
	@Override
	public void upgrade() throws Exception {
		log.info("버전 " + getVersion() + " 업데이트 시작 ... ");
		//-------------------------------------------------------------------------------------------
		//1.HSQLDB 업그레이드
		//-------------------------------------------------------------------------------------------
		ConnectionUtils hsqldb = new ConnectionUtils();
		Connection conn = null;
		try {
			
			conn = hsqldb.getConnection();
			
			//
			QueryRunner run = new QueryRunner();
			
			// 메타데이터 테이블 추가 
			List<String> sql_list = new ArrayList<>();
			sql_list.add("  ALTER TABLE MM_TAGS ALTER COLUMN UNIT SET DEFAULT '';    ");
			
			sql_list.add("  ALTER TABLE MM_TAGS ALTER COLUMN TRIP_HI SET DEFAULT ''; ");
			sql_list.add("  ALTER TABLE MM_TAGS ALTER COLUMN HI SET DEFAULT '';      ");
			sql_list.add("  ALTER TABLE MM_TAGS ALTER COLUMN HI_HI SET DEFAULT '';   ");
			sql_list.add("  ALTER TABLE MM_TAGS ALTER COLUMN LO SET DEFAULT '';      ");
			sql_list.add("  ALTER TABLE MM_TAGS ALTER COLUMN LO_LO SET DEFAULT '';   ");
			sql_list.add("  ALTER TABLE MM_TAGS ALTER COLUMN TRIP_LO SET DEFAULT '';   ");
			
			sql_list.add("  ALTER TABLE MM_TAGS ALTER COLUMN MIN_VALUE SET DEFAULT '';   ");
			sql_list.add("  ALTER TABLE MM_TAGS ALTER COLUMN MAX_VALUE SET DEFAULT '';   ");
			
			sql_list.add("  ALTER TABLE MM_TAGS ALTER COLUMN DISPLAY_FORMAT SET DEFAULT '';   ");
			sql_list.add("  ALTER TABLE MM_TAGS ALTER COLUMN LAT SET DEFAULT '';   ");
			sql_list.add("  ALTER TABLE MM_TAGS ALTER COLUMN LNG SET DEFAULT '';   ");
			sql_list.add("  ALTER TABLE MM_TAGS ALTER COLUMN INTERVAL SET DEFAULT 1000;   ");
			
			sql_list.add("  ALTER TABLE MM_TAGS ALTER COLUMN ALIAS_NAME SET DEFAULT '';   ");
			
			sql_list.add("  ALTER TABLE MM_TAGS ALTER COLUMN RECIEVE_ME SET DEFAULT false;   ");
			
			sql_list.add("  ALTER TABLE MM_TAGS ALTER COLUMN USE_CALCULATION SET DEFAULT 'N';   ");
			sql_list.add("  ALTER TABLE MM_TAGS ALTER COLUMN USE_AGGREGATION SET DEFAULT 'N';   ");
			sql_list.add("  ALTER TABLE MM_TAGS ALTER COLUMN USE_ML_FORECAST SET DEFAULT 'N';   ");
			
			sql_list.add("  ALTER TABLE MM_TAGS ALTER COLUMN PUBLISH_MQTT SET DEFAULT  'N';   ");
			sql_list.add("  ALTER TABLE MM_TAGS ALTER COLUMN PUBLISH_STOMP SET DEFAULT 'N';   ");
			sql_list.add("  ALTER TABLE MM_TAGS ALTER COLUMN PUBLISH_KAFKA SET DEFAULT 'N';   ");
			
			
			sql_list.add("  ALTER TABLE MM_TAGS ALTER COLUMN BOOL_TRUE_PRIORITY  SET DEFAULT '';");
			sql_list.add("  ALTER TABLE MM_TAGS ALTER COLUMN BOOL_TRUE_MESSAGE   SET DEFAULT '';");
			sql_list.add("  ALTER TABLE MM_TAGS ALTER COLUMN BOOL_FALSE_PRIORITY SET DEFAULT '';");
			sql_list.add("  ALTER TABLE MM_TAGS ALTER COLUMN BOOL_FALSE_MESSAGE   SET DEFAULT '';");
			
			for(int i=0; i < sql_list.size(); i++) {
				run.update(conn, sql_list.get(i));
			};
			
			
			//
			run.update(conn, " CHECKPOINT; ");

		}catch(Exception ex) {
			log.error("VersionUpdate_V7_1_0_20201207 - HSQLDB 업그레이드 에러 : " + ex.getMessage(), ex);
		}finally {
			hsqldb.closeConnection(conn);
		};
		
		//-------------------------------------------------------------------------------------------
		//2.카산드라 스키마 업그레이드
		//-------------------------------------------------------------------------------------------
		
		
		log.info("버전 " + getVersion() + " 업데이트 완료");
	}

}


