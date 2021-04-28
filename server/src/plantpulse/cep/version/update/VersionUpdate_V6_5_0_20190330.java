package plantpulse.cep.version.update;

import java.sql.Connection;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import plantpulse.cep.db.ConnectionUtils;
import plantpulse.cep.version.VersionUpdate;
import plantpulse.dbutils.QueryRunner;

/**
 * VersionUpdate_V6_0_1_20181012
 * 
 * @author leesa
 *
 */
public class VersionUpdate_V6_5_0_20190330 implements VersionUpdate {

	private static final Log log = LogFactory.getLog(VersionUpdate_V6_5_0_20190330.class);
	
	@Override
	public long getVersion() {
		return 20190330L;
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
			
			// 제어 히스토리 테이블 추가 
			StringBuffer sql = new StringBuffer();
			sql.append("  CREATE TABLE IF NOT EXISTS PUBLIC.MM_CONTROL (       ");
			sql.append("  		   CONTROL_SEQ BIGINT NOT NULL,         	 ");
			sql.append("  		   CONTROL_DATE  TIMESTAMP,                 ");
			sql.append("  		   CONTROL_USER_ID  VARCHAR(100),                 ");
			
			sql.append("  		   SITE_ID     VARCHAR(100) NOT NULL,     	   ");
			sql.append("  		   SITE_NAME   VARCHAR(1000) NOT NULL,     	   ");
			sql.append("  		   OPC_ID      VARCHAR(100) NOT NULL,     	   ");
			sql.append("  		   OPC_NAME    VARCHAR(1000) NOT NULL,     	   ");
			sql.append("  		   ASSET_ID   VARCHAR(100) NOT NULL,     	   ");
			sql.append("  		   ASSET_NAME   VARCHAR(1000) NOT NULL,     	   ");
			sql.append("  		   TAG_ID     VARCHAR(100) NOT NULL,     	   ");
			sql.append("  		   TAG_NAME    VARCHAR(1000) NOT NULL,     	   ");
			
			sql.append("  		   WRITE_VALUE VARCHAR(1000) NOT NULL,	   ");
			sql.append("  		   RESULT VARCHAR(10) NOT NULL,           ");
			sql.append("  		   MESSAGE VARCHAR(1000) NOT NULL,	   ");
			
			sql.append("  		   ERROR_CODE INT NOT NULL,                ");
			sql.append("  		   ERROR_DESCRIPTION VARCHAR(4000),           ");
			
			sql.append("  		   INSERT_DATE  TIMESTAMP,                 ");
			sql.append("  		   PRIMARY KEY (CONTROL_SEQ)               ");
			sql.append("  )   ");
			run.update(conn, sql.toString());
			
			 sql = new StringBuffer();
			 sql.append("  CREATE SEQUENCE IF NOT EXISTS MM_CONTROL_SEQ START WITH 1000000 ");
			 run.update(conn, sql.toString());
			
			//
			run.update(conn, " CHECKPOINT; ");

		}catch(Exception ex) {
			log.error("VersionUpdate_V6_5_0_20190330 - HSQLDB 업그레이드 에러 : " + ex.getMessage(), ex);
		}finally {
			hsqldb.closeConnection(conn);
		};
		
		//-------------------------------------------------------------------------------------------
		//2.카산드라 스키마 업그레이드
		//-------------------------------------------------------------------------------------------
		
		
		log.info("버전 " + getVersion() + " 업데이트 완료");
	}

}


