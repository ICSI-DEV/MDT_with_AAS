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
public class VersionUpdate_V7_0_0_20190826 implements VersionUpdate {

	private static final Log log = LogFactory.getLog(VersionUpdate_V7_0_0_20190826.class);
	
	@Override
	public long getVersion() {
		return 20190826L;
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
			StringBuffer sql = new StringBuffer();
			sql.append("  CREATE TABLE IF NOT EXISTS PUBLIC.MM_METADATA (       ");
			sql.append("  		   OBJECT_ID     VARCHAR(100) NOT NULL,    ");
			sql.append("  		   KEY           VARCHAR(1000) NOT NULL,    ");
			sql.append("  		   VALUE         VARCHAR(1000) NOT NULL,    ");
			sql.append("  		   DESCRIPTION   VARCHAR(4000),             ");
			sql.append("  		   INSERT_DATE   TIMESTAMP,                 ");
			sql.append("  		   UPDATE_DATE   TIMESTAMP,                 ");
			sql.append("  		   PRIMARY KEY (OBJECT_ID, KEY)             ");
			sql.append("  )   ");
			run.update(conn, sql.toString());
			
			//
			run.update(conn, " CHECKPOINT; ");

		}catch(Exception ex) {
			log.error("VersionUpdate_V7_0_0_20190826 - HSQLDB 업그레이드 에러 : " + ex.getMessage(), ex);
		}finally {
			hsqldb.closeConnection(conn);
		};
		
		//-------------------------------------------------------------------------------------------
		//2.카산드라 스키마 업그레이드
		//-------------------------------------------------------------------------------------------
		
		
		log.info("버전 " + getVersion() + " 업데이트 완료");
	}

}


