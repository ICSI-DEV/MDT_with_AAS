package plantpulse.cep.version.update;

import java.sql.Connection;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import plantpulse.cep.db.ConnectionUtils;
import plantpulse.cep.version.VersionUpdate;
import plantpulse.dbutils.QueryRunner;

/**
 * VersionUpdate_V6_0_1_20181006 업데이트
 * 
 * - 카산드라 필드 추가
 * - HSQLDB 버전 - 사용자 속성 필드 추가
 * - 
 * @author leesa
 *
 */
public class VersionUpdate_V6_0_1_20181006 implements VersionUpdate {

	private static final Log log = LogFactory.getLog(VersionUpdate_V6_0_1_20181006.class);
	
	@Override
	public long getVersion() {
		return 20181006L;
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
			/*
			run.update(conn, " ALTER TABLE PUBLIC.PP_USER ADD ATTR_01 VARCHAR(100); ");
			run.update(conn, " ALTER TABLE PUBLIC.PP_USER ADD ATTR_02 VARCHAR(100); ");
			run.update(conn, " ALTER TABLE PUBLIC.PP_USER ADD ATTR_03 VARCHAR(100); ");
			run.update(conn, " ALTER TABLE PUBLIC.PP_USER ADD ATTR_04 VARCHAR(100); ");
			run.update(conn, " ALTER TABLE PUBLIC.PP_USER ADD ATTR_05 VARCHAR(100); ");
			run.update(conn, " ALTER TABLE PUBLIC.PP_USER ADD ATTR_06 VARCHAR(100); ");
			run.update(conn, " ALTER TABLE PUBLIC.PP_USER ADD ATTR_07 VARCHAR(100); ");
			run.update(conn, " ALTER TABLE PUBLIC.PP_USER ADD ATTR_08 VARCHAR(100); ");
			run.update(conn, " ALTER TABLE PUBLIC.PP_USER ADD ATTR_09 VARCHAR(100); ");
			run.update(conn, " ALTER TABLE PUBLIC.PP_USER ADD ATTR_10 VARCHAR(100); ");
			*/
			
			//
			run.update(conn, " CHECKPOINT; ");

		}catch(Exception ex) {
			log.error("VersionUpdate_V6_0_1_20181006 - HSQLDB 업그레이드 에러 : " + ex.getMessage(), ex);
		}finally {
			hsqldb.closeConnection(conn);
		};
		
		//-------------------------------------------------------------------------------------------
		//2.카산드라 스키마 업그레이드
		//-------------------------------------------------------------------------------------------
		
		
		log.info("버전 " + getVersion() + " 업데이트 완료");
	}

}


