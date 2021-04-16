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
public class VersionUpdate_V7_1_0_20200405 implements VersionUpdate {

	private static final Log log = LogFactory.getLog(VersionUpdate_V7_1_0_20200405.class);
	
	@Override
	public long getVersion() {
		return 20200405L;
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
			sql.append("  ALTER TABLE MM_TAGS ADD DB_BUCKET INT DEFAULT 0 ");
			run.update(conn, sql.toString());
			
			//
			run.update(conn, " CHECKPOINT; ");

		}catch(Exception ex) {
			log.error("VersionUpdate_V7_1_0_20200405 - HSQLDB 업그레이드 에러 : " + ex.getMessage(), ex);
		}finally {
			hsqldb.closeConnection(conn);
		};
		
		//-------------------------------------------------------------------------------------------
		//2.카산드라 스키마 업그레이드
		//-------------------------------------------------------------------------------------------
		
		
		log.info("버전 " + getVersion() + " 업데이트 완료");
	}

}


