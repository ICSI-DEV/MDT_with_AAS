package plantpulse.cep.version.update;

import java.sql.Connection;
import java.util.ArrayList;
import java.util.List;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import plantpulse.cep.db.ConnectionUtils;
import plantpulse.cep.version.VersionUpdate;
import plantpulse.dbutils.QueryRunner;

public class VersionUpdate_V8_1_0_20210207 implements VersionUpdate {

	private static final Log log = LogFactory.getLog(VersionUpdate_V8_1_0_20210207.class);
	
	@Override
	public long getVersion() {
		return 20210207; //반드시 바꿔야 업데이트 됩니다.
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
			sql_list.add(" ALTER TABLE mm_tags ADD COLUMN group_name varchar(100) ");
			sql_list.add(" DROP TABLE user_login_session ");
			sql_list.add(" CREATE TABLE user_login_session (\r\n"
					+ "   login_id      VARCHAR(100) NOT NULL,\r\n"
					+ "   session_key   VARCHAR(100) NOT NULL,\r\n"
					+ "   session_value VARCHAR(4000) NOT NULL,\r\n"
					+ "   update_date   DATETIME,\r\n"
					+ "   PRIMARY KEY (login_id, session_key) \r\n"
					+ ")    ");
			
			for(int i=0; i < sql_list.size(); i++) {
				run.update(conn, sql_list.get(i));
			};
			
			
			//
			run.update(conn, " CHECKPOINT; ");

		}catch(Exception ex) {
			log.error("VersionUpdate_V8_1_0_20210207 - HSQLDB 업그레이드 에러 : " + ex.getMessage(), ex);
		}finally {
			hsqldb.closeConnection(conn);
		};
		
		//-------------------------------------------------------------------------------------------
		//2.카산드라 스키마 업그레이드
		//-------------------------------------------------------------------------------------------
		
		log.info("버전 " + getVersion() + " 업데이트 완료");
	}

}


