package plantpulse.cep.dao;

import java.io.File;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.realdisplay.framework.util.FileUtils;
import org.springframework.stereotype.Repository;

import plantpulse.cep.context.EngineContext;
import plantpulse.cep.db.ConnectionUtils;

@Repository
public class BackupDAO  extends CommonDAO {

	private static final Log log = LogFactory.getLog(BackupDAO.class);

	/**
	 * HSQLDB 백업 (비동기 실행)
	 * @throws Exception
	 */
	public void backup() throws Exception {
		
		ExecutorService executorService = Executors.newFixedThreadPool(1);
		executorService.execute(new Runnable() {
		    public void run() {
		       
		    	//
		    	ConnectionUtils utils = new ConnectionUtils();
				Connection con = null;
				PreparedStatement pstmt = null;
				ResultSet rs = null;

				try {
					long start = System.currentTimeMillis();
					
					log.debug("HSQLDB backup starting...");
					String root = (String) EngineContext.getInstance().getProps().get("_ROOT");
					String backup_path = root + "/../../../plantpulse-backup/hsqldb/";
					FileUtils.forceMkdir(new File(backup_path));
					
					log.info("HSQLDB backup_path : " + backup_path);
					
					//
					con = utils.getConnection();
					//
					//1. 체크포인트로 script 정리
					log.info("HSQLDB checkpoint execute ... ");
					pstmt = con.prepareStatement(" CHECKPOINT ;  ");
					pstmt.execute();
					//2. 백업 실행
					log.info("HSQLDB backup execute ... ");
					pstmt = con.prepareStatement(" BACKUP DATABASE TO " + "'" + backup_path  + "/' NOT BLOCKING ;  ");
					pstmt.execute();
					
					long end = System.currentTimeMillis() - start;
					log.info("HSQLDB backup completed. : duration=[" + end + "]ms");
					
				} catch (Exception ex) {
					log.error("HSQLDB TimeoutBackup failed :  " + ex.getMessage(), ex);
				} finally {
					if (rs != null)
						try {
							rs.close();
						} catch (SQLException e) {
							e.printStackTrace();
						}
					if (pstmt != null)
						try {
							pstmt.close();
						} catch (SQLException e) {
							e.printStackTrace();
						}
					utils.closeConnection(con);
				}
				
		    }
		});
		
		//
		executorService.shutdown();
		
		
	}

}