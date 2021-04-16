package plantpulse.cep.dao;

import java.sql.Connection;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import plantpulse.dbutils.QueryRunner;

/**
 * 주기적으로 체크포인트를 실행한다.
 * 
 * @author leesa
 *
 */
public class CheckpointDAO extends CommonDAO {

	private static final Log log = LogFactory.getLog(CompanyDAO.class);

	public void checkpoint() throws Exception {
		Connection connection = null;
		//
		try {
			QueryRunner run = new QueryRunner();
			connection = super.getConnection();
			run.update(connection, " CHECKPOINT ");
			//
		} catch (Exception ex) {
			log.error("HSQLDB Checkpoint Exception : " + ex.getMessage());
			throw ex;
		} finally {
			super.closeConnection(connection);
		}
	}
}