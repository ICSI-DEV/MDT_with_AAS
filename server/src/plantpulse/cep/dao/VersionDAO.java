package plantpulse.cep.dao;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.Date;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import plantpulse.cep.db.ConnectionUtils;
import plantpulse.dbutils.QueryRunner;

/**
 * VersionDAO
 * 
 * @author leesa
 *
 */
public class VersionDAO extends CommonDAO {

	private static final Log log = LogFactory.getLog(VersionDAO.class);

	/**
	 * 패치된 버진인지 확인한다.
	 * 
	 * @param userId
	 * @param password
	 * @return
	 */
	public void createVersionHistoryTable() {
		//
		ConnectionUtils utils = new ConnectionUtils();
		Connection con = null;
		Statement stmt = null;
		ResultSet rs = null;
		try {
			//
			con = utils.getConnection();
			StringBuffer query = new StringBuffer();
			query.append(" CREATE TABLE IF NOT EXISTS VERSION_HISTORY (VERSION bigint, PATCHED_DATE date) ");
			//
			stmt = con.createStatement(); 
			stmt.execute(query.toString());

		} catch (Exception ex) {
			log.error(ex.getMessage(), ex);
		} finally {
			utils.closeConnection(rs, stmt, con);
		}
	}
	
	
	/**
	 * 패치된 버진인지 확인한다.
	 * 
	 * @param userId
	 * @param password
	 * @return
	 */
	public boolean checkPatchedVersion(long version) {
		//
		ConnectionUtils utils = new ConnectionUtils();
		Connection con = null;
		PreparedStatement pstmt = null;
		ResultSet rs = null;

		//
		boolean is_patched = false;
		try {
			//
			con = utils.getConnection();
			StringBuffer query = new StringBuffer();
			query.append("SELECT *                              \n");
			query.append("  FROM VERSION_HISTORY                \n");
			query.append(" WHERE VERSION = " + version + "     \n");
			//
			pstmt = con.prepareStatement(query.toString());
			rs = pstmt.executeQuery();
			log.debug("[checkPatched query] : " + query.toString());
			//
			if (rs.next()) {
				is_patched = true;
			}
		} catch (Exception ex) {
			log.error(ex.getMessage(), ex);
		} finally {
			utils.closeConnection(rs, pstmt, con);
		}
		return is_patched;
	}

	
    /**
     * 패치가 완료된 버전은 입력 처리한다.
     * 
     * @param version
     * @throws Exception
     */
	public void insertVersion(long version) throws Exception {
		Connection connection = null;
		try {
			connection = super.getConnection();
			//
			StringBuffer query = new StringBuffer();
			query.append(" INSERT INTO VERSION_HISTORY (VERSION, PATCHED_DATE) ");
			query.append(" VALUES (?, ?) ");
			//
			QueryRunner run = new QueryRunner();
			run.update(connection, query.toString(), new Object[] { version, new Date() });
			//
		} catch (Exception ex) {
			log.error(ex);
			throw ex;
		} finally {
			super.closeConnection(connection);
		}
	}

}
