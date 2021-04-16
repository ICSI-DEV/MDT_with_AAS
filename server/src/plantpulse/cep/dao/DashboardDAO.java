package plantpulse.cep.dao;

import java.sql.Connection;
import java.util.List;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import plantpulse.cep.domain.Dashboard;
import plantpulse.dbutils.QueryRunner;
import plantpulse.dbutils.handlers.BeanHandler;
import plantpulse.dbutils.handlers.BeanListHandler;

/**
 * 
 * DashboardDAO
 * 
 * @author lenovo
 *
 */
public class DashboardDAO extends CommonDAO {

	private static final Log log = LogFactory.getLog(DashboardDAO.class);

	/**
	 * getGraphListByUserId
	 * 
	 * @return
	 * @throws Exception
	 */

	public List<Dashboard> getDashboardListByUserId(String insert_user_id) throws Exception {
		Connection connection = null;
		List<Dashboard> list = null;
		//
		try {
			//
			StringBuffer query = new StringBuffer();
			query.append("SELECT *          ");
			query.append("  FROM MM_DASHBOARD   ");
			query.append("  WHERE INSERT_USER_ID = ?   ");
			query.append(" ORDER BY DASHBOARD_TITLE, NVL(LAST_UPDATE_DATE, INSERT_DATE) DESC ");
			query.append("");
			//
			QueryRunner run = new QueryRunner();
			connection = super.getConnection();
			list = run.query(connection, query.toString(), new BeanListHandler<Dashboard>(Dashboard.class), new Object[] { insert_user_id });
			//
		} catch (Exception ex) {
			log.error(ex);
		} finally {
			super.closeConnection(connection);
		}
		return list;

	}
	
	public List<Dashboard> getDashboardListBySecurityId(String security_id) throws Exception {
		Connection connection = null;
		List<Dashboard> list = null;
		//
		try {
			//
			StringBuffer query = new StringBuffer();
			query.append("SELECT *          ");
			query.append("  FROM MM_DASHBOARD   ");
			query.append("  WHERE SHARE = true  ");
			query.append("    AND SHARE_SECURITY_ID = ?   ");
			query.append(" ORDER BY DASHBOARD_TITLE, NVL(LAST_UPDATE_DATE, INSERT_DATE) DESC ");
			query.append("");

			QueryRunner run = new QueryRunner();
			connection = super.getConnection();

			list = run.query(connection, query.toString(), new BeanListHandler<Dashboard>(Dashboard.class), new Object[] { security_id });

			//
		} catch (Exception ex) {
			log.error(ex);
		} finally {
			super.closeConnection(connection);
		}
		return list;
	}
	
	public Dashboard getDashboardById(String dashboard_id) throws Exception {
		Connection connection = null;
		Dashboard one = null;
		//
		try {
			//
			StringBuffer query = new StringBuffer();
			query.append("SELECT *          ");
			query.append("  FROM  MM_DASHBOARD   ");
			query.append("  WHERE DASHBOARD_ID = ?   ");
			query.append("");

			QueryRunner run = new QueryRunner();
			connection = super.getConnection();
			one = run.query(connection, query.toString(), new BeanHandler<Dashboard>(Dashboard.class), new Object[] { dashboard_id });

			// log.info("one=" + one.toString());

			//
		} catch (Exception ex) {
			log.error(ex);
		} finally {
			super.closeConnection(connection);
		}
		return one;

	}

	public void insertDashboard(Dashboard dashboard) throws Exception {
		Connection connection = null;

		//
		try {
			StringBuffer query = new StringBuffer();
			query.append("INSERT INTO MM_DASHBOARD                                ");
			query.append("       (                                            ");
			query.append("         DASHBOARD_ID,                            ");
			query.append("         DASHBOARD_TITLE,                            ");
			query.append("         DASHBOARD_DESC,                                       ");
			query.append("         DASHBOARD_JSON,                                       ");
			query.append("         SHARE,                                       ");
			query.append("         SHARE_SECURITY_ID,                           ");
			query.append("         INSERT_USER_ID,                                ");
			query.append("         INSERT_DATE,                                ");
			query.append("         LAST_UPDATE_DATE                                ");
			query.append("       ) VALUES (                                   ");
			query.append("         ?,       ");
			query.append("         ?,       ");
			query.append("         ?,       ");
			query.append("         ?,       ");
			query.append("         ?,       ");
			query.append("         ?,       ");
			query.append("         ?,       ");
			query.append("         SYSDATE, ");
			query.append("         SYSDATE                                    ");
			query.append("       )                                            ");

			QueryRunner run = new QueryRunner();
			connection = super.getConnection();

			run.update(connection, query.toString(),
					new Object[] { 
							dashboard.getDashboard_id(), 
							dashboard.getDashboard_title(), 
							dashboard.getDashboard_desc(),
							dashboard.getDashboard_json(), 
							dashboard.isShare(), 
							dashboard.getShare_security_id(), 
							dashboard.getInsert_user_id() 
							});
			//

		} catch (Exception ex) {
			log.error(ex);
		} finally {
			super.closeConnection(connection);
		}

	}

	public void updateDashboard(Dashboard dashboard) throws Exception {
		Connection connection = null;
		try {

			//
			StringBuffer query = new StringBuffer();

			query.append("UPDATE MM_DASHBOARD        ");
			query.append("   SET DASHBOARD_TITLE = ? ,       ");
			query.append("       DASHBOARD_DESC  = ? ,        ");
			query.append("       SHARE = ?,                                       ");
			query.append("       SHARE_SECURITY_ID = ?,                           ");
			query.append("       LAST_UPDATE_DATE = SYSDATE  ");
			query.append(" WHERE DASHBOARD_ID = ?   ");
			//
			QueryRunner run = new QueryRunner();
			connection = super.getConnection();

			run.update(connection, query.toString(), new Object[] { 
					dashboard.getDashboard_title(), 
					dashboard.getDashboard_desc(), 
					dashboard.isShare(), 
					dashboard.getShare_security_id(), 
					dashboard.getDashboard_id() });

		} catch (Exception ex) {
			log.error(ex);
		} finally {
			super.closeConnection(connection);
		}
	}

	public void updateDashboardJson(String dashboard_id, String dashboard_json) throws Exception {
		Connection connection = null;
		try {

			//
			StringBuffer query = new StringBuffer();

			query.append("UPDATE MM_DASHBOARD        ");
			query.append("   SET DASHBOARD_JSON = ?,        ");
			query.append("       LAST_UPDATE_DATE = SYSDATE  ");
			query.append(" WHERE DASHBOARD_ID = ?   ");
			//
			QueryRunner run = new QueryRunner();

			connection = super.getConnection();
			run.update(connection, query.toString(), new Object[] { dashboard_json, dashboard_id });

		} catch (Exception ex) {
			log.error(ex);
		} finally {
			super.closeConnection(connection);
		}
	}

	public void deleteDashboard(String dashboard_id) throws Exception {
		Connection connection = null;
		try {
			connection = super.getConnection();
			connection.setAutoCommit(false);
			//
			StringBuffer query = new StringBuffer();
			query.append("\n");
			query.append("DELETE FROM MM_DASHBOARD  \n");
			query.append(" WHERE DASHBOARD_ID = ?   \n");
			query.append(";\n");
			//
			QueryRunner run = new QueryRunner();

			run.update(connection, query.toString(), new Object[] { dashboard_id });

			//
			query = new StringBuffer();
			query.append("\n");
			query.append("DELETE FROM MM_GRAPH  \n");
			query.append(" WHERE DASHBOARD_ID = ?   \n");
			query.append(";\n");
			//
			run.update(connection, query.toString(), new Object[] { dashboard_id });

			connection.commit();

		} catch (Exception ex) {
			connection.rollback();//
			log.error(ex);
		} finally {
			connection.setAutoCommit(true);
			super.closeConnection(connection);
		}
	}
}
