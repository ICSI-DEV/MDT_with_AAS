package plantpulse.cep.dao;

import java.sql.Connection;
import java.util.List;
import java.util.Map;

import org.apache.commons.lang.StringUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import plantpulse.cep.domain.Server;
import plantpulse.dbutils.QueryRunner;
import plantpulse.dbutils.handlers.BeanHandler;
import plantpulse.dbutils.handlers.BeanListHandler;

/**
 * ServerDAO
 * 
 * @author
 *
 */
public class ServerDAO extends CommonDAO {
	private static final Log log = LogFactory.getLog(ServerDAO.class);

	public List<Server> getServerList() throws Exception {
		Connection connection = null;
		List<Server> list = null;
		//
		try {
			//
			StringBuffer query = new StringBuffer();
			query.append("SELECT  * 				");
			query.append("  FROM MM_SERVER	");
			query.append(" ORDER BY NVL(LAST_UPDATE_DATE, INSERT_DATE) DESC ");

			QueryRunner run = new QueryRunner();
			connection = super.getConnection();
			list = run.query(connection, query.toString(), new BeanListHandler<Server>(Server.class), new Object[] {});

			log.debug(list.toString());

			//
		} catch (Exception ex) {
			log.error(ex);
			throw ex;
		} finally {
			super.closeConnection(connection);
		}
		return list;
	}

	public List<Server> getServerList(Map<String, Object> params) throws Exception {
		Connection connection = null;
		List<Server> list = null;
		String serverName = StringUtils.defaultString((String) params.get("server_name"), "");

		log.info("\n serverName : {" + serverName + "}");
		//
		try {
			//
			StringBuffer query = new StringBuffer();
			query.append("SELECT  * 				");
			query.append("  FROM MM_SERVER	");
			query.append(" WHERE SERVER_NAME LIKE '%' || ? || '%'  	\n");
			query.append(" ORDER BY NVL(LAST_UPDATE_DATE, INSERT_DATE) DESC ");

			QueryRunner run = new QueryRunner();
			connection = super.getConnection();
			list = run.query(connection, query.toString(), new BeanListHandler<Server>(Server.class), new Object[] { serverName });
			//
		} catch (Exception ex) {
			log.error(ex);
			throw ex;
		} finally {
			super.closeConnection(connection);
		}
		return list;
	}

	public Server getServer(String server_id) throws Exception {
		Connection connection = null;
		Server server = null;
		//
		try {

			StringBuffer query = new StringBuffer();
			query.append("SELECT  * ");
			query.append("  FROM MM_SERVER		\n");
			query.append(" WHERE SERVER_ID  = ?     	\n");

			QueryRunner run = new QueryRunner();
			connection = super.getConnection();
			server = run.query(connection, query.toString(), new BeanHandler<Server>(Server.class), new Object[] { server_id });
			//
		} catch (Exception ex) {
			log.error(ex);
			throw ex;
		} finally {
			super.closeConnection(connection);
		}
		return server;
	}

	public void saveServer(Server server) throws Exception {
		Connection connection = null;
		//
		try {

			connection = super.getConnection();
			connection.setAutoCommit(false);

			QueryRunner run = new QueryRunner();

			StringBuffer query = new StringBuffer();
			query.append("\n");
			query.append("INSERT INTO MM_SERVER     \n");
			query.append("       (                          \n");
			query.append("         SERVER_ID, 	            \n");
			query.append("         SERVER_NAME,             \n");
			query.append("         CONFIG,                  \n");
			query.append("         AGENT_ID,                \n");
			query.append("         LOGS,                    \n");
			query.append("         INSERT_DATE              \n");
			query.append("       ) values (                 \n");
			query.append("         ?,						\n");
			query.append("         ?,						\n");
			query.append("         ?,						\n");
			query.append("         ?,        				\n");
			query.append("         ?,        				\n");
			query.append("         SYSDATE  				\n");
			query.append("       )           				\n");
			query.append(";									\n");

			run.update(connection, query.toString(), new Object[] { server.getServer_id(), server.getServer_name(), server.getConfig(), server.getAgent_id(), server.getLogs() });

			connection.commit();
			//
		} catch (Exception ex) {
			connection.rollback();
			ex.printStackTrace();
			log.error(ex);
			throw ex;
		} finally {
			connection.setAutoCommit(true);
			super.closeConnection(connection);
		}
	}

	public void updateServer(Server server) throws Exception {
		Connection connection = null;
		try {
			connection = super.getConnection();
			connection.setAutoCommit(false);
			//
			StringBuffer query = new StringBuffer();
			query.append("UPDATE MM_SERVER              \n");
			query.append("   SET SERVER_NAME = ? ,              \n");
			query.append("        CONFIG = ? ,                 	\n");
			query.append("        AGENT_ID = ? ,           		\n");
			query.append("        LOGS = ? ,   	            	\n");
			query.append("        LAST_UPDATE_DATE = SYSDATE	\n");
			query.append(" WHERE SERVER_ID = ?   				\n");
			//
			QueryRunner run = new QueryRunner();
			run.update(connection, query.toString(), new Object[] { server.getServer_name(), server.getConfig(), server.getAgent_id(), server.getLogs(), server.getServer_id() });
			connection.commit();
			//
		} catch (Exception ex) {
			connection.rollback();
			log.error(ex);
			throw ex;
		} finally {
			connection.setAutoCommit(true);
			super.closeConnection(connection);
		}
	}

	public void deleteServer(String server_id) throws Exception {
		Connection connection = null;
		try {
			connection = super.getConnection();
			connection.setAutoCommit(false);

			//
			StringBuffer query = new StringBuffer();
			query.append("DELETE FROM MM_SERVER WHERE SERVER_ID = ? \n");
			//
			QueryRunner run = new QueryRunner();
			run.update(connection, query.toString(), new Object[] { server_id });

			connection.commit();
			//
		} catch (Exception ex) {
			connection.rollback();
			log.error(ex);
			throw ex;
		} finally {
			connection.setAutoCommit(true);
			super.closeConnection(connection);
		}
	}

}
