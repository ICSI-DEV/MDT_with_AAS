package plantpulse.cep.dao;

import java.sql.Connection;
import java.util.List;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import plantpulse.cep.domain.Query;
import plantpulse.dbutils.QueryRunner;
import plantpulse.dbutils.handlers.BeanListHandler;

/**
 * QueryHistoryDAO
 * 
 * @author lenovo
 *
 */
public class QueryHistoryDAO extends CommonDAO {

	private static final Log log = LogFactory.getLog(QueryHistoryDAO.class);

	public List<Query> getQueryHistoryList(String insert_user_id) throws Exception {

		Connection connection = null;
		List<Query> list = null;
		//
		try {
			//
			StringBuffer query = new StringBuffer();
			query.append("SELECT  * ");
			query.append("  FROM MM_QUERY_HISTORY                       ");
			query.append(" WHERE INSERT_USER_ID = ?                       ");
			query.append(" ORDER BY INSERT_DATE DESC ");

			QueryRunner run = new QueryRunner();
			connection = super.getConnection();
			list = (List<Query>) run.query(connection, query.toString(), new BeanListHandler(Query.class), new Object[] { insert_user_id });

			//
		} catch (Exception ex) {
			log.error(ex);
			throw ex;
		} finally {
			super.closeConnection(connection);
		}
		return list;
	}

	public void saveQueryHistory(Query query_history) throws Exception {
		Connection connection = null;

		//
		try {
			connection = super.getConnection();
			connection.setAutoCommit(false);

			QueryRunner run = new QueryRunner();

			// 1. 마스터 저장
			StringBuffer query = new StringBuffer();
			query.append("\n");
			query.append("INSERT INTO MM_QUERY_HISTORY                   \n");
			query.append("       (                                            \n");
			query.append("         QUERY_HISTORY_SEQ,                        \n");
			query.append("         EPL,                            \n");
			query.append("         INSERT_USER_ID,                            \n");
			query.append("         INSERT_DATE                                \n");
			query.append("       ) values (                                   \n");
			query.append("         NEXT VALUE FOR QUERY_HISTORY_SEQ,         \n");
			query.append("         ?,        \n");
			query.append("         ?,        \n");
			query.append("         SYSDATE   \n");
			query.append("       )           \n");
			query.append(";\n");

			run.update(connection, query.toString(), new Object[] { query_history.getEpl(), query_history.getInsert_user_id() });

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

	public void deleteQueryHistory(long query_history_seq) throws Exception {

		Connection connection = null;

		try {

			connection = super.getConnection();
			connection.setAutoCommit(false);

			//
			StringBuffer query = new StringBuffer();
			query.append("DELETE FROM MM_QUERY_HISTORY WHERE QUERY_HISTORY_SEQ = ? \n");
			//
			QueryRunner run = new QueryRunner();
			run.update(connection, query.toString(), new Object[] { query_history_seq });

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

	public void deleteAllQueryHistory(String insert_user_id) throws Exception {

		Connection connection = null;

		try {

			connection = super.getConnection();
			connection.setAutoCommit(false);

			//
			StringBuffer query = new StringBuffer();
			query.append("DELETE FROM MM_QUERY_HISTORY WHERE INSERT_USER_ID = ? \n");
			//
			QueryRunner run = new QueryRunner();
			run.update(connection, query.toString(), new Object[] { insert_user_id });

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
