package plantpulse.cep.dao;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.util.List;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import plantpulse.cep.db.ConnectionUtils;
import plantpulse.cep.domain.Event;
import plantpulse.cep.domain.EventAttribute;
import plantpulse.cep.domain.Statement;
import plantpulse.dbutils.QueryRunner;
import plantpulse.dbutils.handlers.BeanHandler;
import plantpulse.dbutils.handlers.BeanListHandler;

public class CEPServiceDAO extends CommonDAO {

	private static final Log log = LogFactory.getLog(CEPServiceDAO.class);

	// EVENT
	public List<Event> getEventList() throws Exception {

		Connection connection = null;
		List<Event> list = null;
		//
		try {
			//
			StringBuffer query = new StringBuffer();
			query.append("SELECT  * ");
			query.append("  FROM MM_EVENT                       ");
			query.append(" ORDER BY NVL(LAST_UPDATE_DATE, INSERT_DATE) DESC ");

			QueryRunner run = new QueryRunner();
			connection = super.getConnection();
			list = (List<Event>) run.query(connection, query.toString(), new BeanListHandler(Event.class), new Object[] {});

			// append attribute
			for (int i = 0; list != null && i < list.size(); i++) {
				Event event = list.get(i);
				query = new StringBuffer();
				query.append("SELECT  * ");
				query.append("  FROM MM_EVENT_ATTRIBUTES                       ");
				query.append("  WHERE EVENT_NAME = ?                       ");
				query.append(" ORDER BY SORT_ORDER ASC ");

				run = new QueryRunner();
				List<EventAttribute> attr_list = (List<EventAttribute>) run.query(connection, query.toString(), new BeanListHandler(EventAttribute.class), new Object[] { event.getEvent_name() });

				EventAttribute[] event_attributes = attr_list.toArray(new EventAttribute[attr_list.size()]);
				list.get(i).setEvent_attributes(event_attributes);
			}

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

	public Event getEvent(String event_name) throws Exception {

		Connection connection = null;
		Event one = null;
		//
		try {

			StringBuffer query = new StringBuffer();
			query.append("SELECT  * ");
			query.append("  FROM MM_EVENT                       ");
			query.append(" WHERE EVENT_NAME = ?       \n");
			query.append(" ORDER BY NVL(LAST_UPDATE_DATE, INSERT_DATE) DESC ");

			QueryRunner run = new QueryRunner();

			connection = super.getConnection();

			one = (Event) run.query(connection, query.toString(), new BeanHandler(Event.class), new Object[] { event_name });

			// apend attributes
			query = new StringBuffer();
			query.append("SELECT  * ");
			query.append("  FROM MM_EVENT_ATTRIBUTES                       ");
			query.append("  WHERE EVENT_NAME = ?                       ");
			query.append(" ORDER BY SORT_ORDER ASC ");

			run = new QueryRunner();
			List<EventAttribute> attr_list = (List<EventAttribute>) run.query(connection, query.toString(), new BeanListHandler(EventAttribute.class), new Object[] { event_name });

			EventAttribute[] event_attributes = attr_list.toArray(new EventAttribute[attr_list.size()]);
			one.setEvent_attributes(event_attributes);

			//

			//
		} catch (Exception ex) {
			log.error(ex);
			throw ex;
		} finally {
			super.closeConnection(connection);
		}
		return one;
	}

	public void saveEvent(Event event) throws Exception {
		Connection connection = null;

		//
		try {
			connection = super.getConnection();
			connection.setAutoCommit(false);

			QueryRunner run = new QueryRunner();

			// 1. 마스터 저장
			StringBuffer query = new StringBuffer();
			query.append("\n");
			query.append("INSERT INTO MM_EVENT                   \n");
			query.append("       (                                            \n");
			query.append("         EVENT_NAME,                            \n");
			query.append("         EVENT_DESC,                            \n");
			query.append("         INSERT_DATE                                \n");
			query.append("       ) values (                                   \n");
			query.append("         ?,        \n");
			query.append("         ?,        \n");
			query.append("         SYSDATE   \n");
			query.append("       )           \n");
			query.append(";\n");

			run.update(connection, query.toString(), new Object[] { event.getEvent_name(), event.getEvent_desc() });

			// 2. 아트리뷰트 저장
			for (int i = 0; i < event.getEvent_attributes().length; i++) {
				EventAttribute attribute = event.getEvent_attributes()[i];
				query = new StringBuffer();
				query.append("\n");
				query.append("INSERT INTO MM_EVENT_ATTRIBUTES                    \n");
				query.append("       (                                            \n");
				query.append("         EVENT_NAME,                            \n");
				query.append("         FIELD_NAME,                            \n");
				query.append("         FIELD_DESC,                            \n");
				query.append("         JAVA_TYPE,                                \n");
				query.append("         SORT_ORDER                                 \n");
				query.append("       ) values (                                   \n");
				query.append("         ?,        \n");
				query.append("         ?,        \n");
				query.append("         ?,        \n");
				query.append("         ?,        \n");
				query.append("         ?        \n");
				query.append("       )           \n");
				query.append(";\n");

				run.update(connection, query.toString(),
						new Object[] { event.getEvent_name(), attribute.getField_name(), attribute.getField_desc(), attribute.getJava_type(), attribute.getSort_order() });

			}
			;
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

	public void updateEvent(Event event) throws Exception {

		Connection connection = null;

		try {

			connection = super.getConnection();

			//
			StringBuffer query = new StringBuffer();
			query.append("UPDATE MM_EVENT                  \n");
			query.append("   SET EVENT_NAME = ? ,                 \n");
			query.append("       EVENT_DESC = ?  ,                  \n");
			query.append("       LAST_UPDATE_DATE = SYSDATE        \n");
			query.append(" WHERE EVENT_NAME = ?   \n");
			//
			QueryRunner run = new QueryRunner();
			run.update(connection, query.toString(), new Object[] { event.getEvent_name(), event.getEvent_desc(), event.getEvent_name() });

			// 2. 아트리뷰트 저장
			query = new StringBuffer();
			query.append("DELETE FROM MM_EVENT_ATTRIBUTES WHERE EVENT_NAME = ?  \n");
			run.update(connection, query.toString(), new Object[] { event.getEvent_name() });

			for (int i = 0; i < event.getEvent_attributes().length; i++) {
				EventAttribute attribute = event.getEvent_attributes()[i];
				query = new StringBuffer();
				query.append("\n");
				query.append("INSERT INTO MM_EVENT_ATTRIBUTES                    \n");
				query.append("       (                                            \n");
				query.append("         EVENT_NAME,                            \n");
				query.append("         FIELD_NAME,                            \n");
				query.append("         FIELD_DESC,                            \n");
				query.append("         JAVA_TYPE,                                \n");
				query.append("         SORT_ORDER                                 \n");
				query.append("       ) values (                                   \n");
				query.append("         ?,        \n");
				query.append("         ?,        \n");
				query.append("         ?,        \n");
				query.append("         ?,        \n");
				query.append("         ?        \n");
				query.append("       )           \n");
				query.append(";\n");

				run.update(connection, query.toString(),
						new Object[] { event.getEvent_name(), attribute.getField_name(), attribute.getField_desc(), attribute.getJava_type(), attribute.getSort_order() });

			}
			;

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

	public void deleteEvent(String event_name) throws Exception {

		Connection connection = null;

		try {

			connection = super.getConnection();
			connection.setAutoCommit(false);

			//
			StringBuffer query = new StringBuffer();
			query.append("DELETE FROM MM_EVENT WHERE EVENT_NAME = ? \n");
			//
			QueryRunner run = new QueryRunner();
			run.update(connection, query.toString(), new Object[] { event_name });

			// 2. 아트리뷰트 저장
			query = new StringBuffer();
			query.append("DELETE FROM MM_EVENT_ATTRIBUTES WHERE EVENT_NAME = ?  \n");
			run.update(connection, query.toString(), new Object[] { event_name });

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

	/**
	 * Get statement list
	 * 
	 * @return
	 * @throws Exception
	 */
	@SuppressWarnings("unchecked")
	public List<Statement> getStatementList() throws Exception {
		Connection connection = null;
		List<Statement> list = null;
		//
		try {
			//
			StringBuffer query = new StringBuffer();
			query.append("SELECT ");
			query.append("  	STATEMENT_NAME AS statementName, ");
			query.append(" 	 	STATEMENT_DESC AS statementDesc, ");
			query.append("  	EPL AS epl, ");
			query.append("  	IS_PATTERN AS isPattern, ");
			query.append("  	LISTENER AS listener, ");
			query.append("  	INSERT_DATE AS insertDate, ");
			query.append("  	LAST_UPDATE_DATE AS lastUpdateDate ");
			query.append("  FROM MM_STATEMENT                        ");
			query.append(" ORDER BY NVL(LAST_UPDATE_DATE, INSERT_DATE) DESC ");

			QueryRunner run = new QueryRunner();
			connection = super.getConnection();
			list = (List<Statement>) run.query(connection, query.toString(), new BeanListHandler(Statement.class), new Object[] {});

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

	public Statement getStatement(String statementName) throws Exception {
		Connection connection = null;

		Statement one = null;
		//
		try {

			StringBuffer query = new StringBuffer();
			query.append("SELECT ");
			query.append("  	STATEMENT_NAME AS statementName, ");
			query.append("  	STATEMENT_DESC AS statementDesc, ");
			query.append("  	EPL AS epl, ");
			query.append("  	IS_PATTERN AS isPattern, ");
			query.append("  	LISTENER AS listener, ");
			query.append("  	INSERT_DATE AS insertDate, ");
			query.append("  	LAST_UPDATE_DATE AS lastUpdateDate  ");
			query.append("  FROM MM_STATEMENT                        ");
			query.append(" WHERE STATEMENT_NAME = ?       \n");

			QueryRunner run = new QueryRunner();

			connection = super.getConnection();

			one = (Statement) run.query(super.getConnection(), query.toString(), new BeanHandler(Statement.class), new Object[] { statementName });

			//
		} catch (Exception ex) {
			log.error(ex);
			throw ex;
		} finally {
			super.closeConnection(connection);
		}
		return one;

	}

	/**
	 * Save statement
	 * 
	 * @throws Exception
	 */
	public void saveStatement(Statement statement) throws Exception {
		Connection connection = null;

		//
		try {

			StringBuffer query = new StringBuffer();
			query.append("\n");
			query.append("INSERT INTO MM_STATEMENT                    \n");
			query.append("       (                                            \n");
			query.append("         STATEMENT_NAME,                            \n");
			query.append("         STATEMENT_DESC,                            \n");
			query.append("         EPL,                                       \n");
			query.append("         LISTENER,                                  \n");
			query.append("         IS_PATTERN,                                \n");
			query.append("         INSERT_DATE                                \n");
			query.append("       ) values (                                   \n");
			query.append("         ?,        \n");
			query.append("         ?,        \n");
			query.append("         ?,                  \n");
			query.append("         ?,             \n");
			query.append("         ?,            \n");
			query.append("         SYSDATE                                    \n");
			query.append("       )                                            \n");
			query.append(";\n");

			QueryRunner run = new QueryRunner();
			connection = super.getConnection();
			run.update(connection, query.toString(),
					new Object[] { statement.getStatementName(), statement.getStatementDesc(), statement.getEpl(), statement.getListener(), statement.getIsPattern() });

			//
		} catch (Exception ex) {
			log.error(ex);
			throw ex;
		} finally {
			super.closeConnection(connection);
		}

	}

	public void updateStatement(Statement statement) throws Exception {
		ConnectionUtils utils = new ConnectionUtils();
		Connection con = null;
		PreparedStatement pstmt = null;
		ResultSet rs = null;

		try {
			//
			con = utils.getConnection();
			StringBuffer query = new StringBuffer();
			query.append("\n");
			query.append("UPDATE MM_STATEMENT                                 \n");
			query.append("   SET STATEMENT_DESC = '" + statement.getStatementDesc() + "', \n");
			query.append("       EPL = '" + statement.getEpl() + "',                      \n");
			query.append("       LISTENER = '" + statement.getListener() + "',            \n");
			query.append("       IS_PATTERN = '" + statement.getIsPattern() + "',         \n");
			query.append("       LAST_UPDATE_DATE = SYSDATE                           \n");
			query.append(" WHERE STATEMENT_NAME = '" + statement.getStatementName() + "'  \n");
			query.append(";\n");
			//
			log.debug("[updateStatement query] : " + query.toString());
			con.prepareStatement(query.toString()).execute();
		} catch (Exception ex) {
			log.error("[CEPServiceDAO.updateStatement] " + ex.getMessage(), ex);
			throw ex;
		} finally {
			if (rs != null)
				rs.close();
			if (pstmt != null)
				pstmt.close();
			utils.closeConnection(con);
		}
	}

	public void deleteStatement(String statementName) throws Exception {
		ConnectionUtils utils = new ConnectionUtils();
		Connection con = null;
		PreparedStatement pstmt = null;
		ResultSet rs = null;

		try {
			//
			con = utils.getConnection();
			StringBuffer query = new StringBuffer();
			query.append("\n");
			query.append("DELETE FROM MM_STATEMENT             \n");
			query.append(" WHERE STATEMENT_NAME = '" + statementName + "' \n");
			query.append(";\n");
			//
			log.debug("[deleteStatement query] : " + query.toString());
			con.prepareStatement(query.toString()).execute();
		} catch (Exception ex) {
			log.error("[CEPServiceDAO.deleteStatement] " + ex.getMessage(), ex);
			throw ex;
		} finally {
			if (rs != null)
				rs.close();
			if (pstmt != null)
				pstmt.close();
			utils.closeConnection(con);
		}
	}

}
