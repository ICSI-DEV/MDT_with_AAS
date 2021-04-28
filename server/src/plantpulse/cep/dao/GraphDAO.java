package plantpulse.cep.dao;

import java.sql.Connection;
import java.util.List;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.json.JSONObject;

import plantpulse.cep.domain.Graph;
import plantpulse.dbutils.QueryRunner;
import plantpulse.dbutils.handlers.BeanHandler;
import plantpulse.dbutils.handlers.BeanListHandler;
import plantpulse.domain.Tag;

/**
 * 
 * GRAPH_ID VARCHAR(50)PRIMARY KEY, GRAPH_TITLE VARCHAR(50), GRAPH_DESC
 * VARCHAR(1000), GRAPH_GRANT VARCHAR(1000), GRAPH_CONFIG VARCHAR(1000),
 * INSERT_USER_ID VARCHAR(50), INSERT_DATE TIMESTAMP, LAST_UPDATE_DATE
 * TIMESTAMP)
 * 
 * @author lenovo
 * 
 */
public class GraphDAO extends CommonDAO {

	private static final Log log = LogFactory.getLog(GraphDAO.class);

	public Tag selectTagByQuickGraph(String tag_id) throws Exception {

		Connection connection = null;
		Tag tag = null;

		try {

			StringBuffer query = new StringBuffer();
			query.append("SELECT A.*,          \n");
			query.append("       B.SITE_ID     \n");
			query.append("  FROM MM_TAGS A,   \n");
			query.append("       MM_OPCS B    \n");
			query.append(" WHERE A.TAG_ID = ?    \n");
			query.append("   AND A.OPC_ID = B.OPC_ID   \n");

			QueryRunner run = new QueryRunner();
			connection = super.getConnection();
			tag = run.query(connection, query.toString(), new BeanHandler<Tag>(Tag.class), new Object[] { tag_id });

		} catch (Exception e) {
			log.error(e);
			throw e;
		} finally {
			super.closeConnection(connection);
		}

		return tag;
	}

	public List<Graph> getGraphListAll() throws Exception {
		Connection connection = null;
		List<Graph> list = null;
		//
		try {
			//
			StringBuffer query = new StringBuffer();
			query.append("SELECT *          ");
			query.append("  FROM MM_GRAPH   ");
			query.append(" ORDER BY NVL(LAST_UPDATE_DATE, INSERT_DATE) DESC ");
			query.append("");

			QueryRunner run = new QueryRunner();
			connection = super.getConnection();
			list = run.query(connection, query.toString(), new BeanListHandler<Graph>(Graph.class));

			//
		} catch (Exception ex) {
			log.error(ex);
		} finally {
			super.closeConnection(connection);
		}
		return list;

	}

	/**
	 * Get Graph list
	 * 
	 * @return
	 * @throws Exception
	 */

	public List<Graph> getGraphListByUserId(String insert_user_id) throws Exception {

		Connection connection = null;
		List<Graph> list = null;
		//
		try {
			//
			StringBuffer query = new StringBuffer();
			query.append("SELECT *          ");
			query.append("  FROM MM_GRAPH   ");
			query.append("  WHERE INSERT_USER_ID = ?   ");
			query.append(" ORDER BY NVL(LAST_UPDATE_DATE, INSERT_DATE) DESC ");
			query.append("");

			QueryRunner run = new QueryRunner();
			connection = super.getConnection();
			list = run.query(connection, query.toString(), new BeanListHandler<Graph>(Graph.class), new Object[] { insert_user_id });

			//
		} catch (Exception ex) {
			log.error(ex);
		} finally {
			super.closeConnection(connection);
		}
		return list;

	}

	public List<Graph> getGraphListByDashboardId(String dashboard_id) throws Exception {
		Connection connection = null;
		List<Graph> list = null;
		//
		try {
			//
			StringBuffer query = new StringBuffer();
			query.append("SELECT *          ");
			query.append("  FROM MM_GRAPH   ");
			query.append("  WHERE DASHBOARD_ID = ?   ");
			query.append(" ORDER BY NVL(LAST_UPDATE_DATE, INSERT_DATE) DESC ");
			query.append("");

			QueryRunner run = new QueryRunner();
			connection = super.getConnection();
			list = run.query(connection, query.toString(), new BeanListHandler<Graph>(Graph.class), new Object[] { dashboard_id });

			//
		} catch (Exception ex) {
			log.error(ex);
		} finally {
			super.closeConnection(connection);
		}
		return list;

	}

	public Graph getGraphById(String graphId) throws Exception {
		Connection connection = null;
		Graph one = null;
		//
		try {
			//
			StringBuffer query = new StringBuffer();
			query.append("SELECT *          ");
			query.append("  FROM  MM_GRAPH   ");
			query.append("  WHERE GRAPH_ID = ?   ");
			query.append("");

			QueryRunner run = new QueryRunner();
			connection = super.getConnection();
			one = run.query(connection, query.toString(), new BeanHandler<Graph>(Graph.class), new Object[] { graphId });

			// log.info("one=" + one.toString());

			//
		} catch (Exception ex) {
			log.error(ex);
		} finally {
			super.closeConnection(connection);
		}
		return one;

	}

	public void insertGraph(Graph graph) throws Exception {
		Connection connection = null;
		//
		try {

			StringBuffer query = new StringBuffer();
			query.append("INSERT INTO MM_GRAPH                                ");
			query.append("       (                                            ");
			query.append("         DASHBOARD_ID,                            ");
			query.append("         GRAPH_ID,                            ");
			query.append("         GRAPH_TITLE,                            ");
			query.append("         GRAPH_PRIORITY,                            ");
			query.append("         GRAPH_DESC,                                       ");
			query.append("         GRAPH_TYPE,                                       ");
			query.append("         GRAPH_JSON,                                ");
			query.append("         EPL,                                ");
			query.append("         WS_URL,                                ");
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
			query.append("         ?,       ");
			query.append("         ?,       ");
			query.append("         ?,       ");
			query.append("         SYSDATE, ");
			query.append("         SYSDATE                                    ");
			query.append("       )                                            ");

			QueryRunner run = new QueryRunner();
			connection = super.getConnection();
			run.update(connection, query.toString(), new Object[] { graph.getDashboard_id(), graph.getGraph_id(), graph.getGraph_title(), graph.getGraph_priority(), graph.getGraph_desc(),
					graph.getGraph_type(), graph.getGraph_json(), graph.getEpl(), graph.getWs_url(), graph.getInsert_user_id() });
			//

			log.info("GRAPH_JSON=" + JSONObject.fromObject(graph).toString());

		} catch (Exception ex) {
			log.error(ex);
		} finally {
			super.closeConnection(connection);
		}

	}

	public void updateGraph(Graph graph) throws Exception {
		Connection connection = null;
		try {
			//
			StringBuffer query = new StringBuffer();
			query.append("UPDATE MM_GRAPH                  \n");
			query.append("   SET GRAPH_TITLE = ? ,                 \n");
			query.append("       GRAPH_PRIORITY = ? ,               \n");
			query.append("       GRAPH_DESC = ?  ,                  \n");
			query.append("       LAST_UPDATE_DATE = SYSDATE        \n");
			query.append(" WHERE GRAPH_ID = ?   \n");
			//
			QueryRunner run = new QueryRunner();
			connection = super.getConnection();
			run.update(connection, query.toString(), new Object[] { graph.getGraph_title(), graph.getGraph_priority(), graph.getGraph_desc(), graph.getGraph_id() });
		} catch (Exception ex) {
			log.error(ex);
		} finally {
			super.closeConnection(connection);
		}
	}

	public void deleteGraph(String graphId) throws Exception {
		Connection connection = null;

		try {
			//
			StringBuffer query = new StringBuffer();
			query.append("\n");
			query.append("DELETE FROM MM_GRAPH  \n");
			query.append(" WHERE GRAPH_ID = ?   \n");
			query.append(";\n");
			//
			QueryRunner run = new QueryRunner();
			connection = super.getConnection();
			run.update(connection, query.toString(), new Object[] { graphId });
		} catch (Exception ex) {
			log.error(ex);
		} finally {
			super.closeConnection(connection);
		}
	}
}
