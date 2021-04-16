package plantpulse.cep.dao;

import java.sql.Connection;
import java.util.List;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import plantpulse.cep.domain.SCADA;
import plantpulse.dbutils.QueryRunner;
import plantpulse.dbutils.handlers.BeanHandler;
import plantpulse.dbutils.handlers.BeanListHandler;

public class SCADADAO extends CommonDAO {

	private static final Log log = LogFactory.getLog(SCADADAO.class);

	/**
	 * getGraphListByUserId
	 * 
	 * @return
	 * @throws Exception
	 */

	public List<SCADA> getScadaListByUserId(String insert_user_id) throws Exception {
		Connection connection = null;
		List<SCADA> list = null;
		//
		try {
			//
			StringBuffer query = new StringBuffer();
			query.append("SELECT *          ");
			query.append("  FROM MM_SCADA   ");
			query.append("  WHERE INSERT_USER_ID = ?   ");
			query.append(" ORDER BY SCADA_TITLE, NVL(LAST_UPDATE_DATE, INSERT_DATE) DESC ");
			query.append("");

			QueryRunner run = new QueryRunner();
			connection = super.getConnection();

			list = run.query(connection, query.toString(), new BeanListHandler<SCADA>(SCADA.class), new Object[] { insert_user_id });

			//
		} catch (Exception ex) {
			log.error(ex);
		} finally {
			super.closeConnection(connection);
		}
		return list;
	}
	
	/**
	 * getScadaListBySecurityId
	 * @param security_id
	 * @return
	 * @throws Exception
	 */
	public List<SCADA> getScadaListBySecurityId(String security_id) throws Exception {
		Connection connection = null;
		List<SCADA> list = null;
		//
		try {
			//
			StringBuffer query = new StringBuffer();
			query.append("SELECT *          ");
			query.append("  FROM MM_SCADA   ");
			query.append("  WHERE SHARE  = true   ");
			query.append("    AND SHARE_SECURITY_ID  = ?   ");
			query.append(" ORDER BY SCADA_TITLE, NVL(LAST_UPDATE_DATE, INSERT_DATE) DESC ");
			query.append("");

			QueryRunner run = new QueryRunner();
			connection = super.getConnection();

			list = run.query(connection, query.toString(), new BeanListHandler<SCADA>(SCADA.class), new Object[] { security_id });

			//
		} catch (Exception ex) {
			log.error(ex);
		} finally {
			super.closeConnection(connection);
		}
		return list;
	}

	public SCADA getScadaById(String scada_id) throws Exception {
		Connection connection = null;
		SCADA one = null;
		//
		try {
			//
			StringBuffer query = new StringBuffer();
			query.append("SELECT *          ");
			query.append("  FROM  MM_SCADA   ");
			query.append("  WHERE SCADA_ID = ?   ");
			query.append("");

			QueryRunner run = new QueryRunner();
			connection = super.getConnection();
			one = run.query(connection, query.toString(), new BeanHandler<SCADA>(SCADA.class), new Object[] { scada_id });

			// log.info("one=" + one.toString());

			//
		} catch (Exception ex) {
			log.error(ex);
		} finally {
			super.closeConnection(connection);
		}
		return one;

	}

	public void insertSCADA(SCADA scada) throws Exception {
		Connection connection = null;

		//
		try {
			StringBuffer query = new StringBuffer();
			query.append("INSERT INTO MM_SCADA                                ");
			query.append("       (                                            ");
			query.append("         SCADA_ID,                            ");
			query.append("         SCADA_TITLE,                            ");
			query.append("         SCADA_DESC,                                       ");
			query.append("         SVG_CONTENT,                                       ");
			query.append("         TAG_MAPPING_JSON,                                       ");
			query.append("         JAVASCRIPT_LISTENER,                            ");
			query.append("         HTML_LISTENER,                                  ");
			query.append("         SHARE,                                  ");
			query.append("         SHARE_SECURITY_ID,                                  ");
			query.append("         INSERT_USER_ID,                                ");
			query.append("         INSERT_DATE,                                   ");
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

			run.update(connection, query.toString(),
					new Object[] {
							scada.getScada_id(), 
							scada.getScada_title(), 
							scada.getScada_desc(), 
							scada.getSvg_content(), 
							scada.getTag_mapping_json(), 
							scada.getJavascript_listener(),
							scada.getHtml_listener(),
							scada.isShare(), 
							scada.getShare_security_id(),
							scada.getInsert_user_id() 
							});
			//

		} catch (Exception ex) {
			log.error(ex);
		} finally {
			super.closeConnection(connection);
		}

	}

	public void updateSCADA(SCADA scada) throws Exception {
		Connection connection = null;
		try {

			//
			StringBuffer query = new StringBuffer();

			query.append("UPDATE MM_SCADA        ");
			query.append("   SET SCADA_TITLE = ? ,       ");
			query.append("       SCADA_DESC  = ? ,        ");
			query.append("       SVG_CONTENT  = ? ,        ");
			query.append("       TAG_MAPPING_JSON  = ? ,     ");
			query.append("       JAVASCRIPT_LISTENER  = ? ,  ");
			query.append("       HTML_LISTENER  = ? ,        ");
			query.append("       SHARE = ?,                      ");
			query.append("       SHARE_SECURITY_ID = ?,          ");
			query.append("       LAST_UPDATE_DATE = SYSDATE  ");
			query.append(" WHERE SCADA_ID = ?   ");
			//
			QueryRunner run = new QueryRunner();
			connection = super.getConnection();

			run.update(connection, query.toString(), new Object[] { scada.getScada_title(), scada.getScada_desc(),
					scada.getSvg_content(), scada.getTag_mapping_json(), scada.getJavascript_listener(), 
					scada.getHtml_listener(), 
					scada.isShare(),
					scada.getShare_security_id(),
					scada.getScada_id() });

		} catch (Exception ex) {
			log.error(ex);
		} finally {
			super.closeConnection(connection);
		}
	}

	public void deleteSCADA(String scada_id) throws Exception {
		Connection connection = null;
		try {
			connection = super.getConnection();
			connection.setAutoCommit(false);
			//
			StringBuffer query = new StringBuffer();
			query.append("\n");
			query.append("DELETE FROM MM_SCADA  \n");
			query.append(" WHERE SCADA_ID = ?   \n");
			query.append(";\n");
			//
			QueryRunner run = new QueryRunner();

			run.update(connection, query.toString(), new Object[] { scada_id });

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
