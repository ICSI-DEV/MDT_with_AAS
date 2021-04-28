package plantpulse.cep.dao;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.util.List;

import org.apache.commons.lang.StringUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.springframework.stereotype.Repository;

import plantpulse.dbutils.QueryRunner;
import plantpulse.dbutils.handlers.BeanHandler;
import plantpulse.dbutils.handlers.BeanListHandler;
import plantpulse.dbutils.handlers.ScalarHandler;
import plantpulse.domain.OPC;
import plantpulse.domain.Tag;

@Repository
public class OPCDAO extends CommonDAO {

	private static final Log log = LogFactory.getLog(OPCDAO.class);

	public List<OPC> selectOpcs(OPC opc) throws Exception {

		Connection connection = null;
		List<OPC> list = null;
		//
		try {
			//
			StringBuffer query = new StringBuffer();
			query.append("SELECT SITE_ID AS ID,         \n");
			query.append("       S.SITE_NAME AS TEXT,     \n");
			query.append("       '#' AS PARENT,     \n");
			query.append("       0 AS OPC_ORDER,        \n");
			query.append("       'S' AS TYPE,     \n");
			query.append("       SITE_ID        \n");
			query.append("  FROM MM_SITES S    \n");
			query.append(" WHERE SITE_ID = ?    \n");
			query.append(" UNION ALL    \n");
			query.append("SELECT OPC_ID,    \n");
			query.append("       OPC_NAME,    \n");
			query.append("       SITE_ID,    \n");
			query.append("       1,    \n");
			query.append("       'O',    \n");
			query.append("       SITE_ID    \n");
			query.append("  FROM MM_OPCS O    \n");
			query.append(" WHERE O.SITE_ID = ?    \n");
			query.append(" UNION ALL    \n");
			query.append("SELECT TAG_ID,    \n");
			query.append("       TAG_NAME,    \n");
			query.append("       OPC_ID,    \n");
			query.append("       2,    \n");
			query.append("       'P',    \n");
			query.append("       ?    \n");
			query.append("  FROM MM_TAGS T    \n");
			query.append(" WHERE T.OPC_ID IN (SELECT OPC_ID FROM MM_OPCS WHERE SITE_ID = ?)       \n");
			query.append(" ORDER BY 3,4,2   \n");

			//log.debug("OPCDAO.selectOpcs query : {\n" + query.toString() + "}");
			//log.debug("Parameters : [" + opc.toString() + "]");

			QueryRunner run = new QueryRunner();
			connection = super.getConnection();
			list = run.query(connection, query.toString(), new BeanListHandler<OPC>(OPC.class), new Object[] { opc.getSite_id(), opc.getSite_id(), opc.getSite_id(), opc.getSite_id() });

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

	public List<OPC> selectOpcsForAsset(OPC opc) throws Exception {

		Connection connection = null;
		List<OPC> list = null;
		//
		try {
			//
			StringBuffer query = new StringBuffer();
			query.append("SELECT SITE_ID AS ID,         \n");
			query.append("       S.SITE_NAME AS TEXT,     \n");
			query.append("       '#' AS PARENT,     \n");
			query.append("       0 AS OPC_ORDER,        \n");
			query.append("       'S' AS TYPE,     \n");
			query.append("       SITE_ID        \n");
			query.append("  FROM MM_SITES S    \n");
			query.append(" WHERE SITE_ID = ?    \n");
			query.append(" UNION ALL    \n");
			query.append("SELECT OPC_ID,    \n");
			query.append("       OPC_NAME,    \n");
			query.append("       SITE_ID,    \n");
			query.append("       1,    \n");
			query.append("       'O',    \n");
			query.append("       SITE_ID    \n");
			query.append("  FROM MM_OPCS O    \n");
			query.append(" WHERE O.SITE_ID = ?    \n");
			query.append(" UNION ALL    \n");
			query.append("SELECT TAG_ID,    \n");
			query.append("       TAG_NAME,    \n");
			query.append("       OPC_ID,    \n");
			query.append("       2,    \n");
			query.append("       'P',    \n");
			query.append("       ?    \n");
			query.append("  FROM MM_TAGS T    \n");
			query.append(" WHERE T.OPC_ID IN (SELECT OPC_ID FROM MM_OPCS WHERE SITE_ID = ?)       \n");
			query.append("   AND NVL(T.LINKED_ASSET_ID, '') = ''      \n");
			query.append(" ORDER BY 3,4,2   \n");

			//log.debug("OPCDAO.selectOpcs query : {\n" + query.toString() + "}");
			//log.debug("Parameters : [" + opc.toString() + "]");

			QueryRunner run = new QueryRunner();
			connection = super.getConnection();
			list = run.query(connection, query.toString(), new BeanListHandler<OPC>(OPC.class), new Object[] { opc.getSite_id(), opc.getSite_id(), opc.getSite_id(), opc.getSite_id() });

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

	public List<OPC> selectOpcList() throws Exception {

		List<OPC> opcList = null;
		Connection connection = null;
		//
		try {
			//
			StringBuffer query = new StringBuffer();
			query.append("SELECT *   \n");
			query.append("  FROM MM_OPCS  \n");
			query.append("  ORDER BY OPC_ID ASC \n");

			//log.debug("OPCDAO.selectOpcs query : {\n" + query.toString() + "}");

			QueryRunner run = new QueryRunner();
			connection = super.getConnection();
			opcList = run.query(connection, query.toString(), new BeanListHandler<OPC>(OPC.class), new Object[] {});

			log.debug(opcList.toString());
			//
		} catch (Exception ex) {
			log.error(ex);
			throw ex;
		} finally {
			super.closeConnection(connection);
		}
		return opcList;
	}
	
	
	public List<OPC> selectOpcListByType(String opc_type) throws Exception {

		List<OPC> opcList = null;
		Connection connection = null;
		//
		try {
			//
			StringBuffer query = new StringBuffer();
			query.append("SELECT *   \n");
			query.append("  FROM MM_OPCS  \n");
			query.append("  WHERE OPC_TYPE = '" + opc_type + "'  \n");
			query.append("  ORDER BY OPC_ID ASC \n");

			//log.debug("OPCDAO.selectOpcListByType query : {\n" + query.toString() + "}");

			QueryRunner run = new QueryRunner();
			connection = super.getConnection();
			opcList = run.query(connection, query.toString(), new BeanListHandler<OPC>(OPC.class), new Object[] {});

			log.debug(opcList.toString());
			//
		} catch (Exception ex) {
			log.error(ex);
			throw ex;
		} finally {
			super.closeConnection(connection);
		}
		return opcList;
	}
	
	public int selectOpcCount() throws Exception {

		int count = 0;
		Connection connection = null;
		//
		try {
			//
			StringBuffer query = new StringBuffer();
			query.append("SELECT *  \n");
			query.append("  FROM MM_OPCS  \n");

			//log.debug("OPCDAO.selectOpcCount query : {\n" + query.toString() + "}");

			QueryRunner run = new QueryRunner();
			connection = super.getConnection();
			List<OPC> opcList = run.query(connection, query.toString(), new BeanListHandler<OPC>(OPC.class), new Object[] {});
			if(opcList != null && opcList.size() > 0) {
				count = opcList.size();
			}

			log.debug(opcList.toString());
			//
		} catch (Exception ex) {
			log.error(ex);
			throw ex;
		} finally {
			super.closeConnection(connection);
		}
		return count;
	}
	
	

	public List<OPC> selectOpcList(String opc_type) throws Exception {

		List<OPC> opcList = null;
		Connection connection = null;
		//
		try {
			//
			StringBuffer query = new StringBuffer();
			query.append("SELECT A.*,  \n");
			query.append("      (SELECT COUNT(*) FROM MM_TAGS WHERE OPC_ID = A.OPC_ID) AS TAG_COUNT   \n");
			query.append("  FROM MM_OPCS A \n");
			query.append("  WHERE A.OPC_TYPE = '" + opc_type + "'  \n");

			//log.debug("OPCDAO.selectOpcs query : {\n" + query.toString() + "}");

			QueryRunner run = new QueryRunner();
			connection = super.getConnection();
			opcList = run.query(connection, query.toString(), new BeanListHandler<OPC>(OPC.class), new Object[] {});

			log.debug(opcList.toString());
			//
		} catch (Exception ex) {
			log.error(ex);
			throw ex;
		} finally {
			super.closeConnection(connection);
		}
		return opcList;
	}

	public OPC selectOpc(String opc_id) throws Exception {
		Connection connection = null;

		OPC opcInfo = null;
		//
		try {
			//
			StringBuffer query = new StringBuffer();
			query.append("SELECT *    \n");
			query.append("  FROM MM_OPCS    \n");
			query.append(" WHERE OPC_ID = ?    \n");
			//
			//log.debug("OPCDAO.selectOpcInfo query : {\n" + query.toString() + "}");
			//log.debug("Parameters : [" + opc_id + "]");
			//
			QueryRunner run = new QueryRunner();
			connection = super.getConnection();
			opcInfo = run.query(connection, query.toString(), new BeanHandler<OPC>(OPC.class), new Object[] { opc_id });

			//
		} catch (Exception ex) {
			log.error(ex);
			throw ex;
		} finally {
			super.closeConnection(connection);
		}
		//
		return opcInfo;
	};
	
	public OPC selectOpcInfo(OPC opc) throws Exception {
		Connection connection = null;

		OPC opcInfo = null;
		//
		try {
			//
			StringBuffer query = new StringBuffer();
			query.append("SELECT *    \n");
			query.append("  FROM MM_OPCS    \n");
			query.append(" WHERE OPC_ID = ?    \n");
			//
			//log.debug("OPCDAO.selectOpcInfo query : {\n" + query.toString() + "}");
			//log.debug("Parameters : [" + opc.toString() + "]");
			//
			QueryRunner run = new QueryRunner();
			connection = super.getConnection();
			opcInfo = run.query(connection, query.toString(), new BeanHandler<OPC>(OPC.class), new Object[] { opc.getOpc_id() });

			//
		} catch (Exception ex) {
			log.error(ex);
			throw ex;
		} finally {
			super.closeConnection(connection);
		}
		//
		return opcInfo;
	};
	
	public OPC selectOpcInfoByOpcId(String opc_id) throws Exception {
		Connection connection = null;

		OPC opcInfo = null;
		//
		try {
			//
			StringBuffer query = new StringBuffer();
			query.append("SELECT *    \n");
			query.append("  FROM MM_OPCS    \n");
			query.append(" WHERE OPC_ID = ?    \n");
			//
			//log.debug("OPCDAO.selectOpcInfo query : {\n" + query.toString() + "}");
			//log.debug("Parameters : [" + opc_id + "]");
			//
			QueryRunner run = new QueryRunner();
			connection = super.getConnection();
			opcInfo = run.query(connection, query.toString(), new BeanHandler<OPC>(OPC.class), new Object[] { opc_id });

			//
		} catch (Exception ex) {
			log.error(ex);
			throw ex;
		} finally {
			super.closeConnection(connection);
		}
		//
		return opcInfo;
	};

	public boolean hasOPC(String opc_id) throws Exception {

		Connection con = null;
		PreparedStatement pstmt = null;
		ResultSet rs = null;

		boolean aleady_inserted = false;
		//
		try {
			con = super.getConnection();
			//
			StringBuffer query = new StringBuffer();
			query.append("SELECT *    \n");
			query.append("  FROM MM_OPCS    \n");
			query.append(" WHERE OPC_ID = '" + opc_id + "'   \n");
			//
			//
			pstmt = con.prepareStatement(query.toString());
			rs = pstmt.executeQuery();
			//log.debug("[getStatementList query] : " + query.toString());
			//
			if (rs.next()) {
				aleady_inserted = true;
			}
			//
		} catch (Exception ex) {
			log.error(ex);
			throw ex;
		} finally {
			super.closeConnection(rs, pstmt, con);
		}
		//
		return aleady_inserted;
	}

	public boolean duplicateCheckOpc(OPC opc) throws Exception {

		boolean isDuplicated = true;
		Connection connection = null;
		//
		try {
			//
			Object[] param;
			String addQuery = "";

			if (StringUtils.isNotEmpty(opc.getOpc_cls_id()) && StringUtils.isNotEmpty(opc.getOpc_program_id())) {
				addQuery = "         OPC_CLS_ID = ? OR OPC_PROGRAM_ID = ?    \n";
				param = new Object[] { opc.getOpc_cls_id(), opc.getOpc_program_id() };
			} else if (StringUtils.isNotEmpty(opc.getOpc_cls_id()) && StringUtils.isEmpty(opc.getOpc_program_id())) {
				addQuery = "         OPC_CLS_ID = ?    \n";
				param = new Object[] { opc.getOpc_cls_id() };
			} else if (StringUtils.isEmpty(opc.getOpc_cls_id()) && StringUtils.isNotEmpty(opc.getOpc_program_id())) {
				addQuery = "         OPC_PROGRAM_ID = ?    \n";
				param = new Object[] { opc.getOpc_program_id() };
			} else {
				throw new Exception("CLS ID 또는 PROGRAM ID는 필수입력 항목 입니다.");
			}

			StringBuffer query = new StringBuffer();
			query.append("SELECT *    \n");
			query.append("  FROM MM_OPCS    \n");
			query.append(" WHERE 1=1    \n");
			query.append("   AND ( \n");
			query.append(addQuery);
			query.append("       ) \n");

			//log.debug("OPCDAO.duplicateCheckOpc query : {\n" + query.toString() + "}");
			//log.debug("Parameters : [" + opc.toString() + "]");

			QueryRunner run = new QueryRunner();
			connection = super.getConnection();
			List<OPC> list = run.query(connection, query.toString(), new BeanListHandler<OPC>(OPC.class), param);

			isDuplicated = list.size() > 0 ? true : false;

			//log.debug("MM_OPCS COUNT : {" + list.size() + "}");
			//
		} catch (Exception ex) {
			log.error(ex);
			throw ex;
		} finally {
			super.closeConnection(connection);
		}
		return isDuplicated;
	}

	public String insertOpcAndTags(OPC opc, List<Tag> tagList) throws Exception {
		//
		String opc_id = "";
		Connection connection = null;
		//
		try {
			connection = super.getConnection();
			connection.setAutoCommit(false);

			// Get MAX OPC_ID
			StringBuffer query = new StringBuffer();
			QueryRunner run = new QueryRunner();
			query.append("SELECT 'OPC_' || LPAD(NEXT VALUE FOR OPC_SEQ, 5, '0') AS OPC_ID FROM DUAL   \n");
			opc_id = run.query(connection, query.toString(), new ScalarHandler<String>(), new Object[] {});
			opc.setOpc_id(opc_id);

			//
			query = new StringBuffer();
			query.append("INSERT INTO MM_OPCS    \n");
			query.append("       (    \n");
			query.append("         OPC_ID,    \n");
			query.append("         OPC_NAME,    \n");
			query.append("         OPC_TYPE,    \n");
			query.append("         OPC_SUB_TYPE,    \n");
			query.append("         OPC_SERVER_IP,    \n");
			query.append("         OPC_DOMAIN_NAME,    \n");
			query.append("         OPC_LOGIN_ID,    \n");
			query.append("         OPC_PASSWORD,    \n");
			query.append("         OPC_PROGRAM_ID,    \n");
			query.append("         OPC_CLS_ID,    \n");
			query.append("         DESCRIPTION,    \n");
			query.append("         INSERT_DATE,    \n");
			query.append("         SITE_ID,    \n");
			query.append("         OPC_AGENT_IP,    \n");
			query.append("         OPC_AGENT_PORT    \n");
			query.append("       )    \n");
			query.append("VALUES    \n");
			query.append("       (    \n");
			query.append("         ?,    \n");
			query.append("         ?,    \n");
			query.append("         'OPC',    \n");
			query.append("         ?,    \n");
			query.append("         ?,    \n");
			query.append("         ?,    \n");
			query.append("         ?,    \n");
			query.append("         ?,    \n");
			query.append("         ?,    \n");
			query.append("         ?,    \n");
			query.append("         ?,    \n");
			query.append("         SYSDATE,    \n");
			query.append("         ?,    \n");
			query.append("         ?,    \n");
			query.append("         ?    \n");
			query.append("       )    \n");

			//log.debug("query.toString() : {\n" + query.toString() + "}");
			//log.debug("Parameters : [" + opc.toString() + "]");
			//
			run = new QueryRunner();
			run.update(connection, query.toString(), new Object[] { opc.getOpc_id(), StringUtils.upperCase(opc.getOpc_name()), opc.getOpc_sub_type(), opc.getOpc_server_ip(), opc.getOpc_domain_name(), opc.getOpc_login_id(),
					opc.getOpc_password(), opc.getOpc_program_id(), opc.getOpc_cls_id(), opc.getDescription(), opc.getSite_id(), opc.getOpc_agent_ip(), opc.getOpc_agent_port() });

			// Loop Insert Point
			for (Tag tag : tagList) {
				//
				query = new StringBuffer();
				query.append("INSERT INTO MM_TAGS    \n");
				query.append("       (    \n");
				query.append("         TAG_ID,    \n");
				query.append("         TAG_NAME,    \n");
				query.append("         TAG_SOURCE,    \n");
				query.append("         GROUP_NAME,    \n");
				query.append("         OPC_ID,    \n");
				query.append("         INSERT_DATE,    \n");
				query.append("         JAVA_TYPE,    \n");
				query.append("         UNIT,    \n");
				query.append("         DESCRIPTION,    \n");
				query.append("         INTERVAL    \n");
				query.append("       )    \n");
				query.append("VALUES    \n");
				query.append("       (    \n");
				query.append("         'TAG_' || LPAD(NEXT VALUE FOR TAG_SEQ, 6, '0'),    \n");
				query.append("         ?,    \n");
				query.append("         'OPC',    \n");
				query.append("         ?,    \n");
				query.append("         ?,    \n");
				query.append("         SYSDATE,    \n");
				query.append("         ?,    \n");
				query.append("         ?,    \n");
				query.append("         ?,    \n");
				query.append("         1000    \n");
				query.append("       )    \n");

				//log.debug("query.toString() : {\n" + query.toString() + "}");
				//log.debug("Parameters : [" + tag.toString() + "]");

				run = new QueryRunner();
				run.update(connection, query.toString(), new Object[] { 
						tag.getTag_name(), 
						tag.getGroup_name(),
						opc.getOpc_id(),
						tag.getJava_type(), 
						tag.getUnit(), 
						tag.getDescription() });
			}

			connection.commit();
			//
			
			//
			super.getDomainChangeEventBus().onOPCChanged(opc.getOpc_id());
			
		} catch (Exception ex) {
			connection.rollback();
			log.error(ex);
			throw ex;
		} finally {
			connection.setAutoCommit(true);
			super.closeConnection(connection);
		}

		return opc_id;
	}

	public void updateOpcAndTags(OPC opc, List<Tag> tagList) throws Exception {
		//
		Connection connection = null;
		//
		try {
			connection = super.getConnection();
			connection.setAutoCommit(false);

			// update opc
			StringBuffer query = new StringBuffer();
			query.append("UPDATE MM_OPCS    \n");
			query.append("   SET OPC_NAME = ?,    \n");
			query.append("       OPC_SERVER_IP = ?,    \n");
			query.append("       OPC_DOMAIN_NAME = ?,    \n");
			query.append("       OPC_LOGIN_ID = ?,    \n");
			query.append("       OPC_PASSWORD = ?,    \n");
			query.append("       OPC_PROGRAM_ID = ?,    \n");
			query.append("       OPC_CLS_ID = ?,    \n");
			query.append("       DESCRIPTION = ?,    \n");
			query.append("       UPDATE_DATE = SYSDATE,    \n");
			query.append("       OPC_AGENT_IP = ?,    \n");
			query.append("       OPC_AGENT_PORT = ?,    \n");
			query.append("       SITE_ID = ?    \n");
			query.append(" WHERE OPC_ID = ?    \n");

			//log.debug("query.toString() : {\n" + query.toString() + "}");
			//log.debug("Parameters : [" + opc.toString() + "]");
			//
			QueryRunner run = new QueryRunner();
			run.update(connection, query.toString(), new Object[] { StringUtils.upperCase(opc.getOpc_name()), opc.getOpc_server_ip(), opc.getOpc_domain_name(), opc.getOpc_login_id(),
					opc.getOpc_password(), opc.getOpc_program_id(), opc.getOpc_cls_id(), opc.getDescription(), opc.getOpc_agent_ip(), opc.getOpc_agent_port(), opc.getSite_id(), opc.getOpc_id() });

			// Loop Insert Point
			for (Tag tag : tagList) {
				//
				query = new StringBuffer();
				query.append("INSERT INTO MM_TAGS    \n");
				query.append("       (    \n");
				query.append("         TAG_ID,    \n");
				query.append("         TAG_NAME,    \n");
				query.append("         TAG_SOURCE,    \n");
				query.append("         GROUP_NAME,    \n");
				query.append("         OPC_ID,    \n");
				query.append("         INSERT_DATE,    \n");
				query.append("         JAVA_TYPE,    \n");
				query.append("         UNIT,    \n");
				query.append("         DESCRIPTION,    \n");
				query.append("         INTERVAL    \n");
				query.append("       )    \n");
				query.append("VALUES    \n");
				query.append("       (    \n");
				query.append("         'TAG_' || LPAD(NEXT VALUE FOR TAG_SEQ, 6, '0'),    \n");
				query.append("         ?,    \n");
				query.append("         'OPC',    \n");
				query.append("         ?,    \n");
				query.append("         ?,    \n");
				query.append("         SYSDATE,    \n");
				query.append("         ?,    \n");
				query.append("         ?,    \n");
				query.append("         ?,    \n");
				query.append("         1000    \n");
				query.append("       )    \n");

				log.debug("query.toString() : {\n" + query.toString() + "}");
				log.debug("Parameters : [" + tag.toString() + "]");

				run = new QueryRunner();
				run.update(connection, query.toString(), new Object[] {
						tag.getTag_name(), 
						tag.getGroup_name(),
						opc.getOpc_id(), 
						tag.getJava_type(), 
						tag.getUnit(), 
						tag.getDescription() 
						});
			}

			connection.commit();
			//
			
			//
			super.getDomainChangeEventBus().onOPCChanged(opc.getOpc_id());
			
		} catch (Exception ex) {
			connection.rollback();
			log.error(ex);
			throw ex;
		} finally {
			connection.setAutoCommit(true);
			super.closeConnection(connection);
		}
	}

	public void deleteOpcAndTags(OPC opc) throws Exception {
		//
		Connection connection = null;
		//
		try {
			connection = super.getConnection();
			connection.setAutoCommit(false);

			// update opc
			StringBuffer query = new StringBuffer();
			query.append("DELETE FROM  MM_OPCS    \n");
			query.append(" WHERE SITE_ID = ?    \n");
			query.append("   AND OPC_ID = ?    \n");

			//log.debug("query.toString() : {\n" + query.toString() + "}");
			//log.debug("Parameters : [" + opc.toString() + "]");
			//
			QueryRunner run = new QueryRunner();
			run.update(connection, query.toString(), new Object[] { opc.getSite_id(), opc.getOpc_id() });

			// delete tag
			query = new StringBuffer();
			query.append("DELETE FROM MM_TAGS    \n");
			query.append(" WHERE OPC_ID = ?    \n");

			//log.debug("query.toString() : {\n" + query.toString() + "}");
			//log.debug("Parameters : [" + opc.getOpc_id() + "]");

			run = new QueryRunner();
			run.update(connection, query.toString(), new Object[] { opc.getOpc_id() });

			connection.commit();
			//
			//
			super.getDomainChangeEventBus().onOPCChanged(opc.getOpc_id());
			
		} catch (Exception ex) {
			connection.rollback();
			log.error(ex);
			throw ex;
		} finally {
			connection.setAutoCommit(true);
			super.closeConnection(connection);
		}
	};

	/**
	 * 가상 OPC 서버 등록
	 * 
	 * <pre>
	 * AGENT_IP가 없으면, 가상 OPC로 등록한다.
	 * </pre>
	 * 
	 * @param opc
	 * @return
	 * @throws Exception
	 */
	public String insertVituralOpc(OPC opc) throws Exception {
		//
		String opc_id = "";
		Connection connection = null;
		//
		try {
			connection = super.getConnection();
			connection.setAutoCommit(false);

			//
			StringBuffer query = new StringBuffer();
			QueryRunner run = new QueryRunner();

			query = new StringBuffer();
			query.append("INSERT INTO MM_OPCS    \n");
			query.append("       (    \n");
			query.append("         OPC_ID,    \n");
			query.append("         OPC_NAME,    \n");
			query.append("         OPC_TYPE,    \n");
			query.append("         OPC_SUB_TYPE,    \n");
			query.append("         OPC_SERVER_IP,    \n");
			query.append("         OPC_DOMAIN_NAME,    \n");
			query.append("         OPC_LOGIN_ID,    \n");
			query.append("         OPC_PASSWORD,    \n");
			query.append("         OPC_PROGRAM_ID,    \n");
			query.append("         OPC_CLS_ID,    \n");
			query.append("         DESCRIPTION,    \n");
			query.append("         INSERT_DATE,    \n");
			query.append("         SITE_ID,    \n");
			query.append("         OPC_AGENT_IP,    \n");
			query.append("         OPC_AGENT_PORT    \n");
			query.append("       )    \n");
			query.append("VALUES    \n");
			query.append("       (    \n");
			query.append("         ?,    \n");
			query.append("         ?,    \n");
			query.append("         ?,    \n");
			query.append("         ?,    \n");
			query.append("         ?,    \n");
			query.append("         ?,    \n");
			query.append("         ?,    \n");
			query.append("         ?,    \n");
			query.append("         ?,    \n");
			query.append("         ?,    \n");
			query.append("         ?,    \n");
			query.append("         SYSDATE,    \n");
			query.append("         ?,   \n");
			query.append("         ?,   \n");
			query.append("         ?    \n");
			query.append("       )    \n");

			//log.debug("query.toString() : {\n" + query.toString() + "}");
			//log.debug("Parameters : [" + opc.toString() + "]");
			//
			run = new QueryRunner();
			run.update(connection, query.toString(), new Object[] { opc.getOpc_id(), StringUtils.upperCase(opc.getOpc_name()), opc.getOpc_type(), opc.getOpc_sub_type(), opc.getOpc_server_ip(), opc.getOpc_domain_name(),
					opc.getOpc_login_id(), opc.getOpc_password(), opc.getOpc_program_id(), opc.getOpc_cls_id(), opc.getDescription(), opc.getSite_id(), opc.getOpc_agent_ip(), opc.getOpc_agent_port() });

			connection.commit();
			//
			
			//
			super.getDomainChangeEventBus().onOPCChanged(opc.getOpc_id());
			
		} catch (Exception ex) {
			connection.rollback();
			log.error(ex);
			throw ex;
		} finally {
			connection.setAutoCommit(true);
			super.closeConnection(connection);
		}

		return opc_id;
	}

	public String updateVituralOpc(OPC opc) throws Exception {
		//
		String opc_id = "";
		Connection connection = null;
		//
		try {
			connection = super.getConnection();
			connection.setAutoCommit(false);

			//
			StringBuffer query = new StringBuffer();
			QueryRunner run = new QueryRunner();

			// update opc
			query.append("UPDATE MM_OPCS    \n");
			query.append("   SET OPC_NAME = ?,    \n");
			query.append("       OPC_TYPE = ?,    \n");
			query.append("       OPC_SUB_TYPE = ?,    \n");
			query.append("       OPC_SERVER_IP = ?,    \n");
			query.append("       OPC_DOMAIN_NAME = ?,    \n");
			query.append("       OPC_LOGIN_ID = ?,    \n");
			query.append("       OPC_PASSWORD = ?,    \n");
			query.append("       OPC_PROGRAM_ID = ?,    \n");
			query.append("       OPC_CLS_ID = ?,    \n");
			query.append("       DESCRIPTION = ?,    \n");
			query.append("       UPDATE_DATE = SYSDATE,    \n");
			query.append("       OPC_AGENT_IP = ?,    \n");
			query.append("       OPC_AGENT_PORT = ?    \n");
			query.append(" WHERE SITE_ID = ?    \n");
			query.append("   AND OPC_ID = ?    \n");

			//log.debug("query.toString() : {\n" + query.toString() + "}");
			//log.debug("Parameters : [" + opc.toString() + "]");
			//
			run.update(connection, query.toString(),
					new Object[] { StringUtils.upperCase(opc.getOpc_name()), opc.getOpc_type(), opc.getOpc_sub_type(), opc.getOpc_server_ip(), opc.getOpc_domain_name(), opc.getOpc_login_id(), opc.getOpc_password(),
							opc.getOpc_program_id(), opc.getOpc_cls_id(), opc.getDescription(), opc.getOpc_agent_ip(), opc.getOpc_agent_port(), opc.getSite_id(), opc.getOpc_id() });

			connection.commit();
			//
			
			//
			super.getDomainChangeEventBus().onOPCChanged(opc.getOpc_id());
			
		} catch (Exception ex) {
			connection.rollback();
			log.error(ex);
			throw ex;
		} finally {
			connection.setAutoCommit(true);
			super.closeConnection(connection);
		}

		return opc_id;
	}

	public String deleteVituralOpc(OPC opc) throws Exception {
		//
		String opc_id = "";
		Connection connection = null;

		//
		try {
			connection = super.getConnection();
			connection.setAutoCommit(false);

			//
			StringBuffer query = new StringBuffer();
			query.append("DELETE FROM MM_OPCS WHERE OPC_ID =  ?  \n");

			//log.debug("query.toString() : {\n" + query.toString() + "}");
			//log.debug("Parameters : [" + opc.toString() + "]");

			QueryRunner run = new QueryRunner();
			run.update(connection, query.toString(), new Object[] { opc.getOpc_id() });

			connection.commit();
			//
			
			//
			super.getDomainChangeEventBus().onOPCChanged(opc.getOpc_id());
			
		} catch (Exception ex) {
			connection.rollback();
			log.error(ex);
			throw ex;
		} finally {
			connection.setAutoCommit(true);
			super.closeConnection(connection);
		}

		return opc_id;
	}

}
