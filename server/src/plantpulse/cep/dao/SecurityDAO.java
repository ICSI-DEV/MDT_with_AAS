package plantpulse.cep.dao;

import java.sql.Connection;
import java.util.List;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import plantpulse.dbutils.QueryRunner;
import plantpulse.dbutils.handlers.BeanHandler;
import plantpulse.dbutils.handlers.BeanListHandler;
import plantpulse.dbutils.handlers.ScalarHandler;
import plantpulse.domain.Security;
import plantpulse.domain.User;

public class SecurityDAO extends CommonDAO {

	private static final Log log = LogFactory.getLog(SecurityDAO.class);

	public List<Security> getSecurityList() throws Exception {
		Connection connection = null;
		List<Security> list = null;
		//
		try {
			//
			StringBuffer query = new StringBuffer();
			query.append("SELECT  * ");
			query.append("  FROM MM_SECURITY                       ");
			query.append(" ORDER BY NVL(LAST_UPDATE_DATE, INSERT_DATE) DESC ");

			QueryRunner run = new QueryRunner();
			connection = super.getConnection();
			list = (List<Security>) run.query(connection, query.toString(), new BeanListHandler(Security.class), new Object[] {});

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
	
	/**
	 * 태그ID의 보안 그룹에 해당하는 사용자 목록을 반환한다.
	 * 
	 * @param tag_id
	 * @return
	 * @throws Exception
	 */
	public String[] getUserListBySecurityOfTagId(String tag_id) throws Exception {
		Connection connection = null;
		String[] array = null;
		//
		try {
			//
			StringBuffer query = new StringBuffer();
			query.append(" SELECT ");
			query.append(" B.* ");
			query.append(" FROM MM_SECURITY A, USER_LOGIN B ");
			query.append(" WHERE A.SECURITY_ID = B.SECURITY_ID ");
			query.append("   AND OBJECT_PERMISSION_ARRAY LIKE '%" + tag_id + "%' ");

			QueryRunner run = new QueryRunner();
			connection = super.getConnection();
			List<User> list = (List<User>) run.query(connection, query.toString(), new BeanListHandler(User.class));
			
			if(list != null && list.size() > 0){
				array = new String[list.size()];
				for(int i=0; i < list.size(); i++){
					array[i] = list.get(i).getUser_id();
				}
			}else{
				array = new String[0];
			}

			//log.error(list.toString());

			//
		} catch (Exception ex) {
			log.error(ex);
			throw ex;
		} finally {
			super.closeConnection(connection);
		}
		return array;
	}
	
	


	public Security getSecurity(String security_id) throws Exception {
		Connection connection = null;
		Security one = null;
		//
		try {

			StringBuffer query = new StringBuffer();
			query.append("SELECT  * ");
			query.append("  FROM MM_SECURITY                       ");
			query.append(" WHERE SECURITY_ID  = ?       \n");

			QueryRunner run = new QueryRunner();
			connection = super.getConnection();
			one = (Security) run.query(connection, query.toString(), new BeanHandler(Security.class), new Object[] { security_id });

			//
		} catch (Exception ex) {
			log.error(ex);
			throw ex;
		} finally {
			super.closeConnection(connection);
		}
		return one;
	}

	public void saveSecurity(Security security) throws Exception {
		Connection connection = null;
		//
		try {

			connection = super.getConnection();
			connection.setAutoCommit(false);
			

			//
			StringBuffer nquery = new StringBuffer();
			QueryRunner nrun = new QueryRunner();
			nquery.append("SELECT 'SECURITY_' || LPAD(NEXT VALUE FOR SECURITY_SEQ, 5, '0') AS SECURITY_ID FROM DUAL   \n");
			String security_id = nrun.query(connection, nquery.toString(), new ScalarHandler<String>(), new Object[] {});
			security.setSecurity_id(security_id);
			

			QueryRunner run = new QueryRunner();

			// 1. 마스터 저장
			StringBuffer query = new StringBuffer();
			query.append("\n");
			query.append("INSERT INTO MM_SECURITY                            \n");
			query.append("       (                                           \n");
			query.append("         SECURITY_ID,                             \n");
			query.append("         SECURITY_NAME,                            \n");
			query.append("         SECURITY_DESC,                            \n");
			query.append("         OBJECT_PERMISSION_ARRAY,                   \n");
			query.append("         INSERT_USER_ID,                            \n");
			query.append("         INSERT_DATE                                \n");
			query.append("       ) values (                                   \n");
			query.append("	       ?,    \n");
			query.append("         ?,        \n");
			query.append("         ?,        \n");
			query.append("         ?,        \n");
			query.append("         ?,        \n");
			query.append("         SYSDATE  \n");
			query.append("         )  \n");
			query.append(";\n");

			run.update(connection, query.toString(), new Object[] { security.getSecurity_id(), security.getSecurity_name(), security.getSecurity_desc(), security.getObject_permission_array(), security.getInsert_user_id() });

			connection.commit();
			//
			
			//
			super.getDomainChangeEventBus().onSecurityChanged(security.getSecurity_id());
			
		} catch (Exception ex) {
			connection.rollback();
			log.error(ex);
			throw ex;
		} finally {
			connection.setAutoCommit(true);
			super.closeConnection(connection);
		}
	}

	public void updateSecurity(Security security) throws Exception {
		Connection connection = null;
		try {
			connection = super.getConnection();
			connection.setAutoCommit(false);

			//
			StringBuffer query = new StringBuffer();
			query.append("UPDATE MM_SECURITY                  \n");
			query.append("   SET   SECURITY_NAME = ? ,                 \n");
			query.append("         SECURITY_DESC = ? ,                            \n");
			query.append("         OBJECT_PERMISSION_ARRAY = ? ,                         \n");
			query.append("        LAST_UPDATE_DATE = SYSDATE        \n");
			query.append(" WHERE SECURITY_ID = ?   \n");
			//
			QueryRunner run = new QueryRunner();
			run.update(connection, query.toString(), new Object[] { security.getSecurity_name(), security.getSecurity_desc(), security.getObject_permission_array(), security.getSecurity_id() });

			connection.commit();
			//
			
			//
			super.getDomainChangeEventBus().onSecurityChanged(security.getSecurity_id());
			
			
		} catch (Exception ex) {
			connection.rollback();
			log.error(ex);
			throw ex;
		} finally {
			connection.setAutoCommit(true);
			super.closeConnection(connection);
		}
	}

	public void deleteSecurity(String security_id) throws Exception {
		Connection connection = null;
		try {
			connection = super.getConnection();
			connection.setAutoCommit(false);

			//
			StringBuffer query = new StringBuffer();
			query.append("DELETE FROM MM_SECURITY WHERE SECURITY_ID = ? \n");
			//
			QueryRunner run = new QueryRunner();
			run.update(connection, query.toString(), new Object[] { security_id });

			connection.commit();
			//
			
			//
			super.getDomainChangeEventBus().onSecurityChanged(security_id);
			
			
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
