package plantpulse.cep.dao;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import plantpulse.cep.db.ConnectionUtils;
import plantpulse.dbutils.QueryRunner;
import plantpulse.dbutils.handlers.BeanHandler;
import plantpulse.dbutils.handlers.BeanListHandler;
import plantpulse.dbutils.handlers.JSONArrayHandler;
import plantpulse.dbutils.handlers.ScalarHandler;
import plantpulse.domain.User;
import plantpulse.json.JSONArray;
import plantpulse.json.JSONObject;

/**
 * UserDAO
 * 
 * @author lenovo
 *
 */
public class UserDAO extends CommonDAO {

	private static final Log log = LogFactory.getLog(UserDAO.class);

	/**
	 * checkLogin
	 * 
	 * @param userId
	 * @param password
	 * @return
	 */
	public boolean checkLogin(String userId, String password) {
		//
		ConnectionUtils utils = new ConnectionUtils();
		Connection con = null;
		PreparedStatement pstmt = null;
		ResultSet rs = null;

		//
		boolean isMember = false;
		try {
			//
			con = utils.getConnection();
			StringBuffer query = new StringBuffer();
			query.append("\n");
			query.append("SELECT *                              \n");
			query.append("  FROM USER_LOGIN                     \n");
			query.append(" WHERE USER_ID = '" + userId + "'         \n");
			query.append("   AND PASSWORD = '" + password + "'      \n");
			query.append(";\n");
			//
			pstmt = con.prepareStatement(query.toString());
			rs = pstmt.executeQuery();
			log.debug("[getStatementList query] : " + query.toString());
			//
			if (rs.next()) {
				isMember = true;
			}
		} catch (Exception ex) {
			log.error("[UserDAO.checkLogin] " + ex.getMessage(), ex);
		} finally {
			utils.closeConnection(rs, pstmt, con);
		}
		return isMember;
	}

	//
	// EVENT
	public List<User> getUserList() throws Exception {
		Connection connection = null;
		List<User> list = null;
		//
		try {
			//
			StringBuffer query = new StringBuffer();
			query.append("SELECT  * ");
			query.append("  FROM USER_LOGIN                       ");
			query.append(" ORDER BY NVL(LAST_UPDATE_DATE, INSERT_DATE) DESC ");

			QueryRunner run = new QueryRunner();
			connection = super.getConnection();
			list = (List<User>) run.query(connection, query.toString(), new BeanListHandler(User.class), new Object[] {});

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

	public User getUser(String user_id) throws Exception {
		Connection connection = null;
		User one = null;
		//
		try {

			StringBuffer query = new StringBuffer();
			query.append("SELECT  * ");
			query.append("  FROM USER_LOGIN                       ");
			query.append(" WHERE USER_ID  = ?       \n");

			QueryRunner run = new QueryRunner();
			connection = super.getConnection();
			one = (User) run.query(connection, query.toString(), new BeanHandler(User.class), new Object[] { user_id });

			//
		} catch (Exception ex) {
			log.error(ex);
			throw ex;
		} finally {
			super.closeConnection(connection);
		}
		return one;
	}

	public List<User> getTagAlarmRecipient() throws Exception {
		Connection connection = null;
		List<User> userList = null;
		//
		try {

			StringBuffer query = new StringBuffer();
			query.append("SELECT *     ");
			query.append("  FROM USER_LOGIN     ");
			query.append(" WHERE ROLE = 'RECIPIENT'    ");

			QueryRunner run = new QueryRunner();
			connection = super.getConnection();
			userList = run.query(connection, query.toString(), new BeanListHandler<User>(User.class), new Object[] {});
			//
		} catch (Exception ex) {
			log.error(ex);
			throw ex;
		} finally {
			super.closeConnection(connection);
		}
		return userList;
	}

	public void saveUser(User user) throws Exception {
		Connection connection = null;
		//
		try {

			connection = super.getConnection();
			connection.setAutoCommit(false);

			QueryRunner run = new QueryRunner();

			// 1. 마스터 저장
			StringBuffer query = new StringBuffer();
			query.append("\n");
			query.append("INSERT INTO USER_LOGIN                   \n");
			query.append("       (                                            \n");
			query.append("         USER_ID,                            \n");
			query.append("         PASSWORD,                            \n");
			query.append("         ROLE,                            \n");
			query.append("         SECURITY_ID,                            \n");
			query.append("         NAME,                            \n");
			query.append("         ADDRESS,                                         \n");
			query.append("         EMAIL,                                 \n");
			query.append("         PHONE,                                 \n");
			query.append("         INSERT_USER_ID,                            \n");
			query.append("         INSERT_DATE,                                \n");
			query.append("         ATTR_01,                                \n");
			query.append("         ATTR_02,                                \n");
			query.append("         ATTR_03,                                \n");
			query.append("         ATTR_04,                                \n");
			query.append("         ATTR_05,                                \n");
			query.append("         ATTR_06,                                \n");
			query.append("         ATTR_07,                                \n");
			query.append("         ATTR_08,                                \n");
			query.append("         ATTR_09,                                \n");
			query.append("         ATTR_10                                 \n");
			query.append("       ) values (                                   \n");
			query.append("         ?,        \n");
			query.append("         ?,        \n");
			query.append("         ?,        \n");
			query.append("         ?,        \n");
			query.append("         ?,        \n");
			query.append("         ?,        \n");
			query.append("         ?,        \n");
			query.append("         ?,        \n");
			query.append("         ?,        \n");
			query.append("         SYSDATE,  \n");
			query.append("         ?,        \n");
			query.append("         ?,        \n");
			query.append("         ?,        \n");
			query.append("         ?,        \n");
			query.append("         ?,        \n");
			query.append("         ?,        \n");
			query.append("         ?,        \n");
			query.append("         ?,        \n");
			query.append("         ?,        \n");
			query.append("         ?         \n");
			query.append("       )           \n");
			query.append(";\n");

			run.update(connection, query.toString(),
					new Object[] { user.getUser_id(), user.getPassword(), user.getRole(), user.getSecurity_id(), user.getName(), user.getAddress(), user.getEmail(), user.getPhone(),
							user.getInsert_user_id(), user.getAttr_01(), user.getAttr_02(), user.getAttr_03(), user.getAttr_04(), user.getAttr_05(), user.getAttr_06(), user.getAttr_07(),
							user.getAttr_08(), user.getAttr_09(), user.getAttr_10() });

			connection.commit();
			//
			
			//
			super.getDomainChangeEventBus().onUserChanged(user.getUser_id());
			
		} catch (Exception ex) {
			connection.rollback();
			log.error(ex);
			throw ex;
		} finally {
			connection.setAutoCommit(true);
			super.closeConnection(connection);
		}
	}

	public void updateUser(User user) throws Exception {
		Connection connection = null;
		try {
			connection = super.getConnection();
			connection.setAutoCommit(false);

			//
			StringBuffer query = new StringBuffer();
			query.append("UPDATE USER_LOGIN                  \n");
			query.append("   SET PASSWORD = ? ,                 \n");
			query.append("         ROLE = ? ,                            \n");
			query.append("         SECURITY_ID = ? ,                            \n");
			query.append("         NAME = ? ,                            \n");
			query.append("         ADDRESS = ? ,                                         \n");
			query.append("         EMAIL = ? ,                                 \n");
			query.append("         PHONE = ? ,                                 \n");
			query.append("         ATTR_01 = ? ,                                \n");
			query.append("         ATTR_02 = ? ,                                \n");
			query.append("         ATTR_03 = ? ,                                \n");
			query.append("         ATTR_04 = ? ,                                \n");
			query.append("         ATTR_05 = ? ,                                \n");
			query.append("         ATTR_06 = ? ,                                \n");
			query.append("         ATTR_07 = ? ,                                \n");
			query.append("         ATTR_08 = ? ,                                \n");
			query.append("         ATTR_09 = ? ,                                \n");
			query.append("         ATTR_10 = ? ,                                 \n");
			query.append("        LAST_UPDATE_DATE = SYSDATE        \n");
			query.append(" WHERE USER_ID = ?   \n");
			//
			QueryRunner run = new QueryRunner();
			run.update(connection, query.toString(),
					new Object[] { user.getPassword(), user.getRole(), user.getSecurity_id(), user.getName(), user.getAddress(), user.getEmail(), user.getPhone(), user.getAttr_01(), user.getAttr_02(),
							user.getAttr_03(), user.getAttr_04(), user.getAttr_05(), user.getAttr_06(), user.getAttr_07(), user.getAttr_08(), user.getAttr_09(), user.getAttr_10(),
							user.getUser_id() });

			connection.commit();
			//
			
			//
			super.getDomainChangeEventBus().onUserChanged(user.getUser_id());
			
		} catch (Exception ex) {
			connection.rollback();
			log.error(ex);
			throw ex;
		} finally {
			connection.setAutoCommit(true);
			super.closeConnection(connection);
		}
	}

	public void deleteUser(String user_id) throws Exception {
		Connection connection = null;
		try {
			connection = super.getConnection();
			connection.setAutoCommit(false);

			//
			StringBuffer query = new StringBuffer();
			query.append("DELETE FROM USER_LOGIN WHERE USER_ID = ? \n");
			//
			QueryRunner run = new QueryRunner();
			run.update(connection, query.toString(), new Object[] { user_id });

			connection.commit();
			//
			
			//
			super.getDomainChangeEventBus().onUserChanged(user_id);
			
			
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
	 * 세션 프로퍼티를 업데이트한다.
	 * @param login_id
	 * @param session_key
	 * @param session_value
	 * @throws Exception
	 */
	public void updateSessionProperty(String login_id, String session_key, String session_value) throws Exception {
		Connection connection = null;
		try {
			connection = super.getConnection();
			connection.setAutoCommit(false);

			//

			QueryRunner run = new QueryRunner();
			StringBuffer query = new StringBuffer();
			query.append("DELETE FROM user_login_session WHERE login_id = ? and session_key = ? ");
			run.update(connection, query.toString(), new Object[] { login_id, session_key });

			query = new StringBuffer();
			query.append("INSERT INTO user_login_session (login_id, session_key, session_value, update_date) VALUES (?, ?, ?, NOW()) ");
			run.update(connection, query.toString(), new Object[] { login_id, session_key,  session_value });
			
			connection.commit();
			//
		} catch (Exception ex) {
			connection.rollback();
			log.error(ex, ex);
			throw ex;
		} finally {
			connection.setAutoCommit(true);
			super.closeConnection(connection);
		}
	};
	
	
	/**
	 * 사용자 세션 설정값을 반환한다.
	 * @param login_id
	 * @return
	 * @throws Exception
	 */
	public String getSessionProperty(String login_id, String session_key) throws Exception {
		String value = "";
		Connection connection = null;
		try {
			connection = super.getConnection();
			
			//
			StringBuffer query = new StringBuffer();
			query.append("SELECT session_value FROM user_login_session WHERE login_id = ? AND session_key = ? ");
			QueryRunner run = new QueryRunner();			
			value = run.query(connection, query.toString(), new ScalarHandler<String>(), new Object[] { login_id, session_key });
			return value;
		} catch (Exception ex) {
			log.error(ex, ex);
			throw ex;
		} finally {
			super.closeConnection(connection);
		}
	};
	
	/**
	 * 사용자 세션 설정 맵을 반환한다.
	 * @param login_id
	 * @return
	 * @throws Exception
	 */
	public Map<String,String> getSessionPropertyMap(String login_id) throws Exception {
		Map<String,String> map = new HashMap<String,String>();
		Connection connection = null;
		try {
			connection = super.getConnection();
			
			//
			StringBuffer query = new StringBuffer();
			query.append("SELECT * FROM user_login_session WHERE login_id = ? order by session_key asc ");
			QueryRunner run = new QueryRunner();
			JSONArray array = run.query(connection, query.toString(), new JSONArrayHandler(), new Object[] {login_id});
			for(int i=0; array != null && i < array.size(); i++) {
				JSONObject row = array.getJSONObject(i);
				map.put(row.getString("session_key"), row.getString("session_value"));
			}
			return map;
		} catch (Exception ex) {
			log.error(ex, ex);
			throw ex;
		} finally {
			super.closeConnection(connection);
		}
	};
	
	

}
