package plantpulse.cep.dao;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.util.Date;
import java.util.List;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import plantpulse.cep.db.ConnectionUtils;
import plantpulse.cep.domain.Token;
import plantpulse.dbutils.QueryRunner;
import plantpulse.dbutils.handlers.BeanHandler;
import plantpulse.dbutils.handlers.BeanListHandler;

public class TokenDAO extends CommonDAO {

	private static final Log log = LogFactory.getLog(TokenDAO.class);

	/**
	 * checkLogin
	 * 
	 * @param tokenId
	 * @param password
	 * @return
	 */
	public boolean checkToken(String ip, String token) {
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
			query.append("  FROM MM_TOKEN                     \n");
			query.append(" WHERE IP = '" + ip + "'         \n");
			query.append("   AND TOKEN = '" + token + "'      \n");
			query.append(";\n");
			//
			pstmt = con.prepareStatement(query.toString());
			rs = pstmt.executeQuery();
			//
			if (rs.next()) {
				isMember = true;
			}
		} catch (Exception ex) {
			log.error("[TokenDAO.checkToken] " + ex.getMessage(), ex);
		} finally {
			utils.closeConnection(rs, pstmt, con);
		}
		return isMember;
	}

	//
	// EVENT
	public List<Token> getTokenList() throws Exception {
		Connection connection = null;
		List<Token> list = null;
		//
		try {
			//
			StringBuffer query = new StringBuffer();
			query.append("SELECT  * ");
			query.append("  FROM MM_TOKEN                       ");
			query.append(" ORDER BY IP ASC, TOKEN ASC ");

			QueryRunner run = new QueryRunner();
			connection = super.getConnection();
			list = (List<Token>) run.query(connection, query.toString(), new BeanListHandler(Token.class), new Object[] {});

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

	public Token getToken(String token) throws Exception {
		Connection connection = null;
		Token one = null;
		//
		try {

			StringBuffer query = new StringBuffer();
			query.append("SELECT  * ");
			query.append("  FROM MM_TOKEN                       ");
			query.append(" WHERE TOKEN  = ?       \n");

			QueryRunner run = new QueryRunner();
			connection = super.getConnection();
			one = (Token) run.query(connection, query.toString(), new BeanHandler(Token.class), new Object[] { token });

			//
		} catch (Exception ex) {
			log.error(ex);
			throw ex;
		} finally {
			super.closeConnection(connection);
		}
		return one;
	}


	public void saveToken(Token token) throws Exception {
		Connection connection = null;
		//
		try {

			connection = super.getConnection();
			connection.setAutoCommit(false);

			QueryRunner run = new QueryRunner();

			// 1. 마스터 저장
			StringBuffer query = new StringBuffer();
			query.append("\n");
			query.append("INSERT INTO MM_TOKEN                   \n");
			query.append("       (                                            \n");
			query.append("         IP,                            \n");
			query.append("         TOKEN,                            \n");
			query.append("         DESCRIPTION,                            \n");
			query.append("         INSERT_DATE                    \n");
			query.append("       ) values (                      \n");
			query.append("         ?,        \n");
			query.append("         ?,        \n");
			query.append("         ?,        \n");
			query.append("         ?   \n");
			query.append("        );         \n");

			run.update(connection, query.toString(),
					new Object[] { token.getIp(), token.getToken(), token.getDescription(), new Date() });

			connection.commit();
			//
			
			//
			super.getDomainChangeEventBus().onTokenChanged(token.getToken());
			
		} catch (Exception ex) {
			connection.rollback();
			log.error(ex);
			throw ex;
		} finally {
			connection.setAutoCommit(true);
			super.closeConnection(connection);
		}
	}

	public void updateToken(Token token) throws Exception {
		Connection connection = null;
		try {
			connection = super.getConnection();
			connection.setAutoCommit(false);

			//
			StringBuffer query = new StringBuffer();
			query.append("UPDATE MM_TOKEN                 \n");
			query.append("   SET IP = ? ,                 \n");
			query.append("       DESCRIPTION = ?        \n");
			query.append(" WHERE TOKEN = ?   \n");
			//
			QueryRunner run = new QueryRunner();
			run.update(connection, query.toString(),
					new Object[] { token.getIp(), token.getDescription(), token.getToken() });

			connection.commit();
			//
			
			//
			super.getDomainChangeEventBus().onTokenChanged(token.getToken());
			
		} catch (Exception ex) {
			connection.rollback();
			log.error(ex);
			throw ex;
		} finally {
			connection.setAutoCommit(true);
			super.closeConnection(connection);
		}
	}

	public void deleteToken(String token) throws Exception {
		Connection connection = null;
		try {
			connection = super.getConnection();
			connection.setAutoCommit(false);

			//
			StringBuffer query = new StringBuffer();
			query.append("DELETE FROM MM_TOKEN WHERE TOKEN = ? \n");
			//
			QueryRunner run = new QueryRunner();
			run.update(connection, query.toString(), new Object[] { token });

			connection.commit();
			//
			
			//
			super.getDomainChangeEventBus().onTokenChanged(token);
			
			
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
