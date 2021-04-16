package plantpulse.cep.db;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import plantpulse.cep.db.datasource.HSQLDBDataSource;

/**
 * ConnectionUtils
 * @author lsb
 *
 */
public class ConnectionUtils {

	private static final Log log = LogFactory.getLog(ConnectionUtils.class);

	/**
	 * getConnection
	 * 
	 * @return
	 * @throws SQLException
	 * @throws ClassNotFoundException
	 */
	public Connection getConnection() throws SQLException {
		return HSQLDBDataSource.getInstance().getConnection();
	}

	/**
	 * closeConnection
	 * 
	 * @param connection
	 */
	public void closeConnection(Connection connection) {
		try {
			if (connection != null)
				connection.close();
		} catch (SQLException e) {
			log.error("Embeded HSQLServer Connection close failed.", e);
		}
	}

	public void closeConnection(ResultSet rs, Statement pstmt, Connection connection) {
		try {
			if (rs != null)
				rs.close();
			if (pstmt != null)
				pstmt.close();
			if (connection != null)
				connection.close();
		} catch (SQLException e) {
			log.error("Embeded HSQLServer Connection close failed.", e);
		}

	}

}
