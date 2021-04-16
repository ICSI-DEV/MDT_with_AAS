package plantpulse.cep.dao;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;

import plantpulse.cep.db.ConnectionUtils;
import plantpulse.cep.engine.eventbus.DomainChangeEventBus;
import plantpulse.cep.engine.eventbus.DomainChangeManager;

public abstract class CommonDAO {

	private ConnectionUtils utils = new ConnectionUtils();

	public Connection getConnection() throws SQLException {
		return utils.getConnection();
	}

	public void closeConnection(Connection connection) throws SQLException {
		utils.closeConnection(connection);
	}

	public void closeConnection(ResultSet rs, PreparedStatement pstmt, Connection connection) throws SQLException {
		utils.closeConnection(rs, pstmt, connection);
	}
	
	public DomainChangeEventBus getDomainChangeEventBus() throws Exception {
		return DomainChangeManager.getBus();
	}

}
