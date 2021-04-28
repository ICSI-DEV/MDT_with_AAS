package plantpulse.cep.db.datasource;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.Properties;

import org.apache.commons.dbcp2.BasicDataSource;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import plantpulse.cep.engine.utils.PropertiesUtils;


/**
 * HSQLDBDataSource
 * @author lsb
 *
 */
public class HSQLDBDataSource {

	private static final Log log = LogFactory.getLog(HSQLDBDataSource.class);

	private static class HSQLDBDataSourceHolder {
		static HSQLDBDataSource instance = new HSQLDBDataSource();
	}

	public static HSQLDBDataSource getInstance() {
		return HSQLDBDataSourceHolder.instance;
	}

	private BasicDataSource ds;
	
	public static  String DRIVER = "org.hsqldb.jdbc.JDBCDriver";
	public static  String URL = "jdbc:hsqldb:hsql://127.0.0.1:9001/pp";
	public static  String USER_NAME = "sa";
	public static  String PASSWORD  = "plantpulse";
	public static  int MAX_CONNECTIONS = 100;
	public static  int INITIAL_CONNECTIONS = 20;

	
	public void init() throws SQLException {
		try {
			//
			Properties props = PropertiesUtils.read("/storage.properties");
			///
			ds = new BasicDataSource();
			ds.setDriverClassName(props.getProperty("metastore.driver"));
			ds.setUrl(props.getProperty("metastore.url"));
			ds.setUsername(props.getProperty("metastore.user"));
			ds.setPassword(props.getProperty("metastore.password"));
			
			// 
			ds.setMinIdle(INITIAL_CONNECTIONS);
			ds.setMaxIdle(MAX_CONNECTIONS);
			ds.setMaxOpenPreparedStatements(1000);
			ds.setMaxTotal(MAX_CONNECTIONS); // Max Active connection count

			log.info("HSQL DataSource initialized : URL=[" + props.getProperty("metastore.url") + "]");
			
			
		} catch (Exception ex) {
			log.error("HSQL DataSource initialize failed.", ex);
		}

	}

	
	public Connection getConnection() throws SQLException {
		return this.ds.getConnection();
	}
	
	public void close() {
		 try {
			if(ds != null) ds.close();
		} catch (SQLException e) {
			log.error("HSQL DataSource close failed.", e);
		}
	}

}