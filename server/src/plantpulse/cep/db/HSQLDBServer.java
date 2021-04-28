package plantpulse.cep.db;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.hsqldb.Server;

import plantpulse.cep.db.datasource.HSQLDBDataSource;

/**
 * HSQLDBServer
 * 
 * @author lenovo
 *
 */
public class HSQLDBServer {

	private static final Log log = LogFactory.getLog(HSQLDBServer.class);

	private static class EmbedDBServerHolder {
		static HSQLDBServer instance = new HSQLDBServer();
	}

	public static HSQLDBServer getInstance() {
		return EmbedDBServerHolder.instance;
	}

	public static final String DB_PROPERTIES_PATH = "/db.properties";

	private Server hsqlServer = null;

	/**
	 * start
	 * 
	 * @param root
	 */
	public void start(String root) {

		try {
			
			/*
			 * TODO 외부에서 실행

			//
			Properties prop = PropertiesUtils.read(DB_PROPERTIES_PATH);

			//
			String db_scheme = prop.getProperty("db.scheme");
			String db_path = StringUtils.cleanPath("file:" + root + "/db/" + db_scheme);
			
			//db_path = StringUtils.cleanPath("file:c:/plantpulse-server/db/" + db_scheme);
			
			//
			hsqlServer = new Server();

			log.info(String.format("Embeded HSQLServer : db_scheme=[%s], db_path=[%s]", db_scheme, db_path));

			// HSQLDB prints out a lot of informations when
			// starting and closing, which we don't need now.
			// Normally you should point the setLogWriter
			// to some Writer object that could store the logs.
			hsqlServer.setLogWriter(null);
			hsqlServer.setSilent(true);

			// The actual database will be named 'xdb' and its
			// settings and data will be stored in files
			// testdb.properties and testdb.script
			hsqlServer.setDatabaseName(0, "fdd");
			hsqlServer.setDatabasePath(0, db_path);

			// Start the database!
			hsqlServer.start();

			log.info("Embeded HSQLServer started.");
			
			*/

			//
			// SchemeInitializer.init();
			
			//
			
			
			//데이터소스 초기화
		    HSQLDBDataSource.getInstance().init();
		     

		} catch (Exception ex) {
			log.error("Embeded HSQLServer start failed :" + ex.getMessage(), ex);
		}
	}

	/**
	 * stop
	 */
	public void stop() {
		if (hsqlServer != null) {
			hsqlServer.stop();
			log.info("Embeded HSQLServer stoped.");
		}
	}

}