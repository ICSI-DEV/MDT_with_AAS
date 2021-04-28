package plantpulse.cep.engine.storage.session;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import com.datastax.driver.core.Session;

import plantpulse.cep.engine.config.ConfigurationManager;
import plantpulse.dbutils.cassandra.SessionManager;

/**
 * CassandraSessionManager
 * 
 * @author leesa
 *
 */
public class CassandraSessionFixer {
	
	private static final Log log = LogFactory.getLog(CassandraSessionFixer.class);

	/**
	 * 세션을 반환한다.
	 * 
	 * @return
	 * @throws Exception
	 */
	public static Session getSession() throws Exception {
		try {
			String host = ConfigurationManager.getInstance().getServer_configuration().getStorage_host();
			int port = Integer.parseInt(ConfigurationManager.getInstance().getServer_configuration().getStorage_port());
			String user = ConfigurationManager.getInstance().getServer_configuration().getStorage_user();
			String password = ConfigurationManager.getInstance().getServer_configuration().getStorage_password();
			String keyspace = ConfigurationManager.getInstance().getServer_configuration().getStorage_keyspace();
			return SessionManager.getSession(host, port, user, password, keyspace);
		} catch (Exception e) {
			log.error("Cassandra session getting error : " + e.getMessage(), e);
			throw e;
		}
	}
	
	/**
	 * 카산드라 키스페이스를 반환한다.
	 * 
	 * @return
	 */
	public static String getKeyspace() {
		return ConfigurationManager.getInstance().getServer_configuration().getStorage_keyspace();	
	}
	
	/**
	 * 복제 계수 옵션을 반환한다.
	 * 
	 * @return
	 */
	public static String getReplication() {
		return ConfigurationManager.getInstance().getServer_configuration().getStorage_replication();
	}
	
	/**
	 * 추가 컬럼 
	 * 
	 * @return
	 */
	public static String getAppendColumns() {
		return ConfigurationManager.getInstance().getServer_configuration().getStorage_append_columns();
	}
	
	
	public static void shutdown() {
		try {
			getSession().close();
		} catch (Exception e) {
			log.error("Cassandra session close error : " + e.getMessage(), e);
		}
		try {
			getSession().getCluster().close();
		} catch (Exception e) {
			log.error("Cassandra cluster close error : " + e.getMessage(), e);
		}
	}

}
