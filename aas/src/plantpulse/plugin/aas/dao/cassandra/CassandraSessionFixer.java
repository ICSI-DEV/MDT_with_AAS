package plantpulse.plugin.aas.dao.cassandra;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import com.datastax.driver.core.Session;

import plantpulse.dbutils.cassandra.SessionManager;
import plantpulse.plugin.aas.utils.ConstantsJSON;

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
			String host = ConstantsJSON.getConfig().getString("cassandra.host");
			int port =  ConstantsJSON.getConfig().getInt("cassandra.port");
			String user = ConstantsJSON.getConfig().getString("cassandra.user");
			String password = ConstantsJSON.getConfig().getString("cassandra.password");
			String keyspace =  ConstantsJSON.getConfig().getString("cassandra.keyspace");
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
		return ConstantsJSON.getConfig().getString("cassandra.keyspace");
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
