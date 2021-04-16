package plantpulse.plugin.aas.dao.cassandra;

import java.util.HashMap;
import java.util.Map;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import com.datastax.driver.core.ConsistencyLevel;
import com.datastax.driver.core.PreparedStatement;
import com.datastax.driver.core.Session;

/**
 * PreparedStatementCacheFactory
 * @author lsb
 *
 */
public class PreparedStatementCacheFactory {
	
	private static final Log log = LogFactory.getLog(PreparedStatementCacheFactory.class);
	
	private static class StaementCacheFactoryHolder {
		static PreparedStatementCacheFactory instance = new PreparedStatementCacheFactory();
	}

	public static PreparedStatementCacheFactory getInstance() {
		return StaementCacheFactoryHolder.instance;
	}
	
	private Map<String, PreparedStatement> map = new HashMap<String, PreparedStatement>();
	
	/**
	 * PreparedStatement  getting and cache.
	 * @param session
	 * @param query
	 * @param level
	 * @return
	 */
	public synchronized PreparedStatement get(Session session, String query, ConsistencyLevel level){
		if(map.containsKey(query)){
			 return map.get(query);
		}else{
			PreparedStatement statement = session.prepare(query);
			if(level != null){
				//TODO 컨시스턴시를 READ/WRITE로 구분하여 정리해야 함. 
				//statement.setConsistencyLevel(level);
			}
			map.put(query, statement);
			log.info("CQL=[" + query + "] Prepared statement cached.");
			return map.get(query);
		}
	}

}
