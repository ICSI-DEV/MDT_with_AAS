package plantpulse.cep.query;

import java.util.Collections;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import plantpulse.cep.domain.Query;

/**
 * QueryFactory
 * 
 * @author lenovo
 *
 */
public class QueryFactory {

	private static class QueryFactoryHolder {
		static QueryFactory instance = new QueryFactory();
	}

	public static QueryFactory getInstance() {
		return QueryFactoryHolder.instance;
	}

	private Map<String, Query> map = Collections.synchronizedMap(new ConcurrentHashMap<String, Query>());

	public synchronized void addQuery(Query query) {
		map.put(query.getId(), query);
	}

	public synchronized void removeQuery(Query query) {
		map.remove(query.getId());
	}

	public synchronized Map<String, Query> getMap() {
		return map;
	}

}
