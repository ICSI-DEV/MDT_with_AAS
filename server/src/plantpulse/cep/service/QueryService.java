package plantpulse.cep.service;

import java.util.List;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import com.espertech.esper.client.EPServiceProvider;
import com.espertech.esper.client.EPStatement;
import com.espertech.esper.client.UpdateListener;

import plantpulse.cep.dao.QueryHistoryDAO;
import plantpulse.cep.domain.Query;
import plantpulse.cep.engine.CEPEngineManager;
import plantpulse.cep.query.QueryFactory;
import plantpulse.cep.statement.QueryStatement;
import plantpulse.server.mvc.util.Utils;

public class QueryService {

	private static final Log log = LogFactory.getLog(QueryService.class);

	private QueryHistoryDAO dao;

	public QueryService() {
		this.dao = new QueryHistoryDAO();
	}

	public void compile(Query query) throws Exception {
		CEPEngineManager.getInstance().getProvider().getEPAdministrator().compileEPL(Utils.removeLastSemiColon(query.getEpl()));
	}

	public void run(Query query) throws Exception {
		QueryStatement stmt = new QueryStatement(query);
		EPStatement ep = CEPEngineManager.getInstance().getProvider().getEPAdministrator().createEPL(Utils.removeLastSemiColon(query.getEpl()), query.getId());
		ep.addListener(stmt);
		//
		QueryFactory.getInstance().addQuery(query);
	}

	public void run(Query query, String mode) throws Exception {
		QueryStatement stmt = new QueryStatement(query, mode);
		EPStatement ep = CEPEngineManager.getInstance().getProvider().getEPAdministrator().createEPL(Utils.removeLastSemiColon(query.getEpl()), query.getId());
		ep.addListener(stmt);
		//
		QueryFactory.getInstance().addQuery(query);
	}

	public void run(Query query, UpdateListener with_listener) throws Exception {
		EPStatement ep = CEPEngineManager.getInstance().getProvider().getEPAdministrator().createEPL(Utils.removeLastSemiColon(query.getEpl()), query.getId());
		ep.addListener(with_listener);
		//
		QueryFactory.getInstance().addQuery(query);
	}

	public boolean isRunning(Query query) {
		try {
			EPServiceProvider provider = CEPEngineManager.getInstance().getProvider();
			if (provider.getEPAdministrator().getStatement(query.getId()) != null) {
				return provider.getEPAdministrator().getStatement(query.getId()).isStarted() ? true : false;
			} else {
				return false;
			}
		} catch (Exception ex) {
			log.warn("Query is not running with exception : " + ex.getMessage(), ex);
			return false;
		}
	}

	public String getStatus(Query query) {
		EPServiceProvider provider = CEPEngineManager.getInstance().getProvider();
		if (isRunning(query)) {
			EPStatement stmt = provider.getEPAdministrator().getStatement(query.getId());
			if (stmt.isStarted()) {
				return ("start");
			} else if (stmt.isStopped()) {
				return ("stop");
			} else {
				return "unknown";
			}
		} else {
			return "undeployed";
		}
	}

	public void remove(Query query) throws Exception {
		if (isRunning(query)) {
			CEPEngineManager.getInstance().getProvider().getEPAdministrator().getStatement(query.getId()).destroy();
			QueryFactory.getInstance().removeQuery(query);
		} else {
			//log.warn("Undeployed query : " + query.getId());
		}
	}
	
	public void clearTempQuery() throws Exception {
		EPServiceProvider provider = CEPEngineManager.getInstance().getProvider();
		String[] stMM_name_array = provider.getEPAdministrator().getStatementNames();
		for(int i=0; stMM_name_array!=null && i < stMM_name_array.length; i++){
			if(stMM_name_array[i].startsWith("QUERY_TEMP_")){
				if(!provider.getEPAdministrator().getStatement(stMM_name_array[i]).isDestroyed()){
					provider.getEPAdministrator().getStatement(stMM_name_array[i]).destroy();
					log.info("Temp query destroyed : name=[" + stMM_name_array[i] + "]");
				}
			}
		}
	}
	
	public List<Query> getQueryHistoryList(String insert_user_id) throws Exception {
		return dao.getQueryHistoryList(insert_user_id);
	}

	public void saveQueryHistory(Query query_history) throws Exception {
		dao.saveQueryHistory(query_history);
	}

	public void deleteQueryHistory(long query_history_seq) throws Exception {
		dao.deleteQueryHistory(query_history_seq);
	}

	public void deleteAllQueryHistory(String insert_user_id) throws Exception {
		dao.deleteAllQueryHistory(insert_user_id);
	}

}
