package plantpulse.cep.service;

import java.util.ArrayList;
import java.util.List;

import org.apache.commons.lang.StringUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import com.espertech.esper.client.EPServiceProvider;
import com.espertech.esper.client.EPStatement;
import com.espertech.esper.client.UpdateListener;

import plantpulse.cep.domain.Statement;
import plantpulse.cep.engine.CEPEngineManager;

public class CEPStatementService {

	private static final Log log = LogFactory.getLog(CEPStatementService.class);

	public void deployStatement(Statement statement) throws Exception {
		// Start statement
		EPServiceProvider provider = CEPEngineManager.getInstance().getProvider();
		EPStatement stmt = null;
		if (statement.getIsPattern().equals("Y")) {
			stmt = provider.getEPAdministrator().createPattern(statement.getEpl(), statement.getStatementName());
		} else {
			stmt = provider.getEPAdministrator().createEPL(statement.getEpl(), statement.getStatementName());
		}
		if (StringUtils.isNotEmpty(statement.getListener())) {
			stmt.addListener((UpdateListener) Class.forName(statement.getListener()).newInstance());
		}
	}

	public void undeployStatement(Statement statement) throws Exception {
		// Start statement
		EPServiceProvider provider = CEPEngineManager.getInstance().getProvider();
		if (provider.getEPAdministrator().getStatement(statement.getStatementName()) != null) {
			provider.getEPAdministrator().getStatement(statement.getStatementName()).destroy();
		} else {
			log.warn("Undeployed statement = [" + statement.getStatementName() + "]");
		}
		//
	}

	public void redeployStatement(Statement statement) throws Exception {
		undeployStatement(statement);
		//
		deployStatement(statement);
	}

	public void startStatement(Statement statement) throws Exception {
		// Start statement
		EPServiceProvider provider = CEPEngineManager.getInstance().getProvider();
		EPStatement stmt = provider.getEPAdministrator().getStatement(statement.getStatementName());
		if (stmt != null && stmt.isStopped()) {
			stmt.start();
			log.info("[" + statement.getStatementName() + "] statement start!!!");
		} else {
			throw new Exception("Statement [" + statement.getStatementName() + "] aleady started or undeployed!");
		}
	}

	public void stopStatement(String statementName) throws Exception {
		// Stop statement
		EPServiceProvider provider = CEPEngineManager.getInstance().getProvider();
		EPStatement stmt = provider.getEPAdministrator().getStatement(statementName);
		if (stmt != null && stmt.isStarted()) {
			stmt.stop();
			log.info("[" + statementName + "] statement stop!!!");
		} else {
			throw new Exception("Statement [" + statementName + "] aleady stoped or undeployed!");
		}
	}

	public List<Statement> getStatementStatus(List<Statement> statementList) {
		EPServiceProvider provider = CEPEngineManager.getInstance().getProvider();
		List<Statement> list = new ArrayList<Statement>();
		for (int i = 0; statementList != null && i < statementList.size(); i++) {

			Statement statement = statementList.get(i);
			// log.info("statement : " + statement.toString());
			// if(provider == null) log.error("pv null");
			EPStatement stmt = provider.getEPAdministrator().getStatement(statement.getStatementName());
			try {
				if (stmt != null && stmt.isStarted()) {
					statement.setStatus("start");
				} else if (stmt != null && stmt.isStopped()) {
					statement.setStatus("stop");
				} else if (stmt == null) {
					statement.setStatus("undeployed");
				}
			} catch (Exception e) {
				//
				statement.setStatus("error");
			}
			list.add(statement);
		}
		return list;
	}
}
