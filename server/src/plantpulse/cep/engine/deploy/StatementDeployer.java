package plantpulse.cep.engine.deploy;

import java.util.List;

import org.apache.commons.lang.StringUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import com.espertech.esper.client.EPServiceProvider;
import com.espertech.esper.client.EPStatement;
import com.espertech.esper.client.UpdateListener;

import plantpulse.cep.domain.Statement;
import plantpulse.cep.engine.logging.EngineLogger;
import plantpulse.cep.service.CEPService;

public class StatementDeployer implements Deployer {

	private static final Log log = LogFactory.getLog(StatementDeployer.class);
	
	private EPServiceProvider provider;
	
	public StatementDeployer(EPServiceProvider provider) {
		this.provider = provider;
	}

	public void deploy() {
		// 2.싱글 스테이트먼트 로딩
		CEPService service = new CEPService();
		try {
			List<Statement> statementList = service.getStatementList();
			if (statementList == null) {
				log.warn("Not found any EPL statement.");
			}

			for (int x = 0; statementList != null && x < statementList.size(); x++) {
				Statement statement = statementList.get(x);
				try {
					EPStatement stmt = null;
					if (statement.getIsPattern().equals("Y")) {
						stmt = provider.getEPAdministrator().createPattern(statement.getEpl(), statement.getStatementName());
					} else {
						stmt = provider.getEPAdministrator().createEPL(statement.getEpl(), statement.getStatementName());
					}
					if (StringUtils.isNotEmpty(statement.getListener())) {
						stmt.addListener((UpdateListener) Class.forName(statement.getListener()).newInstance());
					}
					//
				} catch (Exception ex) {
					EngineLogger.error("스테이트먼트 [" + statement.getStatementName() + "]을 배치하는 도중 오류가 발생하였습니다 : 에러=" + ex.getMessage());
					log.warn("Statement regist error : " + ex.getMessage(), ex);
				}
			}
		} catch (Exception ex) {
			EngineLogger.error("스테이트먼트를 배치하는 도중 오류가 발생하였습니다 : 에러=" + ex.getMessage());
			log.warn("Statement processing error : " + ex.getMessage(), ex);
		}
	}
}
