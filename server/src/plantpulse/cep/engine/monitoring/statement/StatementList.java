package plantpulse.cep.engine.monitoring.statement;

import java.util.ArrayList;
import java.util.List;

/**
 * StatementList
 * 
 * @author lsb
 *
 */
public class StatementList {

	/**
	 * getList
	 * 
	 * @return
	 */
	public static List<Statement> getList(String serviceId) {
		
		List<Statement> list = new ArrayList<Statement>();
		list.add(new CurrentTimeStatement(serviceId, "CEP_MONITOR_CURRENT_TIME_STATEMENT"));
		list.add(new EngineMetricSummaryStatement(serviceId, "CEP_MONITOR_ENGINE_SUMMARY_STATEMENT"));
		list.add(new EngineMetricStatement(serviceId, "CEP_MONITOR_ENGINE_METRIC_STATEMENT"));
		list.add(new StatementMetricStatement(serviceId, "CEP_MONITOR_STATEMENT_METRIC_STATEMENT"));
		list.add(new StatementPerformanceStatement(serviceId, "CEP_MONITOR_STATEMENT_PERFORMANCE_STATEMENT"));
		list.add(new OSPerformanceStatement(serviceId, "CEP_MONITOR_OS_PERFORMANCE_STATEMENT"));
		
		return list;
	}

}
