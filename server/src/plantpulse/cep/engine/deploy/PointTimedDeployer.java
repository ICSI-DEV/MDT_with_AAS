package plantpulse.cep.engine.deploy;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import plantpulse.cep.engine.logging.EngineLogger;

/**
 * PointTimedDeployer
 * 
 * @author lsb
 *
 */
public class PointTimedDeployer  implements Deployer {

	private static final Log log = LogFactory.getLog(PointAggregationDeployer.class);
	
	private static final String BASE_TIME = "60 SECONDS";

	public void deploy() {
		try {

				//
			   /*
				StringBuffer eql = new StringBuffer();
				eql.append(" INSERT INTO PointTimed  "
						    + "SELECT  "
							+ "current_timestamp() as timestamp, "
							+ "id, "
							+ "name, "
							+ "type, "
							+ "count(id) as count, "
							+ "first(to_double(value)) as first, "
							+ "last(to_double(value))  as last, "
							+ "avg(to_double(value))   as avg, "
							+ "min(to_double(value))   as min, "
							+ "max(to_double(value))   as max, "
							+ "sum(to_double(value))   as sum, " 
							+ "stddev(to_double(value)) as stddev, " 
							+ "median(to_double(value)) as median  " 
							+ "");     
				eql.append("  FROM  Point(type = 'int' or type = 'long' or type = 'float' or type = 'double').win:ext_timed_batch(timestamp, " + BASE_TIME + ")");
				eql.append(" GROUP  by id, name, type  ");
				eql.append(" HAVING first(to_double(value)) IS NOT NULL AND last(to_double(value)) IS NOT NULL ");
				eql.append(" OUTPUT SNAPSHOT EVERY " + BASE_TIME + "  ");
				
				
			
				//
				EPStatement ep = CEPEngineManager.getInstance().getProvider().getEPAdministrator().createEPL(eql.toString(), "TAG_ITMED_EVENT");
			
			*/
		} catch (Exception ex) {
			EngineLogger.error("태그 집계 기준 이벤트 처리기를 배치하는 도중 오류가 발생하였습니다 : 에러=" + ex.getMessage());
			log.warn("Point timed deploy error : " + ex.getMessage(), ex);
		}
	}

}
