package plantpulse.cep.engine.deploy;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import plantpulse.cep.engine.logging.EngineLogger;

/**
 * PointRowDeployer
 * 
 * @author lsb
 *
 */
public class PointMapBinaryDeployer implements Deployer {

	private static final Log log = LogFactory.getLog(PointMapBinaryDeployer.class);
	
	public static final String term = "1 SECONDS";

	public void deploy() {
		try {
			//
			//String job_id = "TAG_POINT_MAP_BINARY_JOB_" + term;
			//JobManager.getInstance().startJob(job_id, "0/1 * * * * ?", new TagPointMapBynaryJob(), term);
            //
		} catch (Exception ex) {
			EngineLogger.error("태그 포인트 맵 바이너리 잡을 배치하는 도중 오류가 발생하였습니다 : 에러=" + ex.getMessage());
			log.error("Point map binary job deploy error : " + ex.getMessage(), ex);
		}
	}

}
