package plantpulse.cep.engine.deploy;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import plantpulse.cep.engine.logging.EngineLogger;
import plantpulse.cep.engine.scheduling.job.TagPointMapJob;

/**
 * PointRowDeployer
 * 
 * 
 * @author lsb
 *
 */
public class PointMapDeployer implements Deployer {

	private static final Log log = LogFactory.getLog(PointMapDeployer.class);

	public void deploy() {
		try {
			//TODO 태그 포인트 맵 푸시 제거
			//TagPointPushJob tag_point_push_job = new TagPointPushJob();
			//tag_point_push_job.start();
			
			//
			TagPointMapJob tag_point_map_job = new TagPointMapJob();
			tag_point_map_job.start();
			
			log.info("Point and map push job deployed.");

		} catch (Exception ex) {
			EngineLogger.error("태그 포인트 & 맵 푸시 처리기를 배치하는 도중 오류가 발생하였습니다 : 에러=" + ex.getMessage());
			log.error("Point row deploy error : " + ex.getMessage(), ex);
		}
	}

}
