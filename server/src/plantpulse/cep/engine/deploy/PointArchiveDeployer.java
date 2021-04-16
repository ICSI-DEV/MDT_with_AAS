package plantpulse.cep.engine.deploy;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import plantpulse.cep.engine.logging.EngineLogger;

/**
 * PointArchiveDeployer
 * 
 * @author lsb
 *
 */
public class PointArchiveDeployer implements Deployer {

	private static final Log log = LogFactory.getLog(PointArchiveDeployer.class);
	
	//매 1시간마다 실행
	public static final String CRON = "0 0 * * * ?";

	public void deploy() {
		try {

			//TODO 태그 아카이브 잡 비활성화
			//String job_id = "TAG_ARCHIVE_JOB";
			//JobManager.getInstance().startJob(job_id, CRON, new TagPointArchiveJob());
			
			log.info("Point achive job deployed.");

		} catch (Exception ex) {
			EngineLogger.error("태그 포인트 아카이브 처리기를 배치하는 도중 오류가 발생하였습니다 : 에러=" + ex.getMessage());
			log.error("Point achive deploy error : " + ex.getMessage(), ex);
		}
	}

}
