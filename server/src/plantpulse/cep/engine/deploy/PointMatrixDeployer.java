package plantpulse.cep.engine.deploy;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import plantpulse.cep.engine.logging.EngineLogger;

/**
 * 
 * PointMatrixDeployer
 * 
 * 1초
 * 
 * @author lsb
 *
 */
public class PointMatrixDeployer implements Deployer {

	//
	private static final Log log = LogFactory.getLog(PointMatrixDeployer.class);

	//
	public void deploy() {
		try {
			   
			//
		    // String job_id = "TAG_POINT_MATRIX_JOB";
		    // JobManager.getInstance().startJob(job_id, JobDateUtils.getCronPattern("1 SECONDS"), new TagPointMatrixJob(), "1 SECONDS");
		    // log.info("Point matrix job deployed.");
		    //
		} catch (Exception ex) {
			EngineLogger.error("태그 포인트 매트릭스 처리기를 배치하는 도중 오류가 발생하였습니다 : 에러=" + ex.getMessage());
			log.error("Point matrix deploy error : " + ex.getMessage(), ex);
		}
	}

}
