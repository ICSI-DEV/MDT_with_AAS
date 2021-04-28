package plantpulse.cep.engine.deploy;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import plantpulse.cep.engine.logging.EngineLogger;
import plantpulse.cep.engine.scheduling.JobDateUtils;
import plantpulse.cep.engine.scheduling.JobManager;
import plantpulse.cep.engine.scheduling.job.SystemHealthStatusJob;
import plantpulse.cep.engine.storage.StorageConstants;

/**
 * SystemHealthStatusDeployer
 * @author lsb
 *
 */
public class SystemHealthStatusDeployer implements Deployer  {

	private static final Log log = LogFactory.getLog(SystemHealthStatusDeployer.class);

	public void deploy() {
		try {

			String job_id = "SYSTEM_HEALTH_STATUS_JOB";
			String term   = StorageConstants.SYSTEM_HEALTH_TERM;
			JobManager.getInstance().startJob(job_id, JobDateUtils.getCronPattern(term), new SystemHealthStatusJob(),  term);
			
			log.info("System health status job deployed.");

		} catch (Exception ex) {
			EngineLogger.error("시스템 헬스 상태 처리기를 배치하는 도중 오류가 발생하였습니다 : 에러=" + ex.getMessage());
			log.error("System health status deploy error : " + ex.getMessage(), ex);
		}
	}

}
