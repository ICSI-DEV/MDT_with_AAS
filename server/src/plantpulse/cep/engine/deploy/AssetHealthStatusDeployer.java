package plantpulse.cep.engine.deploy;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import plantpulse.cep.engine.logging.EngineLogger;
import plantpulse.cep.engine.scheduling.JobDateUtils;
import plantpulse.cep.engine.scheduling.JobManager;
import plantpulse.cep.engine.scheduling.job.AssetHealthStatusJob;
import plantpulse.cep.engine.storage.StorageConstants;

/**
 * AssetHealthStatusDeployer
 * @author lsb
 *
 */
public class AssetHealthStatusDeployer implements Deployer  {

	private static final Log log = LogFactory.getLog(AssetHealthStatusDeployer.class);

	public void deploy() {
		try {

			String job_id = "TAG_ASSET_HEALTH_STATUS_JOB";
			String term   = StorageConstants.ASSET_HEALTH_TERM;
			JobManager.getInstance().startJob(job_id, JobDateUtils.getCronPattern(term), new AssetHealthStatusJob(),  term);
			
			log.info("Asset health status job deployed.");

		} catch (Exception ex) {
			EngineLogger.error("에셋 헬스 상태 처리기를 배치하는 도중 오류가 발생하였습니다 : 에러=" + ex.getMessage());
			log.error("Asset health status deploy error : " + ex.getMessage(), ex);
		}
	}

}
