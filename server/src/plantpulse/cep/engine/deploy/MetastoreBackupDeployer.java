package plantpulse.cep.engine.deploy;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import plantpulse.cep.engine.logging.EngineLogger;
import plantpulse.cep.engine.scheduling.JobManager;
import plantpulse.cep.engine.scheduling.job.HSQLDBBackupJob;


/**
 * 메타스토어
 * 매월 1일 정기 백업
 * @author lsb
 *
 */
public class MetastoreBackupDeployer implements Deployer  {

	private static final Log log = LogFactory.getLog(MetastoreBackupDeployer.class);

	public void deploy() {
		try {
			//
			HSQLDBBackupJob job = new HSQLDBBackupJob();
			JobManager.getInstance().startJob("METASTORE_BACKUP_JOB", "0 0 12 1 1/1 ? *", job);
			log.info("Metastore backup  deployed.");

		} catch (Exception ex) {
			EngineLogger.error("메타스토어 백업 처리기를 배치하는 도중 오류가 발생하였습니다 : 에러=" + ex.getMessage());
			log.error("Metastore backup deploy error : " + ex.getMessage(), ex);
		}
	}

}
