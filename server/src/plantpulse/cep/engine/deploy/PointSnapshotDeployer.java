package plantpulse.cep.engine.deploy;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import plantpulse.cep.engine.logging.EngineLogger;
import plantpulse.cep.engine.scheduling.JobDateUtils;
import plantpulse.cep.engine.scheduling.JobManager;
import plantpulse.cep.engine.scheduling.job.TagPointSnapshotJob;
import plantpulse.cep.engine.storage.StorageConstants;

/**
 * 
 * SELECT id, timestamp, count(*), avg(to_double(value)), min(to_double(value)),
 * max(to_double(value)), stddev(to_double(value)) FROM Point(type = 'int' or type
 * = 'double' or type = 'float' or type = 'long').win:time_batch(10 sec) GROUP
 * BY id;
 * 
 * 
 * 10초 1분 10분 1시간 12시간 24시간
 * 
 * @author lsb
 *
 */
public class PointSnapshotDeployer implements Deployer {

	private static final Log log = LogFactory.getLog(PointAggregationDeployer.class);

	public void deploy() {
		try {

			for (int i = 0; i < StorageConstants.SNAPSHOT_TERMS_ARRAY.length; i++) {
				//
				final String term = StorageConstants.SNAPSHOT_TERMS_ARRAY[i];
				String job_id = "TAG_SNAPSHOT_JOB_" + term.replaceAll(" ", "_");
				JobManager.getInstance().startJob(job_id, JobDateUtils.getCronPattern(term), new TagPointSnapshotJob(), term);
			}
			
			log.info("Point snapshot job deployed.");

		} catch (Exception ex) {
			EngineLogger.error("태그 포인트 스냅샷 처리기를 배치하는 도중 오류가 발생하였습니다 : 에러=" + ex.getMessage());
			log.error("Point snapshot deploy error : " + ex.getMessage(), ex);
		}
	}

}
