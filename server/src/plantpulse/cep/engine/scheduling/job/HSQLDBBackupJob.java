package plantpulse.cep.engine.scheduling.job;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import plantpulse.cep.dao.BackupDAO;

/**
 * HSQLDBBackupJob
 * @author lsb
 *
 */
public class HSQLDBBackupJob extends SchedulingJob {

	private static final Log log = LogFactory.getLog(HSQLDBBackupJob.class);
	
	public HSQLDBBackupJob() {
		//
	}

	@Override
	public void run() {
		//
		try {

			
			BackupDAO dao = new BackupDAO();
			dao.backup();
			
			log.info("HSQLDB backup job completed.");
			
		} catch (Exception e) {
			log.error("System health status job failed : " + e.getMessage(), e);
		}
		;
	}

}