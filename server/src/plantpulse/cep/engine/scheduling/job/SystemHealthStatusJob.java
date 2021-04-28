package plantpulse.cep.engine.scheduling.job;

import java.util.concurrent.TimeUnit;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import plantpulse.cep.engine.async.AsynchronousExecutor;
import plantpulse.cep.engine.async.Worker;
import plantpulse.cep.engine.logging.LoggingCount;
import plantpulse.cep.engine.scheduling.JobConstants;
import plantpulse.cep.engine.scheduling.JobDateUtils;
import plantpulse.cep.service.client.StorageClient;

/**
 * TagDataRowJob
 * @author lsb
 *
 */
public class SystemHealthStatusJob  extends SchedulingJob {

	private static final Log log = LogFactory.getLog(SystemHealthStatusJob.class);

	private String term;
	
	public SystemHealthStatusJob() {
		//
	}

	@Override
	public void run() {
		//
		try {

			log.debug("System health status job starting...");
			
			term = context.getJobDetail().getJobDataMap().getString("term");

			final long current_timestamp  = (System.currentTimeMillis()/1000) * 1000;
			final String from = JobDateUtils.from(current_timestamp, term);
			final String to  = JobDateUtils.to(current_timestamp);
			
			final StorageClient client = new StorageClient();
			
			TimeUnit.MILLISECONDS.sleep(JobConstants.JOB_DELAY_10_SEC);
			
			AsynchronousExecutor exec = new AsynchronousExecutor();
			exec.execute(new Worker() {
				public String getName() { return "SystemHealthStatusJob"; };
				@Override
				public void execute() {
					try {
						
						long start = System.currentTimeMillis();
						   //
							int info_count  = LoggingCount.INFO_COUNT.get();
							int warn_count  = LoggingCount.WARN_COUNT.get();
							int error_count = LoggingCount.ERROR_COUNT.get();
							
						        String status = "NORMAL";
						        if(info_count > 0){
						        	status = "INFO";
						        }
						        if(warn_count > 0){
						        	status = "WARN";
						        }
						        if(error_count > 0){
						        	status = "ERROR";
						        }
								
						        //
								client.forInsert().insertSystemHealthStatus(
										current_timestamp, 
										JobDateUtils.toTimestamp(from), 
										JobDateUtils.toTimestamp(to), 
										status, 
										info_count, 
										warn_count, 
										error_count
								);
								
							    //
								LoggingCount.reset();
								
							  long end = System.currentTimeMillis() - start;
							  log.debug("System health status all inserted : exec_time=[" + end + "]ms");
							  
					} catch (Exception e) {
						log.error("System health status insert error : " + e.getMessage(), e);
					}
				}
				
			});

		} catch (Exception e) {
			log.error("System health status job failed : " + e.getMessage(), e);
		}
		;
	}

}
