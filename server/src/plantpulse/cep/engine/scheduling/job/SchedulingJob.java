package plantpulse.cep.engine.scheduling.job;

import org.quartz.Job;
import org.quartz.JobExecutionContext;
import org.quartz.JobExecutionException;

/**
 * SchedulingJob
 * 
 * @author lsb
 *
 */
abstract public class SchedulingJob implements Job, Runnable {

	public JobExecutionContext context;

	public void execute(JobExecutionContext context) throws JobExecutionException {
		this.context = context;
		run();
	}

	public abstract void run();
	
}
