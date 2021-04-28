package plantpulse.cep.engine.scheduling;

import java.util.Map;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.quartz.CronScheduleBuilder;
import org.quartz.Job;
import org.quartz.JobBuilder;
import org.quartz.JobDetail;
import org.quartz.Scheduler;
import org.quartz.Trigger;
import org.quartz.TriggerBuilder;

/**
 * JobManager using Quarts
 * 
 * @author lsb
 *
 */
public class JobManager {
	
	private static final Log log = LogFactory.getLog(JobManager.class);

	private static class JobManagerHolder {
		static JobManager instance = new JobManager();
	}

	public static JobManager getInstance() {
		return JobManagerHolder.instance;
	}
	
	
	public static final String JOB_GROUP = "PP_JOB";
	
	/**
	 * startJob
	 * @param id
	 * @param cron
	 * @param cls
	 * @param term
	 * @throws Exception
	 */
	public void startJob(String id, String cron, Job cls) throws Exception{
	
		try {
			//
			JobDetail job = JobBuilder.newJob(cls.getClass()).withIdentity(id,  JOB_GROUP).build();
			Trigger trigger = TriggerBuilder.newTrigger()
					  .withIdentity(id, "PP_TRIGGER")
					  .withSchedule(CronScheduleBuilder.cronSchedule(cron))
					  .build();
			               
			 Scheduler s = JobSchedulerFactory.getInstance().getScheduler();
	         //
			 s.scheduleJob(job, trigger);
					
			//
			log.info("Job scheduled : id=[" + id + "], cron=[" + cron + "], job_class=[" + cls.toString() + "]");
		
		}catch(Exception ex){
			log.error("Job scheduled failed : " + ex.getMessage(), ex);
		}
	}
	
	/**
	 * startJob
	 * @param id
	 * @param cron
	 * @param cls
	 * @param term
	 * @throws Exception
	 */
	public void startJob(String id, String cron, Job cls, String term) throws Exception{
	
		try {
			//
			JobDetail job = JobBuilder.newJob(cls.getClass()).withIdentity(id, JOB_GROUP).build();
			job.getJobDataMap().put("term", term);
			Trigger trigger = TriggerBuilder.newTrigger()
					  .withIdentity(id, "PP_TRIGGER")
					  .withSchedule(CronScheduleBuilder.cronSchedule(cron))
					  .build();
			  
			 Scheduler s = JobSchedulerFactory.getInstance().getScheduler();
			 s.scheduleJob(job, trigger);
					      
			//
			log.info("Job scheduled : id=[" + id + "], cron=[" + cron + "], job_class=[" + cls.toString() + "]");
		
		}catch(Exception ex){
			log.error("Job scheduled failed : " + ex.getMessage(), ex);
		}
	}
	
	/**
	 * startJob
	 * @param id
	 * @param cron
	 * @param cls
	 * @param term
	 * @param attributes
	 * @throws Exception
	 */
	public void startJob(String id, String cron, Job cls, String term, Map<String,String> attributes) throws Exception{
	
		try {
			//
			JobDetail job = JobBuilder.newJob(cls.getClass()).withIdentity(id, JOB_GROUP).build();
			job.getJobDataMap().put("term", term);
			job.getJobDataMap().putAll(attributes);
			
			Trigger trigger = TriggerBuilder.newTrigger()
					  .withIdentity(id, "PP_TRIGGER")
					  .withSchedule(CronScheduleBuilder.cronSchedule(cron))
					  .build();
			  
			 Scheduler s = JobSchedulerFactory.getInstance().getScheduler();
			 s.scheduleJob(job, trigger);
					      
			//
			log.info("Job scheduled : id=[" + id + "], cron=[" + cron + "], job_class=[" + cls.toString() + "]");
		
		}catch(Exception ex){
			log.error("Job scheduled failed : " + ex.getMessage(), ex);
		}
	}
	

}
