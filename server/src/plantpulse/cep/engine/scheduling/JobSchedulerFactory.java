package plantpulse.cep.engine.scheduling;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.quartz.Scheduler;
import org.quartz.SchedulerException;
import org.quartz.impl.StdSchedulerFactory;



/**
 * JobSchedulerFactory
 * @author leesa
 *
 */
public class JobSchedulerFactory {
	
	private static final Log log = LogFactory.getLog(JobSchedulerFactory.class);
	
	private static class JobSchedulerFactoryHolder {
		static JobSchedulerFactory instance = new JobSchedulerFactory();
	}

	public static JobSchedulerFactory getInstance() {
		return JobSchedulerFactoryHolder.instance;
	}

	private StdSchedulerFactory schedulerFactory;
	
	private Scheduler scheduler;

	public static final int DEFAULT_THREAD_COUNT = 10;
	
	private boolean inited  = false;
	
	

	public void init() throws Exception {
		  try {
			  /*
		        Properties props = new Properties();
		        props.put(StdSchedulerFactory.PROP_SCHED_INSTANCE_NAME,  "PP_SCHEDULER");
		        props.put(StdSchedulerFactory.PROP_SCHED_INSTANCE_ID,    "PLATFORM");
		        props.put(StdSchedulerFactory.PROP_SCHED_THREAD_NAME,     "_THREAD");
		        
		        //
		        props.put("org.quartz.threadPool.class",          "org.quartz.simpl.SimpleThreadPool");
		        props.put("org.quartz.threadPool.threadCount",    "10");
		        props.put("org.quartz.threadPool.threadPriority",  "5");
		        props.put("org.quartz.threadPool.threadsInheritContextClassLoaderOfInitializingThread", "true");
		        props.put("org.quartz.jobStore.class", "plantpulse.cep.engine.scheduling.InMemoryJobStore");
		        */

		       //  schedulerFactory = new StdSchedulerFactory();
		        
		        scheduler = StdSchedulerFactory.getDefaultScheduler();
		    	
		        //Listener attached to group named "group 1" only.
		    	//scheduler.getListenerManager().addJobListener(
		    	//	new PlatfromJobListener(), GroupMatcher.jobGroupEquals("PP_JOB")
		    	//);
		        
		    	scheduler.getListenerManager().addTriggerListener(new JobTriggerListener());
		    	
		    	
		        inited = true;
		        
		        log.info("Job scheduler factory inited.");
		    } catch (SchedulerException e) {
		        throw new Exception("JobSchedulerFactory init error : " + e.getMessage(), e);
		    }
	}
	
	

	public void start() throws Exception {
		  try {
			    scheduler.start();
		        log.info("Job scheduler factory started.");
		    } catch (SchedulerException e) {
		        throw new Exception("JobSchedulerFactory start error : " + e.getMessage(), e);
		    }
	}
	
	public void resumeAll()  {
		  try {
			    scheduler.resumeAll();
		        log.info("Job scheduler factory resumeAll.");
		    } catch (SchedulerException e) {
		        log.error("JobSchedulerFactory resumeAll error : " + e.getMessage(), e);
		    }
	}
	
	public void shutdown()  {
		  try {
		        scheduler.shutdown();
		        inited = false;
		        log.info("Job scheduler factory shutdowned.");
		    } catch (SchedulerException e) {
		        log.error("JobSchedulerFactory shutdown error : " + e.getMessage(), e);
		    }
	}
	
	public  Scheduler getScheduler() throws Exception  {
	  if(!inited) {
		  throw new Exception("JobSchedulerFactory is not inited.");
	  }
	  return scheduler;
	}

}
