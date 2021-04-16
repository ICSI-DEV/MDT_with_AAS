package plantpulse.cep.engine.async;

import java.util.Timer;
import java.util.TimerTask;
import java.util.concurrent.ThreadPoolExecutor;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

public class AsynchronousExecutorMonitor {
	
	private static final Log log = LogFactory.getLog(AsynchronousExecutorMonitor.class);
	
	 private ThreadPoolExecutor executor;
	 private int second;
	 
	 private boolean run = false;
	 
	 private long WARN_PENDING_TASK_SIZE = 2_000; //
	 
	 private long PENDING_WORKER_LOG_COUNT = 1_000; //
	 
	 private Timer timer ;
     
	 public AsynchronousExecutorMonitor(ThreadPoolExecutor threadPoolExecutor, int time){
	    this.executor = threadPoolExecutor;
	    this.second = time;
	 }
	 

	 public void start() {
		     timer = new Timer();
	         timer.scheduleAtFixedRate(new MonitorTimerTask(this.executor), 60 * 1000, second * 1000);
	         run = true;
	 }
	 
	 public class MonitorTimerTask extends TimerTask {
		 private ThreadPoolExecutor executor;
		 public MonitorTimerTask(ThreadPoolExecutor executor) {
			 this.executor = executor;
		 }
		 
		@Override
		public void run() {
			AsynchronousExecutorStatus.POOL_SIZE = executor.getPoolSize();
			AsynchronousExecutorStatus.CORE_POOL_SIZE =  this.executor.getCorePoolSize();
			AsynchronousExecutorStatus.MAXIMUM_POOL_SIZE =  this.executor.getMaximumPoolSize();
			AsynchronousExecutorStatus.ACTIVE_COUNT = this.executor.getActiveCount();
			AsynchronousExecutorStatus.COMPLETED_TASK_COUNT = this.executor.getCompletedTaskCount();
			AsynchronousExecutorStatus.TASK_COUNT = this.executor.getTaskCount();
			AsynchronousExecutorStatus.PENDING_TASK_SIZE = this.executor.getQueue().size();
			
			//액티브 쓰레드가 CPU 풀의 80% 이상일때 모니터링 표시
			//펜딩이 1개 이상일때 모니터링 표시
			if((this.executor.getCorePoolSize() * 0.8) <= this.executor.getActiveCount()
					||
					0 < this.executor.getQueue().size()) {
				log.info(String.format("Aync threads monitor : " +
						"pool=[%d/%d|%d] " +
						"active_threads=[%d], " +
						"pedding_queues=[%d], " +
				        "tasks_completed=[%d]" ,
				        
				        this.executor.getPoolSize(),
				        this.executor.getCorePoolSize(),
				        this.executor.getMaximumPoolSize(),
				        this.executor.getActiveCount(),
				        this.executor.getQueue().size(),
				        this.executor.getCompletedTaskCount()
				        ));
				
			};;
			
			if( this.executor.getQueue().size() > PENDING_WORKER_LOG_COUNT) {
				try {
					StringBuffer wsb = new StringBuffer();
					int size = AsynchronousExecutorPool.getInstance().getActiveWorkerList().size();
			        for (Worker w : AsynchronousExecutorPool.getInstance().getActiveWorkerList()) {
			        	wsb.append(((Worker) w).getName()).append(", ");
			        };
			        wsb.append(" size=["  + size + "]");
					log.warn("Active aync workers : " + wsb.toString());
				}catch(Exception ex) {
					//ignore
				}
			};
			
			if(executor.getQueue().size() >= WARN_PENDING_TASK_SIZE) {
				AsynchronousExecutorStatus.IS_BUSY = true;
				log.info("Async thread pooling system heavy load : pedding_task_size=[" +  executor.getQueue().size() + "], busy_size=[>=" + WARN_PENDING_TASK_SIZE + "]");
			}else {
				AsynchronousExecutorStatus.IS_BUSY = false;
			}
			
			 run = true; //?
		}    
	 };
	 
	public int getPoolSize() {
		return executor.getPoolSize();
	};

	public int getCorePoolSize() {
		return executor.getCorePoolSize();
	}

	public int getMaximumPoolSize() {
		return executor.getMaximumPoolSize();
	}

	public int getActiveCount() {
		return executor.getActiveCount();
	}

	public long getCompletedTaskCount() {
		return executor.getCompletedTaskCount();
	}

	public long getTaskCount() {
		return executor.getTaskCount();
	}
	
	public long getPeddingTaskCount() {
		return executor.getQueue().size();
	}
	
	public void shutdown() {
		if(run) {
			run = false;
			if(timer != null) timer.cancel();
			if(executor != null) executor.shutdown();
		}
	}
}