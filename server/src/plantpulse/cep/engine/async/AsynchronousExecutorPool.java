package plantpulse.cep.engine.async;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import com.google.common.util.concurrent.ThreadFactoryBuilder;

import plantpulse.cep.engine.async.throttling.ThrottlingExecutorService;
import plantpulse.cep.engine.properties.PropertiesLoader;

/**
 * AsynchronousExecutorPool
 * <pre> 단일 실행으로 완료되는 쓰레드을 풀링하는 풀</pre>
 * 
 * @author leesa
 *
 */
public class AsynchronousExecutorPool {
	
	private static final Log log = LogFactory.getLog(AsynchronousExecutorPool.class);

	public static final int PROCESSOR_CORES = Runtime.getRuntime().availableProcessors();
	public static final int CORE_SIZE   = PROCESSOR_CORES ;
	
	public static final int PARALLELISM = PROCESSOR_CORES ; 
	
	//
	public static final int RATE_LIMIT   = 40_000; 
	
	//
	public static final boolean IS_THROTTLING   = true; 
	
	//
	public static final int MONITORING_SEC = 1;         //1초
	
	//
	public static final int SHUTDOWN_TIMEOUT_SEC = 30;
	
	//
	private AsynchronousExecutorMonitor monitor =  null;

	

    private  final List<Worker> activeWorkers = Collections.synchronizedList(new ArrayList<>());

    
	private static class AsyncPoolHolder {
		static AsynchronousExecutorPool instance = new AsynchronousExecutorPool();
	}

	public static AsynchronousExecutorPool getInstance() {
		return AsyncPoolHolder.instance;
	};
	
	private ThreadPoolExecutor executor = null;
	
	public ExecutorService getPool(){
		return executor;
	}
	
	public void start(){
		 
		  log.info("Asynchronous executor pool starting ... ");
	  	  
		  //
	
		  int parallelism = Integer.parseInt(PropertiesLoader.getEngine_properties().getProperty("engine.async.parallelism", PARALLELISM + ""));
		  int ratelimit   = Integer.parseInt(PropertiesLoader.getEngine_properties().getProperty("engine.async.ratelimit", RATE_LIMIT + ""));
		  
		  
		  //
		  if(IS_THROTTLING) {
			  executor = new ThrottlingExecutorService(parallelism, parallelism,  ratelimit, TimeUnit.SECONDS);
		  }else {
			  ThreadFactory thread_factory = new ThreadFactoryBuilder()
						.setNameFormat("PP-ASYNC-POOL-WORKER-%d")
						.setDaemon(false)
		        		.setPriority(Thread.MAX_PRIORITY)
						.build();
				
				 executor = new ThreadPoolExecutor(
						CORE_SIZE, // 실행할 최소
						CORE_SIZE,  // 최대 Thread 지원수
						10, TimeUnit.SECONDS,   // 10초 동안 아이들일경우 종료
						new ArrayBlockingQueue<Runnable>(1_000_000), 
						thread_factory);
		  };
		
		  //
		  monitor = new AsynchronousExecutorMonitor(executor, MONITORING_SEC);
		  monitor.start();
		 
		  //
		  log.info("Asynchronous executor pool started : processors=[" + PROCESSOR_CORES + "], "
		  		+ " parallelism=[" + parallelism + "], rate_limit_of_seconds_window=[" + ratelimit + "]");
		 
	};
	
	
	public AsynchronousExecutorMonitor getMonitor() {
		return monitor;
	}
	
	
	public void shutdown() {
		   executor.shutdown(); 
		   try {
		     if (!executor.awaitTermination(SHUTDOWN_TIMEOUT_SEC, TimeUnit.SECONDS)) {
		       executor.shutdownNow(); //
		       if (!executor.awaitTermination(SHUTDOWN_TIMEOUT_SEC, TimeUnit.SECONDS))
		           log.error("Asynchronous executor pool did not terminated.");
		     }
		   } catch (InterruptedException ie) {
		     executor.shutdownNow();
		     Thread.currentThread().interrupt();
		   }
	 };
	 

	 public boolean isBusy() {
		 return AsynchronousExecutorStatus.IS_BUSY;
	 };
	 
	 
	 public void addWorker(Worker r){
		 activeWorkers.add(r);
	 }

	 public void removeWorker(Worker r){
		 activeWorkers.remove(r);
	 }
	 
	 public List<Worker> getActiveWorkerList(){
		 return activeWorkers;
	 }

}
