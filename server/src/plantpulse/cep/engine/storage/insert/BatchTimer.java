package plantpulse.cep.engine.storage.insert;

import java.util.Timer;
import java.util.TimerTask;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.json.JSONObject;

import com.google.common.util.concurrent.ThreadFactoryBuilder;

import plantpulse.cep.engine.CEPEngineManager;
import plantpulse.cep.engine.config.ConfigurationManager;
import plantpulse.cep.engine.diagnostic.DiagnosticHandler;
import plantpulse.cep.engine.properties.PropertiesLoader;
import plantpulse.cep.engine.storage.StorageProductType;
import plantpulse.cep.engine.storage.insert.cassandra.CassandraBatchWriteWorker;
import plantpulse.cep.engine.storage.insert.cassandra.CassandraTokenAwareBatchWriteWorker;
import plantpulse.cep.engine.storage.timeseries.TimeSeriesDatabase;
import plantpulse.diagnostic.Diagnostic;

/**
 * CassandraBatchTableOneTimer
 * 
 * @author lsb
 * 
 */
public class BatchTimer {

	private static Log log = LogFactory.getLog(BatchTimer.class);

	
	//
	private static final boolean BATCH_DELEGATION_CLUSTER = BatchProperties.getBoolean("batch.delegation.cluster");
	
	//
	private static final int TIMER_SCHEDULE_DELAY = BatchProperties.getInt("batch.timer_schedule_delay");
	private static final int TIMER_SCHEDULE_PERIOD = BatchProperties.getInt("batch.timer_schedule_period");

	//
	private static final int THREAD_POOL_CORE_SIZE = BatchProperties.getInt("batch.thread_pool_core_size");
	private static final int THREAD_POOL_MAX_SIZE = BatchProperties.getInt("batch.thread_pool_max_size");
	private static final int THREAD_POOL_IDLE_TIMEOUT = BatchProperties.getInt("batch.thread_pool_idle_timeout");
	private static final int THREAD_POOL_QUEUE_SIZE = BatchProperties.getInt("batch.thread_pool_queue_size");
	private static final int THREAD_POOL_BATCH_THREAD_SIZE = BatchProperties.getInt("batch.thread_pool_batch_thread_size");
	private static final int THREAD_POOL_AWAIT_TERMINATION_TIMEOUT_SEC = BatchProperties.getInt("batch.thread_pool_await_termination_timeout_sec");
	
	
	private static final boolean MICRO_BATCH_ENABLED       = BatchProperties.getBoolean("batch.token.micro.batch.enabled");
	private static final boolean PATITION_GROUPING_ENABLED = BatchProperties.getBoolean("batch.batch.patition.grouping.enabled");
	private static final boolean USE_ASYNC_STATEMENT = BatchProperties.getBoolean("batch.use.async.statement");
	

	private Timer timer;
	private int timer_number;

	private String storage_db_type = "";
	private String storage_db_version = "";
	
	private int replication_factor = 1;
	
	public  boolean token_micro_batch_enabled = MICRO_BATCH_ENABLED;
	private boolean patition_grouping_enabled = PATITION_GROUPING_ENABLED;
	private boolean use_async_statement = USE_ASYNC_STATEMENT;
	
	private TimeSeriesDatabase timeseries_sync = null;

	/**
	 * Batch Timer
	 * 
	 * @param timer_number
	 */
	public BatchTimer(int timer_number) {
		this.timer_number = timer_number;
		this.init();
	}

	/**
	 * 
	 */
	public void init() {

		//
		storage_db_type = ConfigurationManager.getInstance().getServer_configuration().getStorage_db_type();
		storage_db_version = ConfigurationManager.getInstance().getServer_configuration().getStorage_db_version();
 
        //		
		String replication_json = PropertiesLoader.getStorage_properties().getProperty("storage.replication", "{ 'replication_factor' : 1 }");
		replication_factor = JSONObject.fromObject(replication_json).getInt("replication_factor");
		if(replication_factor > 1) { //복제 팩터가 1 이상일 경우 자동으로 활성화
			token_micro_batch_enabled = true;
		};
		
		
		//
		log.info("Storage batch timer[" + timer_number + "] initiallized : storage_db_type=[" + storage_db_type + " | "+ storage_db_version + "], "
				+ "replication_factor=[" + replication_factor + "], token_micro_batch_enabled=[" + token_micro_batch_enabled + "], patition_grouping_enabled=[" + patition_grouping_enabled + "]" );

		//
		 timeseries_sync = CEPEngineManager.getInstance().getTimeseries_database();
	}

	/**
	 * 
	 */
	public void start() {

		if(BATCH_DELEGATION_CLUSTER) {
			return; //클러스터에 배치를 위임했을 경우, 실행하지 않습니다.
		};
		
		if(!BATCH_DELEGATION_CLUSTER) {
			log.warn("Version 8.0.0, the batch has been separated and is no longer used.");
			return; //클러스터에 배치를 위임했을 경우, 실행하지 않습니다.
		};
		
		timer = new Timer("PP_STORAGE_BATCH_TIMER_" + timer_number);

		timer.scheduleAtFixedRate(new TimerTask() {

			@Override
			public void run() {
				try {

					log.debug("BatchTimer[" + timer_number + "] started.");
					//
					BatchWorkerRejectedHandler rejection_handler = new BatchWorkerRejectedHandler();

					//
					ThreadFactory thread_factory = new ThreadFactoryBuilder()
							.setNameFormat("PP_BATCH_TIMER_THREAD_POOL-[" + timer_number + "]-%d")
							.setDaemon(false)
			        		.setPriority(Thread.MAX_PRIORITY)
							.build();
					
					ThreadPoolExecutor  executor_pool = new ThreadPoolExecutor(
							THREAD_POOL_CORE_SIZE, // 실행할 최소
							THREAD_POOL_MAX_SIZE,  // 최대 Thread 지원수
							THREAD_POOL_IDLE_TIMEOUT, // 30초 동안 아이들일경우 종료
							TimeUnit.SECONDS, 
							new ArrayBlockingQueue<Runnable>(THREAD_POOL_QUEUE_SIZE), 
							thread_factory, 
							rejection_handler);

					//
					//BatchExecutorPoolMonitor monitor = new BatchExecutorPoolMonitor(timer_number, executor_pool, 10);
					//Thread monitorThread = new Thread(monitor);
					//monitorThread.start();
			
					
					BatchTimerStastics.getInstance().getTIMER_ACTIVE().incrementAndGet();
					
					
					 
					for (int i = 0; i < THREAD_POOL_BATCH_THREAD_SIZE; i++) {
						Runnable thread = null;
						if (storage_db_type.equals(StorageProductType.CASSANDRA) || storage_db_type.equals(StorageProductType.SCYLLADB)) {
							if (storage_db_version.startsWith("3") || storage_db_version.startsWith("4")) {
								  if(token_micro_batch_enabled){
									  thread = new CassandraTokenAwareBatchWriteWorker(timer_number, i, patition_grouping_enabled, replication_factor, timeseries_sync); 
								  }else{
									  thread = new CassandraBatchWriteWorker(timer_number, i, patition_grouping_enabled, use_async_statement, timeseries_sync);
								  };
							} else {
								throw new Exception("Unsupport storage_db_version [" + storage_db_version + "]");
							}
						} else {
							log.error("Unsupport db type : storage_db_type=[" + storage_db_type + "]");
						}
						// Runnable
						executor_pool.execute(thread);
						Thread.sleep(10);
					}// for
					
					
					
					//
					executor_pool.shutdown();
					try {
					    if (!executor_pool.awaitTermination(THREAD_POOL_AWAIT_TERMINATION_TIMEOUT_SEC, TimeUnit.SECONDS)) {
					    	log.error("BatchTimer[" + timer_number + "] operation could not be completed. "
					    			+ "await_termination_timeout=[" + THREAD_POOL_AWAIT_TERMINATION_TIMEOUT_SEC + "]sec. "
					    					+ "Database performance tuning or node expansion is essential to complete the task within the set time frame. After call shutdownNow().");
					    	
					    	DiagnosticHandler.handleFormLocal(System.currentTimeMillis(), Diagnostic.LEVEL_ERROR, 
					    			"데이터베이스에 포인트를 저장하기 위한 쓰레드의 작업 시간이 [" + THREAD_POOL_AWAIT_TERMINATION_TIMEOUT_SEC + "]초 이상 소요되었습니다." );
					    	
					    	executor_pool.shutdownNow();
					    }
					} catch (InterruptedException e1) {
						executor_pool.shutdownNow();
					    Thread.currentThread().interrupt();
					}
					log.debug("BatchTimer[" + timer_number + "] shutdown.");
					


				} catch (Exception ex) {
					log.error("BatchTimer error : " + ex.getMessage(), ex);
				} finally {
					BatchTimerStastics.getInstance().getTIMER_ACTIVE().decrementAndGet();
				}
				
			};

		}, TIMER_SCHEDULE_DELAY, TIMER_SCHEDULE_PERIOD); //
		
		
	}

	public void stop() {
		//
		if(timer != null) {
			timer.cancel();
		}
	};

}
