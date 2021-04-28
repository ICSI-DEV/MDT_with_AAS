package plantpulse.cep.engine.messaging.backup.timer;

import java.io.File;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.commons.io.FileUtils;
import org.apache.commons.lang.StringUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.infobip.lib.popout.FileQueue;

import com.codahale.metrics.Timer;

import plantpulse.cep.engine.CEPEngineManager;
import plantpulse.cep.engine.async.AsynchronousExecutorStatus;
import plantpulse.cep.engine.logging.EngineLogger;
import plantpulse.cep.engine.messaging.backup.TimeoutBackupDB;
import plantpulse.cep.engine.messaging.backup.TimeoutBackupReProcessTimer;
import plantpulse.cep.engine.messaging.listener.MessageListenerStatus;
import plantpulse.cep.engine.monitoring.metric.MetricNames;
import plantpulse.cep.engine.monitoring.metric.MonitoringMetrics;
import plantpulse.cep.engine.pipeline.DataFlowPipe;
import plantpulse.cep.engine.timer.TimerExecuterPool;
import plantpulse.event.opc.Point;
import plantpulse.json.JSONObject;

public class TimeoutBackupReProcessTimerFileQueue implements TimeoutBackupReProcessTimer {
	
	private static final Log log = LogFactory.getLog(TimeoutBackupReProcessTimerFileQueue.class);
	
	private final DataFlowPipe dataflow = CEPEngineManager.getInstance().getData_flow_pipe();
	
	private static final int TIMER_PERIOD   = 1000 * 10; // 1 초마다
	
	private static final int LIMIT_PROCESS_COUNT  = 100_000; //1초당 백업 1만건씩 처리
	
	private static final int REMAINING_WARNNING_COUNT = 1_000_000; //
	
	private ScheduledFuture<?> task;
	
	private TimeoutBackupDB bmdb;
	
	public TimeoutBackupReProcessTimerFileQueue(TimeoutBackupDB bmdb) {
		this.bmdb = bmdb;
	}

	public void start() {
		//
		//
		task = TimerExecuterPool.getInstance().getPool().scheduleAtFixedRate(new Runnable() {
			@Override
			public void run() {
				
				final Timer.Context time = MonitoringMetrics.getTimer(MetricNames.MESSAGE_TIMEOUT_RE_PROCESSED_TIME).time();
				try {
					if(!AsynchronousExecutorStatus.IS_BUSY) {
						//
						long total = bmdb.size();
						if(total > 0) {
							 long start = System.currentTimeMillis();
							 //
						 	 log.debug("Re-processing timeout message from backup : target=[" + LIMIT_PROCESS_COUNT + "], total=[" + total + "]");
						 	 
							 @SuppressWarnings("unchecked")
							FileQueue<String> queue = (FileQueue<String>) bmdb.getDb();
							final AtomicInteger processed = new AtomicInteger(0);
							
							  int loop = 0;
							  while (true) {
								   loop++;
								   if(LIMIT_PROCESS_COUNT < loop) {
							    	   break;
							       };
							       
								    final String value =  queue.poll();
									    if(StringUtils.isNotEmpty(value)) {
									    final JSONObject json = JSONObject.fromObject(value);
								        final Point point = (Point) JSONObject.toBean(json, Point.class);
								        
								      
									 	 dataflow.flow(point);
									     MessageListenerStatus.TIMEOUT_BACKUPED_REPROCESS_COUNT.incrementAndGet();
									     processed.incrementAndGet();
									    //
								    }
									       
								}
							 
							 //남은 백업 건수
							 MessageListenerStatus.TIMEOUT_BACKUPED_REMAINING_COUNT.set(queue.size());
							
							long end = System.currentTimeMillis() - start;
						    //
							String file_size = FileUtils.byteCountToDisplaySize(folderSize(bmdb.getFile()));
							log.info("Re-processing timeout message from backup : processed_count/exec_time=[" + processed.get() + "/" + end + "ms], remaining_count=[" + bmdb.size() + "], file_size=[" + file_size + "]");
							
							if(bmdb.size() > REMAINING_WARNNING_COUNT) {
								EngineLogger.warn("현재 시스템이 처리해야할 타임아웃 메세지가 너무 많습니다 : 대기 건수=[" + bmdb.size() + "]");
							};
						}
					}else {
						log.debug("The thread pooling system is busy and holds work.");
					};
					//
					//bmdb.commit();
				} catch (Exception e) {
					log.error("Backup timeout message re-proecessing error : " + e.getMessage(), e);
				} finally {
					time.stop();
					MonitoringMetrics.getMeter(MetricNames.MESSAGE_TIMEOUT_RE_PROCESSED).mark();
				}

			}

		}, 1000 * 60 * 1, TIMER_PERIOD, TimeUnit.MILLISECONDS);

		log.info("Backup timeout message re-process timer started.");

	}

	public void stop() {
		task.cancel(true);
	};
	
	
	public static long folderSize(File directory) {
	    long length = 0;
	    for (File file : directory.listFiles()) {
	        if (file.isFile())
	            length += file.length();
	        else
	            length += folderSize(file);
	    }
	    return length;
	}
	

}
