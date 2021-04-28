package plantpulse.cep.engine.messaging.backup.timer;

import java.util.List;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.commons.io.FileUtils;
import org.apache.commons.lang.StringUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.redisson.api.RQueue;

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

/**
 * TimeoutBackupReProcessTimerInMemory
 * 
 * @author leesa
 *
 */
public class TimeoutBackupReProcessTimerInMemory implements TimeoutBackupReProcessTimer {

	private static final Log log = LogFactory.getLog(TimeoutBackupReProcessTimerInMemory.class);

	private final DataFlowPipe dataflow = CEPEngineManager.getInstance().getData_flow_pipe();

	private static final int TIMER_PERIOD = 10_000;  //10 초마다

	private static final int POLL_COUNT = 1_00_000;  //10 만건씩 처리
	
	private static final int REMAINING_WARNNING_COUNT = 1_000_000; //

	private ScheduledFuture<?> task;

	private TimeoutBackupDB bmdb;

	public TimeoutBackupReProcessTimerInMemory(TimeoutBackupDB bmdb) {
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
					if (!AsynchronousExecutorStatus.IS_BUSY) {
						//
						long total = bmdb.size();
						if (total > 0) {
							long start = System.currentTimeMillis();
							//
							log.debug("Re-processing timeout message from backup : target=[" + POLL_COUNT + "], total=[" + total + "]");

							@SuppressWarnings("unchecked")
							RQueue<String> queue = (RQueue<String>) bmdb.getDb();
							
							final AtomicInteger processed = new AtomicInteger(0);
							List<String> failed_list = queue.poll(POLL_COUNT);
							for (int i = 0; i < failed_list.size(); i++) {
								final String value = failed_list.get(i);
								if (StringUtils.isNotEmpty(value)) {
									final JSONObject json = JSONObject.fromObject(value);
									final Point point = (Point) JSONObject.toBean(json, Point.class);

									//
									dataflow.flow(point);
									MessageListenerStatus.TIMEOUT_BACKUPED_REPROCESS_COUNT.incrementAndGet();
									processed.incrementAndGet();
									//
								}
							}

							// 남은 백업 건수
							MessageListenerStatus.TIMEOUT_BACKUPED_REMAINING_COUNT.set(queue.size());

							long end = System.currentTimeMillis() - start;
							//
							String mem_size = FileUtils.byteCountToDisplaySize(bmdb.diskSize());
							log.info("Re-processing timeout message from backup : processed_count/exec_time=["
									+ processed.get() + "/" + end + "ms], remaining_count=[" + bmdb.size()
									+ "], memory_size=[" + mem_size + "]");

							if (bmdb.size() > REMAINING_WARNNING_COUNT) {
								EngineLogger.warn(
										"현재 시스템이 처리해야할 타임아웃 메세지가 너무 많습니다 : 대기 건수=[" + bmdb.size() + "]");
							}
							;
						}
					} else {
						log.debug("The thread pooling system is busy and holds work.");
					}
					;
					//
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

}
