package plantpulse.cep.engine.storage;

import java.util.Date;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.json.JSONObject;

import plantpulse.cep.engine.async.AsynchronousExecutorStatus;
import plantpulse.cep.engine.monitoring.MonitorConstants;
import plantpulse.cep.engine.storage.buffer.DBDataPointBufferFactory;
import plantpulse.cep.engine.storage.insert.BatchTimerStastics;
import plantpulse.cep.engine.timer.TimerExecuterPool;
import plantpulse.cep.listener.ResultServiceManager;
import plantpulse.cep.listener.push.PushClient;

/**
 * StorageMonitor
 * 
 * @author leesa
 *
 */
public class StorageMonitor {

	private static Log log = LogFactory.getLog(StorageMonitor.class);

	private ScheduledFuture<?> timer;

	private static long PREV_PROCESS_TOTAL_DATA = 0;

	public StorageMonitor() {
		
	}

	public void start() {

		timer = TimerExecuterPool.getInstance().getPool().scheduleAtFixedRate(new Runnable() {

			@Override
			public void run() {
				try {

					StorageStatus status = getStatus();

					if (true) {
						log.debug("=================================================================");
						log.debug("STORAGE MONITOR : DATE=[" + new Date() + "]");
						log.debug("=================================================================");
						log.debug("ACTIVE_TIMER = [" + status.getActive_timer() + "]");
						log.debug("ACTIVE_BATCH = [" + status.getActive_batch() + "]");
						log.debug("PROCESS_CURRENT_DATA = [" + status.getProcess_current_data() + "]");
						log.debug("PROCESS_TOTAL_DATA = [" + status.getProcess_total_data() + "]");
						log.debug("TOTAL_SAVED_TAG_DATA_COUNT = [" + status.getTotal_saved_tag_data_count() + "]");
						log.debug("WRITE_PER_SEC/1THREAD = [" + status.getWrite_per_sec_thread() + "]");
						log.debug("PENDING_DATA_SIZE_IN_BUFFER = [" + (status.getProcess_total_data()) + "]");
						log.debug("=================================================================");
					}

					PREV_PROCESS_TOTAL_DATA = status.getProcess_total_data();

					JSONObject json = JSONObject.fromObject(status);
					//추가
					json.put("thread_active_count", AsynchronousExecutorStatus.ACTIVE_COUNT);
					json.put("thread_pending_task_size", AsynchronousExecutorStatus.PENDING_TASK_SIZE);
					json.put("thread_pool_size", AsynchronousExecutorStatus.POOL_SIZE);
					json.put("thread_is_busy", AsynchronousExecutorStatus.IS_BUSY);
					
					PushClient push = ResultServiceManager.getPushService().getPushClient(MonitorConstants.PUSH_URL_STORAGE);
					push.sendJSON(json);

				} catch (Exception e) {
					log.error("Storage monitor error : " + e.getMessage(), e);
				} finally {

				}
			}

		}, 30 * 1000, 1000 * 1, TimeUnit.MILLISECONDS); //

	}

	public void stop() {
		timer.cancel(true);
		timer = null;
	}

	public StorageStatus getStatus() {
		StorageStatus status = new StorageStatus();
		status.setActive_timer((BatchTimerStastics.getInstance().getTIMER_ACTIVE().get()));
		status.setActive_batch((BatchTimerStastics.getInstance().getBATCH_ACTIVE().get()));
		status.setProcess_current_data(BatchTimerStastics.getInstance().getTOTAL_BATCH_PROCESS_COUNT().get() - PREV_PROCESS_TOTAL_DATA);
		status.setProcess_total_data((BatchTimerStastics.getInstance().getTOTAL_BATCH_PROCESS_COUNT().get()));
		status.setTotal_saved_tag_data_count(BatchTimerStastics.getInstance().getTOTAL_SAVED_PROCESS_COUNT().get());
		status.setWrite_per_sec_thread(
				Math.round((BatchTimerStastics.getInstance().getTOTAL_BATCH_PROCESS_COUNT().get() * 1000.0 / (BatchTimerStastics.getInstance().getTOTAL_BATCH_PROCESS_TIME_MS().get()))));
		status.setPending_data_size_in_buffer((DBDataPointBufferFactory.getDBDataBuffer().size()));
		return status;
	}

}
