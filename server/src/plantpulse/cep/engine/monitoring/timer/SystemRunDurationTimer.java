package plantpulse.cep.engine.monitoring.timer;

import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import plantpulse.cep.engine.timer.TimerExecuterPool;
import plantpulse.cep.service.client.StorageClient;

/**
 * SystemRunDurationTimer
 * 
 * @author leesa
 *
 */
public class SystemRunDurationTimer  implements MonitoringTimer {

	private static final Log log = LogFactory.getLog(SystemRunDurationTimer.class);

	private static final int TIMER_DELAY  = (1000 * 10); // 딜레이
	private static final int TIMER_PERIOD = (1000 * 10); // 10초마다 마다 실행

	private ScheduledFuture<?> task;
	

	public void start() {
		task = TimerExecuterPool.getInstance().getPool().scheduleAtFixedRate(new Runnable() {
			@Override
			public void run() {
				try {
					StorageClient client = new StorageClient();
					client.forInsert().updateSystemRunDuration();
				} catch (Exception e) {
					log.error("System running duration update error : " + e.getMessage() , e);
				}

			}

		}, TIMER_DELAY, TIMER_PERIOD, TimeUnit.MILLISECONDS);

		log.info("System running duration timer started.");

	}

	public void stop() {
		task.cancel(true);
	};


}