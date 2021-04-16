package plantpulse.cep.engine.timer;

import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import plantpulse.cep.engine.thread.CommonThreadFactory;

/**
 * TimerExecuterPool
 * 
 * @author leesa
 *
 */
public class TimerExecuterPool {

	private static final Log log = LogFactory.getLog(TimerExecuterPool.class);

	public static final int CORE_SIZE = 4;
	public static final int MAX_SIZE = 8;

	private static class TimerExecuterPoolHolder {
		static TimerExecuterPool instance = new TimerExecuterPool();
	}

	public static TimerExecuterPool getInstance() {
		return TimerExecuterPoolHolder.instance;
	};

	private static final int SHUTDOWN_TIMEOUT_SEC = 5;

	private ScheduledThreadPoolExecutor pool = null;

	public void start() {
		pool = new ScheduledThreadPoolExecutor(CORE_SIZE, new CommonThreadFactory("PP-TIMER-POOL"));
		pool.setCorePoolSize(CORE_SIZE);
		pool.setMaximumPoolSize(MAX_SIZE);
		log.info("TimerExecuterPool inited : core=[" + CORE_SIZE + "], max=[" + MAX_SIZE + "]");
	}

	public void shutdown() {
		pool.shutdown();
		try {
			if (!pool.awaitTermination(SHUTDOWN_TIMEOUT_SEC, TimeUnit.SECONDS)) {
				pool.shutdownNow(); //
				if (!pool.awaitTermination(SHUTDOWN_TIMEOUT_SEC, TimeUnit.SECONDS))
					log.error("Timer executor pool did not terminated.");
			}
		} catch (InterruptedException ie) {
			pool.shutdownNow();
			Thread.currentThread().interrupt();
		}
	};

	public ScheduledThreadPoolExecutor getPool() {
		return pool;
	}

}
