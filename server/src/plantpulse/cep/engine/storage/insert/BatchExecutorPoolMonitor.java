package plantpulse.cep.engine.storage.insert;

import java.util.concurrent.ThreadPoolExecutor;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

public class BatchExecutorPoolMonitor implements Runnable {

	private static Log log = LogFactory.getLog(BatchExecutorPoolMonitor.class);

	private int timer_number;

	private ThreadPoolExecutor executor;

	private int seconds;

	private boolean run = true;

	public BatchExecutorPoolMonitor(int timer_number, ThreadPoolExecutor executor, int delay) {
		this.timer_number = timer_number;
		this.executor = executor;
		this.seconds = delay;
	}

	public void shutdown() {
		this.run = false;
	}

	@Override
	public void run() {
		while (run) {

			try {
				log.debug(String.format("Storage batch executor pool monitor : parent_timer[" + timer_number + "], pool=[%d/%d], active=[%d], completed=[%d], task=[%d], is_shutdown=[%s], is_terminated=[%s]",
						this.executor.getCorePoolSize(), this.executor.getMaximumPoolSize(), this.executor.getActiveCount(), this.executor.getCompletedTaskCount(), this.executor.getTaskCount(),
						this.executor.isShutdown(), this.executor.isTerminated()));
				//
				Thread.sleep(seconds * 1000);

				//
			} catch (InterruptedException e) {
				log.error(e);
			}
		}

	}
}