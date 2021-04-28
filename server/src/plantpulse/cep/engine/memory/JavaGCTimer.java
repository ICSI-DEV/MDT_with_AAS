package plantpulse.cep.engine.memory;

import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import plantpulse.cep.engine.timer.TimerExecuterPool;
import plantpulse.cep.engine.utils.ByteUtils;

/**
 * JavaGCTimer
 * @author lsb
 *
 */
public class JavaGCTimer {

	private static final Log log = LogFactory.getLog(JavaGCTimer.class);
	
	private static final int GC_INERVAL = (1000 * 60) * 10; //10분

	private ScheduledFuture<?> task;

	public void start() {

		task = TimerExecuterPool.getInstance().getPool().scheduleAtFixedRate(new Runnable() {
			@Override
			public void run() {
				try {

					log.info("Java system GC starting...");
					System.gc();
					log.info("Java system GC completed : free_memory=[" + ByteUtils.humanReadableByteCount(Runtime.getRuntime().freeMemory()) + "]");

				} catch (Exception e) {
					log.error("JavaGCTimer failed : " + e.getMessage(), e);
				}

			}

		}, GC_INERVAL, GC_INERVAL, TimeUnit.MILLISECONDS); // 10분에 한번씩
	}

	public void stop() {
		task.cancel(true);
	}

}
