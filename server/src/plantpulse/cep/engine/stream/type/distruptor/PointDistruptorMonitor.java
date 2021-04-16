package plantpulse.cep.engine.stream.type.distruptor;

import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import com.lmax.disruptor.dsl.Disruptor;

import plantpulse.cep.engine.stream.processor.PointStreamStastics;
import plantpulse.cep.engine.timer.TimerExecuterPool;

/**
 * PointDistruptorMonitor
 * @author leesa
 *
 */
public class PointDistruptorMonitor  {

	private static Log log = LogFactory.getLog(PointDistruptorMonitor.class);

	private ScheduledFuture<?> timer;
	
	private Disruptor<PointDistruptorable> distruptor;

	public PointDistruptorMonitor(Disruptor<PointDistruptorable> distruptor) {
		this.distruptor = distruptor;
	}

	public void start() {

		timer = TimerExecuterPool.getInstance().getPool().scheduleAtFixedRate(new Runnable() {

			@Override
			public void run() {
				try {
						
					PointStreamStastics.REMAINING_CAPACITY.set(distruptor.getRingBuffer().remainingCapacity());
					
				} catch (Exception e) {
					log.error("Point distruptor monitoring failed : " + e.getMessage(), e);
				} finally {

				}
			}

		}, 60 * 1000, 1000 * 10, TimeUnit.MILLISECONDS); //

	}

	public void stop() {
		timer.cancel(true);
		timer = null;
	}

}
