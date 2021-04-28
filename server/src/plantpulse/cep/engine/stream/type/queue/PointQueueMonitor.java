package plantpulse.cep.engine.stream.type.queue;

import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.redisson.api.RQueue;

import plantpulse.cep.engine.logging.EngineLogger;
import plantpulse.cep.engine.stream.processor.PointStreamStastics;
import plantpulse.cep.engine.stream.queue.cep.CEPDataPointQueue;
import plantpulse.cep.engine.stream.type.distruptor.PointDistruptorable;
import plantpulse.cep.engine.timer.TimerExecuterPool;


/**
 * PointQueueMonitor
 * @author leesa
 *
 */
public class PointQueueMonitor {

	private static Log log = LogFactory.getLog(PointQueueMonitor.class);

	private ScheduledFuture<?> timer;
	
	private int WARN_QUEUE_SIZE = 10_000 * 10; //10만건

	public PointQueueMonitor() {
		
	}

	public void start() {

		timer = TimerExecuterPool.getInstance().getPool().scheduleAtFixedRate(new Runnable() {

			@Override
			public void run() {
				try {
					
					CEPDataPointQueue<RQueue<PointDistruptorable>> queue = PointQueueFactory.getCEPDataPointQueue("STREAM_CEP_POINT_QUEUE_MONITOR");
						
					if(queue.getQueue() != null){
						int pending_queue_count = queue.getQueue().size();
						PointStreamStastics.PENDING_QUEUE_COUNT.addAndGet(pending_queue_count) ;
						if(pending_queue_count > WARN_QUEUE_SIZE){
							EngineLogger.warn("태그 포인트 큐에서 소비 프로세스가 너무 느립니다. 대기중인 현재 큐는 [" + pending_queue_count +"]개가 있습니다.");
							log.warn("Tag point queue consume very slow : waiting_queue = [" + pending_queue_count + "]");
						}
					};
					
					
				} catch (Exception e) {
					log.error("Point queue monitoring failed : " + e.getMessage(), e);
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
