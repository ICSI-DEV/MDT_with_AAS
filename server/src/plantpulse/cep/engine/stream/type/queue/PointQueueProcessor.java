package plantpulse.cep.engine.stream.type.queue;

import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.redisson.api.RQueue;

import com.google.common.util.concurrent.ThreadFactoryBuilder;

import plantpulse.cep.engine.stream.StreamProcessor;
import plantpulse.cep.engine.stream.queue.cep.CEPDataPointQueue;
import plantpulse.cep.engine.stream.type.distruptor.PointDistruptorable;
import plantpulse.event.opc.Point;

/**
 * PointQueueProcessor
 * 
 * @author lsb
 * 
 */
public class PointQueueProcessor implements StreamProcessor {

	private static Log log = LogFactory.getLog(PointQueueProcessor.class);

	//
	private static final int THREAD_SIZE = Runtime.getRuntime().availableProcessors();
	private static final int THREAD_POOL_CORE_SIZE   = 4;
	private static final int THREAD_POOL_MAX_SIZE    = THREAD_SIZE;
	private static final int THREAD_POOL_IDLE_TIMEOUT = 10;
	private static final int THREAD_POOL_QUEUE_SIZE = 10240;
	
	private ThreadFactory thread_factory = null;
	private ThreadPoolExecutor  executor = null;
	private PointQueueMonitor monitor = null;
	
	private CEPDataPointQueue<RQueue<PointDistruptorable>> queue = null;

	public void start() {
	
		//
		queue = PointQueueFactory.getCEPDataPointQueue("STREAM_CEP_QUEUE_PROCESSOR");
		
		//
		thread_factory = new ThreadFactoryBuilder()
				.setNameFormat("PP_STREAM_THREAD_POOL-%d")
				.setDaemon(false)
        		.setPriority(Thread.MAX_PRIORITY)
				.build();
		
		executor  = new ThreadPoolExecutor(
				THREAD_POOL_CORE_SIZE, // 실행할 최소
				THREAD_POOL_MAX_SIZE,  // 최대 Thread 지원수
				THREAD_POOL_IDLE_TIMEOUT, // 30초 동안 아이들일경우 종료
				TimeUnit.SECONDS, 
				new ArrayBlockingQueue<Runnable>(THREAD_POOL_QUEUE_SIZE), 
				thread_factory);
		
		//
		PointQueueTaskThread thread = new PointQueueTaskThread();
		for (int i = 0; i < THREAD_POOL_CORE_SIZE; i++) {
			executor.execute(thread);
		}
		//
		monitor = new PointQueueMonitor();
		monitor.start();
		//
		log.info("Point queue processor intiallized.");
	}

	public void publish(Point point) {
		try {
			PointDistruptorable data = new PointDistruptorable();
			data.setPoint(point);
			queue.getQueue().addAsync(data);
		} catch (Exception e) {
			log.error("Point queue data process failed.", e);
		}
	}

	@Override
	public void stop() {
		if(monitor != null) {
			monitor.stop();
		}
		if(executor != null) {
			executor.shutdownNow();
		}
	}

}
