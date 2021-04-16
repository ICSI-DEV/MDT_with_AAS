package plantpulse.cep.engine.stream.type.distruptor;

import java.util.concurrent.ThreadFactory;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import com.google.common.util.concurrent.ThreadFactoryBuilder;
import com.lmax.disruptor.BusySpinWaitStrategy;
import com.lmax.disruptor.EventHandler;
import com.lmax.disruptor.RingBuffer;
import com.lmax.disruptor.dsl.Disruptor;
import com.lmax.disruptor.dsl.ProducerType;

import plantpulse.cep.engine.stream.StreamProcessor;
import plantpulse.event.opc.Point;

/**
 * PointDistruptorProcessor
 * 
 * @author leesa
 *
 */
public class PointDistruptorProcessor implements StreamProcessor {

	private static final Log log = LogFactory.getLog(PointDistruptorProcessor.class);

	private Disruptor<PointDistruptorable> disruptor = null;

	private static final int _BUFFER_SIZE = 1048576; // 1024, 64, 8
	
	private static final boolean _ENABLE_BATCH = false;
	
	private PointDistruptorMonitor monitor = null;

	public void start() throws Exception {
		try {
			log.info("LMAX Disruptor staring for streaming CEP ... ");

			//
			ThreadFactory thread_factory = new ThreadFactoryBuilder().setNameFormat("PP_STREAM_DISTRUPTOR_POOL-%d").setDaemon(true).setPriority(Thread.MAX_PRIORITY).build();
			// Construct the Disruptor
			disruptor = new Disruptor<PointDistruptorable>(PointDistruptorable.FACTORY, _BUFFER_SIZE, thread_factory,
					ProducerType.MULTI, new BusySpinWaitStrategy());

		    //
			if (_ENABLE_BATCH) {
				disruptor.handleEventsWith(new PointDistruptorEventBatchHandler());
			} else {
				disruptor.handleEventsWith(new PointDistruptorEventDefaultHandler())
						.then(new EventHandler<PointDistruptorable>() {
							@Override
							public void onEvent(PointDistruptorable event, long sequence, boolean endOfBatch)
									throws Exception {
								event.clear();
							}
						});
			}
			;

			//
			monitor = new PointDistruptorMonitor(disruptor);
			monitor.start();
			//
			disruptor.start();
			log.info("LMAX Disruptor started.");
		} catch (Exception ex) {
			log.error("LMAX Disruptor starting error : " + ex.getMessage(), ex);
		}
	};

	/**
	 * publish point
	 * 
	 * @param point
	 */
	public void publish(Point point) {
		RingBuffer<PointDistruptorable> ringBuffer = disruptor.getRingBuffer();
		long sequence = ringBuffer.next(); // Grab the next sequence
		try {
			PointDistruptorable event = ringBuffer.get(sequence); // Get the entry in the Disruptor
			event.setPoint(point); // Fill with data
		} finally {
			ringBuffer.publish(sequence);
		}
	};

	@Override
	public void stop() {
		if(monitor != null) {
			monitor.stop();
		}
		if (disruptor != null) {
			disruptor.shutdown();
		}
	}

}
