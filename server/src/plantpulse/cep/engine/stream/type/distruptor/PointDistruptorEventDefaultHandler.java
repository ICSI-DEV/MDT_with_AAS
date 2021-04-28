package plantpulse.cep.engine.stream.type.distruptor;

import com.lmax.disruptor.EventHandler;

import plantpulse.cep.engine.stream.processor.PointStreamProcessor;

public class PointDistruptorEventDefaultHandler implements EventHandler<PointDistruptorable> {

	private PointStreamProcessor processor = new PointStreamProcessor();
	
	@Override
	public void onEvent(PointDistruptorable event, long sequence, boolean endOfBatch)  {
		try {
			processor.process(event.getPoint());
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

}
