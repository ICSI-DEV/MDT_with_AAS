package plantpulse.cep.engine.stream.type.distruptor.trans;

import com.lmax.disruptor.BlockingWaitStrategy;
import com.lmax.disruptor.RingBuffer;
import com.lmax.disruptor.dsl.Disruptor;
import com.lmax.disruptor.dsl.ProducerType;
import com.lmax.disruptor.util.DaemonThreadFactory;

import plantpulse.cep.engine.stream.StreamProcessor;
import plantpulse.cep.engine.stream.type.distruptor.PointDistruptorable;
import plantpulse.event.opc.Point;

public class PointStreamDistruptorWithTranslator implements StreamProcessor {
	
    private final int RING_SIZE = 1048576; // 1024, 64, 8
    
    private Disruptor<PointDistruptorable> disruptor = null;
    
    private RingBuffer<PointDistruptorable> ringBuffer;
    
    private MultiProducerWithTranslator.Publisher p  = null;
    
    private MultiProducerWithTranslator.IMessage message = null;
    
    private MultiProducerWithTranslator.ITransportable transportable = null;
    
    
    public  void start() throws Exception
    {
        disruptor = new Disruptor<>(
        		PointDistruptorable.FACTORY, RING_SIZE, DaemonThreadFactory.INSTANCE, ProducerType.MULTI, new BlockingWaitStrategy());
        disruptor.handleEventsWith(new MultiProducerWithTranslator.Consumer()).then(new MultiProducerWithTranslator.Consumer());
        ringBuffer = disruptor.getRingBuffer();
        p = new MultiProducerWithTranslator.Publisher();
        message = new MultiProducerWithTranslator.IMessage();
        transportable = new MultiProducerWithTranslator.ITransportable();
        
        disruptor.start();
       
    }

    
    /**
     * publish point
     * @param point
     */
    public void publish(Point point)
    {
    	 ringBuffer.publishEvent(p, message, transportable, point);
    };
    
    
    @Override
	public void stop() {
		if(disruptor != null) {
			disruptor.shutdown();
		}
	}
}
