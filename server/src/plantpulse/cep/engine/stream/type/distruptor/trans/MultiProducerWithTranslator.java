package plantpulse.cep.engine.stream.type.distruptor.trans;


import com.lmax.disruptor.EventHandler;
import com.lmax.disruptor.EventTranslatorThreeArg;

import plantpulse.cep.engine.stream.processor.PointStreamProcessor;
import plantpulse.cep.engine.stream.type.distruptor.PointDistruptorable;
import plantpulse.event.opc.Point;


public class MultiProducerWithTranslator {
	
	public static class IMessage
    {
    }

	public static class ITransportable
    {
    }


    public static class Publisher implements EventTranslatorThreeArg<PointDistruptorable, IMessage, ITransportable, Point>
    {
        @Override
        public void translateTo(PointDistruptorable event, long sequence, IMessage msg, ITransportable itrans, Point point)
        {
            event.setMessage(msg);
            event.setTransportable(itrans);
            event.setPoint(point);;
        }
    }

    public static class Consumer implements EventHandler<PointDistruptorable>
    {
    	
        private PointStreamProcessor processor = new PointStreamProcessor();
        
        @Override
        public void onEvent(PointDistruptorable event, long sequence, boolean endOfBatch)
        {
        	try {
				processor.process(event.getPoint());
			} catch (Exception e) {
				e.printStackTrace();
			}
        }
    }

    
};