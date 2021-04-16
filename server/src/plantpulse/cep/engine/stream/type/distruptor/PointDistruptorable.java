package plantpulse.cep.engine.stream.type.distruptor;


import com.lmax.disruptor.EventFactory;

import plantpulse.cep.engine.stream.type.distruptor.trans.MultiProducerWithTranslator.IMessage;
import plantpulse.cep.engine.stream.type.distruptor.trans.MultiProducerWithTranslator.ITransportable;
import plantpulse.event.opc.Point;

/**
 * PointDistruptorable
 * @author leesa
 *
 */
public class PointDistruptorable implements java.io.Serializable {

	private static final long serialVersionUID = 8864741649599702569L;
	
	private Point point; // 큐에 들어간 시간

	private IMessage message;
	
	private ITransportable transportable;
     
	public PointDistruptorable() {
		super();
	};
	
	public PointDistruptorable(Point point) {
		super();
		this.point = point;
	}


	public Point getPoint() {
		return point;
	}


	public void setPoint(Point point) {
		this.point = point;
	};
	

	public void setMessage(IMessage message) {
		this.message = message;
	}

	public void setTransportable(ITransportable transportable) {
		this.transportable = transportable;
	}

	//
	public void clear() {
		point = null;
	}
	

    public static final EventFactory<PointDistruptorable> FACTORY = new EventFactory<PointDistruptorable>()
    {
        @Override
        public PointDistruptorable newInstance()
        {
            return new PointDistruptorable();
        }
    };

	
	

}
