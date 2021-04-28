package plantpulse.cep.engine.pipeline;

import plantpulse.event.opc.Point;

/**
 * MessageDataFlowMessageDataFlow
 * @author lsb
 *
 */
public interface DataFlowPipe {
	
	public void init();
	
	public void flow(Point point) throws Exception;
	
	public void close();
	
}
