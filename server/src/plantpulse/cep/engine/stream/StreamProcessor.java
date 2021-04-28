package plantpulse.cep.engine.stream;

import plantpulse.event.opc.Point;

/**
 * StreamProcessor
 * @author leesa
 *
 */
public interface StreamProcessor {
	
	 public  void start() throws Exception;
	
	 public  void publish(Point point);
	 
	 public  void stop();

}
