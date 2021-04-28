package plantpulse.cep.engine.logging;

import java.util.concurrent.atomic.AtomicInteger;


/**
 * LoggingCount
 * @author lsb
 *
 */
public class LoggingCount {
	
	//메세지 수신
	public static AtomicInteger INFO_COUNT = new AtomicInteger();
	public static AtomicInteger WARN_COUNT  = new AtomicInteger();
	public static AtomicInteger ERROR_COUNT = new AtomicInteger();
	
	
	public static void reset(){
		INFO_COUNT.set(0);
		WARN_COUNT.set(0);
		ERROR_COUNT.set(0);
	}

}
