package plantpulse.cep.engine.network;

import com.google.common.util.concurrent.AtomicDouble;

/**
 * 네트워크 지연 MS
 * @author leesa
 *
 */
public class Latency {
	
	public static final String UNIT = "ms";
	
	public static final AtomicDouble AVG = new AtomicDouble(0.0);
	public static final AtomicDouble MIN = new AtomicDouble(0.0);
	public static final AtomicDouble MAX = new AtomicDouble(0.0);

}
