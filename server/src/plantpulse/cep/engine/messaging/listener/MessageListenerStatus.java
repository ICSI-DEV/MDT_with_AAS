package plantpulse.cep.engine.messaging.listener;

import java.util.concurrent.atomic.AtomicLong;



/**
 * MessageListenerStatus
 * @author leesa
 *
 */
public class MessageListenerStatus {
	
	//
	public static AtomicLong TOTAL_IN_COUNT = new AtomicLong();
	public static AtomicLong TOTAL_IN_BYTES = new AtomicLong();
	//
	public static AtomicLong TOTAL_IN_COUNT_BY_KAFKA = new AtomicLong();
	public static AtomicLong TOTAL_IN_COUNT_BY_MQTT   = new AtomicLong();
	public static AtomicLong TOTAL_IN_COUNT_BY_STOMP  = new AtomicLong();
	public static AtomicLong TOTAL_IN_COUNT_BY_HTTP  = new AtomicLong();
	//
	public static AtomicLong TOTAL_IN_COUNT_BY_BLOB  = new AtomicLong();
	//
	public static AtomicLong TOTAL_IN_RATE_1_SEC  = new AtomicLong();
	public static AtomicLong TOTAL_IN_RATE_1_MIN  = new AtomicLong();
	
	//
	public static AtomicLong TIMEOUT_BACKUPED_TOTAL_COUNT = new AtomicLong();
	public static AtomicLong TIMEOUT_BACKUPED_REPROCESS_COUNT = new AtomicLong();
	public static AtomicLong TIMEOUT_BACKUPED_REMAINING_COUNT = new AtomicLong();
	
	
}
