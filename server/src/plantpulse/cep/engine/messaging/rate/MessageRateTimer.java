package plantpulse.cep.engine.messaging.rate;

import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import plantpulse.cep.engine.CEPEngineManager;
import plantpulse.cep.engine.messaging.listener.MessageListenerStatus;
import plantpulse.cep.engine.timer.TimerExecuterPool;
import plantpulse.json.JSONObject;


/**
 * MessageRateTimer
 * 
 * @author leesa
 *
 */
public class MessageRateTimer {

	private static final Log log = LogFactory.getLog(MessageRateTimer.class);

	private ScheduledFuture<?> timer_1_sec;
	private ScheduledFuture<?> timer_1_min;
	
	/**
	 * start
	 */
	public void start() {
		//
		
		AtomicLong last_total_message_count_1sec = new AtomicLong(MessageListenerStatus.TOTAL_IN_COUNT.get());
		AtomicLong last_total_message_count_1min = new AtomicLong(MessageListenerStatus.TOTAL_IN_COUNT.get());
		

		//
		timer_1_sec = TimerExecuterPool.getInstance().getPool().scheduleAtFixedRate(new Runnable() {
			@Override
			public void run() {
				try {
					
					long curr = MessageListenerStatus.TOTAL_IN_COUNT.get() - last_total_message_count_1sec.get();
					MessageListenerStatus.TOTAL_IN_RATE_1_SEC.set(curr);
					last_total_message_count_1sec.set(MessageListenerStatus.TOTAL_IN_COUNT.get()); 
					JSONObject json = new JSONObject();
					json.put("tatal_count_by_1sec", MessageListenerStatus.TOTAL_IN_COUNT.get());
					json.put("rate_by_1sec", MessageListenerStatus.TOTAL_IN_RATE_1_SEC.get());
					
					CEPEngineManager.getInstance().getMessageBroker().getSTOMPPublisher().send("/messaging/rate", json.toString());
					
				} catch (Exception e) {
					log.error(e, e);
				}
			}
		}, 10*1000, 1 * 1000, TimeUnit.MILLISECONDS);
		
		//
		timer_1_min = TimerExecuterPool.getInstance().getPool().scheduleAtFixedRate(new Runnable() {
			@Override
			public void run() {
				try {
					long curr = MessageListenerStatus.TOTAL_IN_COUNT.get() - last_total_message_count_1min.get();
					MessageListenerStatus.TOTAL_IN_RATE_1_MIN.set(curr);
					last_total_message_count_1min.set(MessageListenerStatus.TOTAL_IN_COUNT.get()); 
				} catch (Exception e) {
					log.error(e);
				}
			}
		}, 10*1000, 60 * 1000, TimeUnit.MILLISECONDS);

		log.info("Message rate calculate timer started.");
	}

	public void stop() {
		timer_1_sec.cancel(true);
		timer_1_min.cancel(true);
	};


}
