package plantpulse.cep.engine.messaging.background;

import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import plantpulse.cep.engine.logging.EngineLogger;
import plantpulse.cep.engine.messaging.listener.MessageListener;

/**
 * BackgroundMessageListenerThread
 * @author leesa
 *
 */
public class BackgroundMessageListenerThread implements Runnable {
	
	private static final Log log = LogFactory.getLog(BackgroundMessageListenerThread.class);
	
	private MessageListener listener;
	private AtomicBoolean started ;  
	
	public BackgroundMessageListenerThread(MessageListener listener, AtomicBoolean started) {
		this.listener = listener;
		this.started = started;
	}

	@Override
	public void run() {
		//
		Thread.currentThread().setName("PP-MESSAGE-LISTENER[" + listener.getClass().getName() + "]");
		Thread.currentThread().setPriority(Thread.MAX_PRIORITY);
		//
		int count = 0;
		while(!started.get()){
			try {
				Thread.sleep(2 * 1000);
				log.debug("서버 시작전이라 메세지 리스너를 시작 못함 [" + (++count) + "] : " + System.currentTimeMillis());
			} catch (InterruptedException e) {
				log.error("InterruptedException starting multi protocol message listeners : [" + listener.getClass().getName() + "]", e);;
			}
		};
		//
		try {
			listener.startListener();
			log.info("메세지 리스너를 시작하였습니다 : 클래스=[" + listener.getClass().getName() + "]");
		} catch (Exception e) {
			EngineLogger.error("메세지 리스너를 시작하지 못하였습니다 : 클래스=[" + listener.getClass().getName() + "], 에러=[" +  e.getMessage() + "]");
		}
	}

}
