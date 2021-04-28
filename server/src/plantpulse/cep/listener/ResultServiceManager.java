package plantpulse.cep.listener;

import plantpulse.cep.listener.push.PushService;
import plantpulse.cep.listener.push.PushServiceImpl;
import plantpulse.cep.listener.streaming.StreamingService;
import plantpulse.cep.listener.streaming.StreamingServiceImpl;

/**
 * ResultServiceManager
 * 
 * @author leesangboo
 *
 */
public class ResultServiceManager {

	/**
	 * 푸쉬 서비스를 반환한다.
	 * 
	 * @return
	 * @throws Exception
	 */
	public static PushService getPushService() throws Exception {
		return new PushServiceImpl();
	}

	/**
	 * 스트리밍 서비스를 반환한다.
	 * 
	 * @return
	 * @throws Exception
	 */
	public static StreamingService getStreamingService() throws Exception {
		return new StreamingServiceImpl();
	}

}
