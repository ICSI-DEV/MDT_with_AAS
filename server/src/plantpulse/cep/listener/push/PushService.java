package plantpulse.cep.listener.push;

/**
 * PushService
 * 
 * @author leesangboo
 *
 */
public interface PushService {

	/**
	 * 푸쉬 클라이언트를 반환한다.
	 * 
	 * @param uri
	 * @return
	 * @throws PushException
	 */
	public PushClient getPushClient(String path) throws Exception;

}
