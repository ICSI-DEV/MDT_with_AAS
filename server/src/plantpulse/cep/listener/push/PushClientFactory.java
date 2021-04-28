package plantpulse.cep.listener.push;

/**
 * PushClientFactory
 * 
 * @author leesangboo
 *
 */
public class PushClientFactory {

	// private static Log log = LogFactory.getLog(PushClientFactory.class);

	public static PushClient createPushClient(String uri) throws Exception {
		return new PushClient(uri);
	}

}