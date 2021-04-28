package plantpulse.cep.listener.push;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

/**
 * PushServiceImpl
 * 
 * @author leesangboo
 *
 */
public class PushServiceImpl implements PushService {

	private static final Log log = LogFactory.getLog(PushServiceImpl.class);

	/*
	 * (non-Javadoc)
	 * 
	 * @see org.realdisplay.cep.engine.listener.service.push.PushService#
	 * getPushClient(java.lang.String)
	 */
	@Override
	public PushClient getPushClient(String path) throws Exception {
		try {
			return PushClientFactory.createPushClient(path);
		} catch (Exception ex) {
			log.error(ex.getMessage(), ex);
			throw new Exception(ex.getMessage(), ex);
		}
	}

}
