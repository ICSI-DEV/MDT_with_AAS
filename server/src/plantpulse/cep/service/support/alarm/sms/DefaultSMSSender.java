package plantpulse.cep.service.support.alarm.sms;

import java.util.Map;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;


/**
 * DefaultSMSSender
 * @author lsb
 *
 */
public class DefaultSMSSender implements SMSSender   {
	
	private static final Log log = LogFactory.getLog(DefaultSMSSender.class);

	@Override
	public void send(String phone, Map<String, Object> data) throws Exception {
		log.info("SMS Sended : " + phone + ", data=" + data.toString());
		
	}


}
