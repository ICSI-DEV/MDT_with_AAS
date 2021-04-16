package plantpulse.cep.service.support.alarm.sms;

import java.util.Map;

public interface SMSSender {
	
	public void send(String phone, Map<String,Object> data) throws Exception;

}
