package plantpulse.cep.service.client;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.springframework.stereotype.Service;

import com.espertech.esper.client.EPServiceProvider;

import plantpulse.cep.engine.CEPEngineManager;

@Service
public class CEPClient {

	private static final Log log = LogFactory.getLog(MessageClient.class);
	
	public EPServiceProvider  getCEP() throws Exception{
		return CEPEngineManager.getInstance().getProvider();
	}
	
}