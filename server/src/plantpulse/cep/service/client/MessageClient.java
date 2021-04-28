package plantpulse.cep.service.client;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.json.JSONObject;
import org.springframework.stereotype.Service;

import plantpulse.cep.domain.Trigger;
import plantpulse.cep.engine.CEPEngineManager;
import plantpulse.cep.engine.messaging.publisher.kafka.KAFKAPublisher;
import plantpulse.cep.engine.messaging.publisher.mqtt.MQTTPublisher;
import plantpulse.cep.engine.messaging.publisher.stomp.STOMPPublisher;

@Service
public class MessageClient {

	private static final Log log = LogFactory.getLog(MessageClient.class);
	
	
	public KAFKAPublisher toKAFKA() throws Exception{
		return CEPEngineManager.getInstance().getMessageBroker().getKAFKAPublisher();
	}
	
	public MQTTPublisher toMQTT() throws Exception{
		return CEPEngineManager.getInstance().getMessageBroker().getMQTTPublisher();
	}
	
	public STOMPPublisher toSTOMP() throws Exception{
		return CEPEngineManager.getInstance().getMessageBroker().getSTOMPPublisher();
	}

	
}