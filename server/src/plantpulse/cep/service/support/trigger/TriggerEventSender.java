package plantpulse.cep.service.support.trigger;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.json.JSONObject;

import plantpulse.cep.domain.Trigger;
import plantpulse.cep.engine.CEPEngineManager;

public class TriggerEventSender {
	
	
	private static final Log log = LogFactory.getLog(TriggerListener.class);
	
	/**
	 * 
	 * @param destination
	 * @param data
	 * @throws Exception
	 */
	public void sendTrigger(Trigger trigger, long timestamp, JSONObject data) throws Exception {
	
		//
		try {

			//
			long start = System.currentTimeMillis();
			
			if (trigger.getMq_protocol().equals("MQTT")) {
				CEPEngineManager.getInstance().getMessageBroker()
				.getMQTTPublisher().send(trigger.getMq_destination(), data.toString());
			} else if (trigger.getMq_protocol().equals("STOMP")) {
				CEPEngineManager.getInstance().getMessageBroker()
				.getSTOMPPublisher().send(trigger.getMq_destination(), data.toString());
			} else if (trigger.getMq_protocol().equals("KAFKA")) {
				CEPEngineManager.getInstance().getMessageBroker()
				.getKAFKAPublisher().send(trigger.getMq_destination(), "TRIGGER", data.toString());
			} else {
				throw new Exception("Unsupport mq protocol = [" + trigger.getMq_protocol() + "]");
			}
			
			long end = System.currentTimeMillis() - start;

			log.debug("MQ  SEND TIME === " + end);

			//
		} catch (Exception ex) {
			log.error("MQ send error : destination=[" + trigger.getMq_destination() + "] : " + ex.getMessage(), ex);
			throw ex;
		} finally {
		}
	}


}
