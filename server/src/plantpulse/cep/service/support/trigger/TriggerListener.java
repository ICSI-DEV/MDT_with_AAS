package plantpulse.cep.service.support.trigger;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.json.JSONObject;

import com.espertech.esper.client.EventBean;
import com.espertech.esper.client.UpdateListener;

import plantpulse.cep.domain.Trigger;
import plantpulse.cep.engine.CEPEngineManager;
import plantpulse.cep.engine.logging.EngineLogger;
import plantpulse.cep.service.client.MessageClient;
import plantpulse.cep.service.client.StorageClient;

/**
 * TriggerListener
 * 
 * @author lsb
 * 
 */
public class TriggerListener implements UpdateListener {

	private static final Log log = LogFactory.getLog(TriggerListener.class);

	private Trigger trigger;
	
	private TriggerEventSender sender = new TriggerEventSender();

	public TriggerListener(Trigger trigger) {
		this.trigger = trigger;
	}

	@Override
	public void update(EventBean[] newEvents, EventBean[] arg1) {

		try {

			if (newEvents != null && newEvents.length > 0) {

				for (int i = 0; i < newEvents.length; i++) {
					EventBean event = newEvents[i];
					//
					if (event != null) {
						String json_text = CEPEngineManager.getInstance().getProvider().getEPRuntime().getEventRenderer().renderJSON("json", event);
						JSONObject json = JSONObject.fromObject(JSONObject.fromObject(json_text).getJSONObject("json").toString());
						log.debug("Trigger event underlying : json=" + json.toString());

						long timestamp = System.currentTimeMillis();
						//
						if (trigger.isUse_mq()) {
							try {
								
								sender.sendTrigger(trigger, timestamp, json);
							} catch (Exception ex) {
								log.error("Trigger mq streaming failed : " + trigger.getTrigger_id() , ex);
							}
						}
						//
						if (trigger.isUse_storage()) {
							try {
								StorageClient client = new StorageClient();
								client.forInsert().insertTrigger(trigger, timestamp, json);
							} catch (Exception ex) {
								log.error("Trigger storage save failed : " + trigger.getTrigger_id() , ex);
							}
						}
						//
					}
				}
			}
			
			log.debug("Trigger execute successfully : trigger_id=[" + trigger.getTrigger_id() + "]");

		} catch (Exception ex) {
			EngineLogger.error("트리거[" + trigger.getTrigger_id() + "] 업데이트 리스너에서 오류가 발생하였습니다.");
			log.error("Trigger update listener failed : " + trigger.getTrigger_id() , ex);
		}
	}

}
