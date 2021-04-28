package plantpulse.cep.service;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import com.espertech.esper.client.EPServiceProvider;
import com.espertech.esper.client.EventType;

import plantpulse.cep.domain.Event;
import plantpulse.cep.domain.EventAttribute;
import plantpulse.cep.engine.CEPEngineManager;
import plantpulse.cep.engine.utils.JavaTypeUtils;

public class CEPEventService {

	private static final Log log = LogFactory.getLog(CEPEventService.class);

	/**
	 * deployEvent
	 * 
	 * @param event
	 * @throws Exception
	 */
	public void deployEvent(Event event) throws Exception {
		Map<String, Object> event_map = new HashMap<String, Object>();
		event_map.put("timestamp", Long.class); // 이벤트 등록시 timestamp 필드는 필수
		event_map.put("host", String.class);
		event_map.put("agent", String.class);
		for (int i = 0; i < event.getEvent_attributes().length; i++) {
			EventAttribute attr = event.getEvent_attributes()[i];
			event_map.put(attr.getField_name(), JavaTypeUtils.getJavaType(attr.getJava_type()));
		}
		EPServiceProvider provider = CEPEngineManager.getInstance().getProvider();
		provider.getEPAdministrator().getConfiguration().addEventType(event.getEvent_name(), event_map);
		log.info("Event deployed : name=[" + event.getEvent_name() + "]");
	}

	/**
	 * undeployEvent
	 * 
	 * @param event
	 * @throws Exception
	 */
	public void undeployEvent(Event event) throws Exception {
		EPServiceProvider provider = CEPEngineManager.getInstance().getProvider();
		provider.getEPAdministrator().getConfiguration().removeEventType(event.getEvent_name(), true);
		log.info("Event undeployed : name=[" + event.getEvent_name() + "]");
	}

	/**
	 * 
	 * @param event
	 * @throws Exception
	 */
	public void redeployEvent(Event event) throws Exception {
		undeployEvent(event);
		deployEvent(event);
		log.info("Event redeployed : name=[" + event.getEvent_name() + "]");
	}

	/**
	 * setEventStatus
	 * 
	 * @param event_list
	 * @return
	 */
	public List<Event> setEventStatus(List<Event> event_list) {
		EPServiceProvider provider = CEPEngineManager.getInstance().getProvider();
		List<Event> list = new ArrayList<Event>();
		for (int i = 0; event_list != null && i < event_list.size(); i++) {
			//
			Event event = event_list.get(i);
			EventType type = provider.getEPAdministrator().getConfiguration().getEventType(event.getEvent_name());
			try {
				if (type != null) {
					event.setStatus("start");
				} else if (type == null) {
					event.setStatus("undeployed");
				}
			} catch (Exception e) {
				//
				event.setStatus("error");
			}
			list.add(event);
		}
		return list;
	}
}
