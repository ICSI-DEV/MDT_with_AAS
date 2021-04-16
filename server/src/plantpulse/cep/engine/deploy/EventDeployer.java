package plantpulse.cep.engine.deploy;

import java.util.List;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import plantpulse.cep.domain.Event;
import plantpulse.cep.engine.logging.EngineLogger;
import plantpulse.cep.service.CEPEventService;
import plantpulse.cep.service.CEPService;

public class EventDeployer implements Deployer  {

	private static final Log log = LogFactory.getLog(EventDeployer.class);

	public void deploy() {
		CEPService cep_service = new CEPService();
		try {
			List<Event> list = cep_service.getEventList();
			for (int i = 0; list != null && i < list.size(); i++) {
				Event event = list.get(i);
				CEPEventService cep_event_service = new CEPEventService();
				cep_event_service.deployEvent(event);
			}
		} catch (Exception ex) {
			EngineLogger.error("이벤트를 배치하는 도중 오류가 발생하였습니다 : 에러=" + ex.getMessage());
			log.warn("Event deploy error : " + ex.getMessage(), ex);
		}
	}

}
