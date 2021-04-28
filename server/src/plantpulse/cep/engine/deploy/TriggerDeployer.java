package plantpulse.cep.engine.deploy;

import java.util.List;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import plantpulse.cep.domain.Trigger;
import plantpulse.cep.engine.logging.EngineLogger;
import plantpulse.cep.service.TriggerService;

public class TriggerDeployer implements Deployer {

	private static final Log log = LogFactory.getLog(TriggerDeployer.class);

	public void deploy() {
		TriggerService trigger_service = new TriggerService();
		try {
			List<Trigger> list = trigger_service.getTriggerList();
			for (int i = 0; list != null && i < list.size(); i++) {
				Trigger trigger = list.get(i);
				trigger_service.deployTrigger(trigger);
			}
		} catch (Exception ex) {
			EngineLogger.error("트리거를 배치하는 도중 오류가 발생하였습니다 : 에러=" + ex.getMessage());
			log.warn("Trigger deploy error : " + ex.getMessage(), ex);
		}
	}

}
