package plantpulse.cep.service;

import java.util.ArrayList;
import java.util.List;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import com.espertech.esper.client.EPServiceProvider;
import com.espertech.esper.client.EPStatement;

import plantpulse.cep.dao.TriggerDAO;
import plantpulse.cep.domain.Trigger;
import plantpulse.cep.engine.CEPEngineManager;
import plantpulse.cep.service.support.trigger.TriggerListener;

public class TriggerService {

	private static final Log log = LogFactory.getLog(TriggerService.class);

	private TriggerDAO dao = new TriggerDAO();

	/**
	 * 
	 * @return
	 * @throws Exception
	 */
	public List<Trigger> getTriggerList() throws Exception {
		return dao.getTriggerList();
	}

	/**
	 * 
	 * @param trigger_id
	 * @return
	 * @throws Exception
	 */
	public Trigger getTrigger(String trigger_id) throws Exception {
		return dao.getTrigger(trigger_id);
	}

	/**
	 * 
	 * @param event
	 * @throws Exception
	 */
	public void saveTrigger(Trigger event) throws Exception {
		dao.saveTrigger(event);
	}

	/**
	 * 
	 * @param event
	 * @throws Exception
	 */
	public void updateTrigger(Trigger event) throws Exception {
		dao.updateTrigger(event);
	}

	/**
	 * 
	 * @param trigger_id
	 * @throws Exception
	 */
	public void deleteTrigger(String trigger_id) throws Exception {
		dao.deleteTrigger(trigger_id);
	}

	/**
	 * setEventStatus
	 * 
	 * @param event_list
	 * @return
	 */
	public List<Trigger> setTriggerStatus(List<Trigger> trigger_list) {
		EPServiceProvider provider = CEPEngineManager.getInstance().getProvider();
		List<Trigger> list = new ArrayList<Trigger>();
		for (int i = 0; trigger_list != null && i < trigger_list.size(); i++) {
			//
			Trigger trigger = trigger_list.get(i);
			EPStatement stmt = provider.getEPAdministrator().getStatement(trigger.getTrigger_id());
			try {

				if (stmt != null && stmt.isStarted()) {
					trigger.setStatus("start");
				} else if (stmt != null && stmt.isStopped()) {
					trigger.setStatus("stop");
				} else if (stmt != null && stmt.isDestroyed()) {
					trigger.setStatus("destroyed");
				} else if (stmt == null) {
					trigger.setStatus("undeployed");
				}
			} catch (Exception e) {
				//
				trigger.setStatus("error");
			}
			list.add(trigger);
		}
		return list;
	}

	/**
	 * 
	 * @param trigger
	 */
	public void deployTrigger(Trigger trigger) {
		EPServiceProvider provider = CEPEngineManager.getInstance().getProvider();
		EPStatement stmt = null;
		stmt = provider.getEPAdministrator().createEPL(trigger.getEpl(), trigger.getTrigger_id());
		stmt.addListener(new TriggerListener(trigger));
		log.info("Deployed trigger = [" + trigger.getTrigger_id() + "]");

	}

	/**
	 * 
	 * @param trigger
	 */
	public void undeployTrigger(Trigger trigger) {
		EPServiceProvider provider = CEPEngineManager.getInstance().getProvider();
		if (provider.getEPAdministrator().getStatement(trigger.getTrigger_id()) != null) {
			provider.getEPAdministrator().getStatement(trigger.getTrigger_id()).destroy();
		} else {
			log.warn("Undeployed trigger = [" + trigger.getTrigger_id() + "]");
		}
		//
	}

	/**
	 * 
	 * @param trigger
	 */
	public void redeployTrigger(Trigger trigger) {
		undeployTrigger(trigger);
		deployTrigger(trigger);
		log.info("Redeployed trigger = [" + trigger.getTrigger_id() + "]");
	}
}
