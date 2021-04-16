package plantpulse.cep.engine.messaging.backup;

import plantpulse.cep.engine.CEPEngineManager;

/**
 * TimeoutMessageBackup
 * @author leesa
 *
 */
public class TimeoutMessageBackup {
	
	public TimeoutMessageBackup() {
		
	}
	
	public  void storeTimeoutMessage(String in) {
		CEPEngineManager.getInstance().getMessageBroker().getBackup().storeTimeoutMessage(in);
	}

}
