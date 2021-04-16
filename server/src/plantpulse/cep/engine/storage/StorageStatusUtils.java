package plantpulse.cep.engine.storage;

import plantpulse.cep.engine.CEPEngineManager;


/**
 * StorageStatusUtils
 * @author leesa
 *
 */
public class StorageStatusUtils {
	
	/**
	 * 현재 스토리지 상태 반환
	 * @return
	 */
	public static StorageStatus now() {
		return CEPEngineManager.getInstance().getStorage_processor().getMonitor().getStatus();
	}

}
