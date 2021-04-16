package plantpulse.cep.engine.storage;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

/**
 * StorageProcessorFactory
 * @author leesa
 *
 */
public class StorageProcessorFactory {

	private static final Log log = LogFactory.getLog(StorageProcessorFactory.class);

	private static class StorageProcessorFactoryHolder {
		static StorageProcessorFactory instance = new StorageProcessorFactory();
	}

	public static StorageProcessorFactory getInstance() {
		return StorageProcessorFactoryHolder.instance;
	};
	
	private StorageProcessor storage_prcoessor;


	/**
	 * 초기화
	 */
	public void init() {
		storage_prcoessor = new BaseStorageProcessor();
		storage_prcoessor.init();
		log.info("Storage processor inited : class_name=[" + storage_prcoessor.getClass().getName() + "]");
    }

	/**
	 * 
	 * @return
	 */
	public StorageProcessor getStorageProcessor() {
		return storage_prcoessor;
	}
	

}
