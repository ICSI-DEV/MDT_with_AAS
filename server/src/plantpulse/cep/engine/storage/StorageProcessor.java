package plantpulse.cep.engine.storage;

import java.util.List;

import plantpulse.cep.engine.storage.buffer.DBDataPointBuffer;
import plantpulse.cep.engine.storage.insert.BatchTimer;
/**
 * StorageProcessor
 * 
 * @author leesa
 *
 */
public interface StorageProcessor {

	public void init();
	
	public DBDataPointBuffer getBuffer();
	
	public List<BatchTimer> getBatchs() ;
	
	public StorageMonitor getMonitor();
	
	public void stop();

}
