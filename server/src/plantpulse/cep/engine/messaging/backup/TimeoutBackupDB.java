package plantpulse.cep.engine.messaging.backup;

import java.io.File;

/**
 * TimeoutBackupDB
 * 
 * @author leesa
 *
 */
public interface TimeoutBackupDB {

	public void init();

	public Object getDb();

	public File getFile();

	public void add(String in);
	
	public String poll();

	public void close();

	public String getType();
	
	public int size();
	
	public long diskSize();

}
