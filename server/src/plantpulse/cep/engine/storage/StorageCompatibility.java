package plantpulse.cep.engine.storage;

import plantpulse.cep.engine.properties.PropertiesLoader;

/**
 * StorageCompatibility
 * 
 * @author leesa
 *
 */
public class StorageCompatibility {
	
	public static boolean isCassandra() {
		return PropertiesLoader.getStorage_properties().getProperty("storage.db_type", "CASSANDRA").equals("CASSANDRA");
	}
	
     public static boolean isScylla() {
    	return PropertiesLoader.getStorage_properties().getProperty("storage.db_type", "CASSANDRA").equals("SCYLLADB");
	}

	 public static String compatSSTableComacpctionClassName() {
		 if(isCassandra()) return "class";
		 if(isScylla()) return "sstable_compression";
		 return "class";
	};
	

}
