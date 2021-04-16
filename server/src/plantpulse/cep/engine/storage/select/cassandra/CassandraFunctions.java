package plantpulse.cep.engine.storage.select.cassandra;

import org.apache.commons.lang.StringUtils;

public class CassandraFunctions {

	    public static Double to_double(String input) {
	    	if(StringUtils.isNotEmpty(input)){
	    		return Double.parseDouble(input);
	    	}else{
	    		return null;
	    	}
	    }

}