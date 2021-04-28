package plantpulse.cep.engine.lookups;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Esper lookup methods
 * 
 * select assetId, location, x_coord, y_coord from AssetMoveEvent as asset,
       method:com.mypackage.MyLookupLib.getAssetHistory(assetId, assetCode) as history
 * @author lsb
 *
 */
public class DatabaseLookup {
	
	 // For each column in a row, provide the property name and type
	  //
	  @SuppressWarnings("rawtypes")
	  public static Map<String, Class> getAssetHistoryMetadata() {
		Map<String, Class> propertyNames = new HashMap<String, Class>();
	    propertyNames.put("location", String.class);
	    propertyNames.put("x_coord", Integer.class);
	    propertyNames.put("y_coord", Integer.class);
	    return propertyNames;
	  }

	  // Lookup rows based on assetId and assetCode
	  // 
	  public static List<Map<String, Object>> getAssetHistory(String assetId, String assetCode) {
		  List<Map<String, Object>> rows = new ArrayList<Map<String, Object>>();	// this sample returns rows
	    for (int i = 0; i < 2; i++) {
	    	Map<String,Object> row = new HashMap<String,Object>();
	    	row.put("location", "somevalue");
	        row.put("x_coord", 100);
	        rows.add(row);
	   	 }
	    return rows;
	  }
}