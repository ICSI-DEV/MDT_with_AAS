package plantpulse.server.mvc.util;

import java.io.UnsupportedEncodingException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;

import org.json.JSONArray;
import org.json.JSONException;
import org.json.JSONObject;

/**
 * JSONSortUtils
 * 
 * @author leesa
 *
 */
public class JSONSortUtils {
	
	/**
	 * JSONSortUtils
	 * 
	 * @param array
	 * @param json_key
	 * @return
	 * @throws UnsupportedEncodingException
	 */
public static JSONArray sortArrayByInJSONKey(JSONArray array, final String json_key) throws UnsupportedEncodingException {
		
	    JSONArray jsonArr = array;
	    JSONArray sortedJsonArray = new JSONArray();

	    List<JSONObject> jsonValues = new ArrayList<JSONObject>();
	    for (int i = 0; i < jsonArr.size(); i++) {
	        jsonValues.add(jsonArr.getJSONObject(i));
	    }
	    Collections.sort( jsonValues, new Comparator<JSONObject>() {
	        private final String KEY_NAME =  json_key;

	        @Override
	        public int compare(JSONObject a, JSONObject b) {
	        	Long valA = new Long(0);
	            Long valB = new Long(0);

	            try {
	                valA = a.getLong(KEY_NAME);
	                valB = b.getLong(KEY_NAME);
	            } 
	            catch (JSONException e) {
	                //do something
	            }

	            return valA.compareTo(valB);
	            //if you want to change the sort order, simply use the following:
	            //return -valA.compareTo(valB);
	        }
	    });

	    for (int i = 0; i < jsonArr.size(); i++) {
	        sortedJsonArray.add(jsonValues.get(i));
	    }
	    
	    return sortedJsonArray;
	};
	
	
public static JSONArray sortArrayByInArrayIndex(JSONArray array, final int array_index) throws UnsupportedEncodingException {
		
	    JSONArray jsonArr = array;
	    JSONArray sortedJsonArray = new JSONArray();

	    List<JSONArray> jsonValues = new ArrayList<JSONArray>();
	    for (int i = 0; i < jsonArr.size(); i++) {
	        jsonValues.add(jsonArr.getJSONArray(i));
	    }
	    Collections.sort( jsonValues, new Comparator<JSONArray>() {
	        private final int KEY_INDEX =  array_index;

	        @Override
	        public int compare(JSONArray a, JSONArray b) {
	        	Long valA = new Long(0);
	            Long valB = new Long(0);

	            try {
	                valA = a.getLong(KEY_INDEX);
	                valB = b.getLong(KEY_INDEX);
	            } 
	            catch (JSONException e) {
	                //do something
	            }

	            return valA.compareTo(valB);
	            //if you want to change the sort order, simply use the following:
	            //return -valA.compareTo(valB);
	        }
	    });

	    for (int i = 0; i < jsonArr.size(); i++) {
	        sortedJsonArray.add(jsonValues.get(i));
	    }
	    
	    return sortedJsonArray;
	};
	

}
