package plantpulse.cep.service.support.tag;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;


/**
 * TagLocationFactory
 * 
 * @author leesa
 *
 */
public class TagLocationFactory {
	
	private static final Log log = LogFactory.getLog(TagLocationFactory.class);
	
	private static class TagLocationFactoryHolder {
		static TagLocationFactory instance = new TagLocationFactory();
	}

	public static TagLocationFactory getInstance() {
		return TagLocationFactoryHolder.instance;
	}
	
	private Map<String, String> location_map = new ConcurrentHashMap<String, String>();
	
	public void putTagLocation(String tag_id, String location){
		location_map.put(tag_id, location);
	}
	
	public void updateTagLocation(String tag_id, String location){
		location_map.put(tag_id, location);
		log.debug("Tag location updated : tag_id=[" + tag_id + "], location=["  + location + "]" );
	}
	
	public String getTagLocation(String tag_id){
		if(location_map.containsKey(tag_id)) {
		 return location_map.get(tag_id);
		}else {
			return "...";
		}
	};
	
	public String getTagLocationWithoutTagName(String tag_id){
		if(location_map.containsKey(tag_id)) {
			String location = location_map.get(tag_id);
		    return location.substring(0, location.lastIndexOf("/"));
		}else {
			return "...";
		}
	}

};