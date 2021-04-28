package plantpulse.cep.service.support.tag;

import java.util.List;
import java.util.Map;
import java.util.concurrent.CopyOnWriteArrayList;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;


/**
 * TagLocationFactory
 * 
 * @author leesa
 *
 */
public class TagListFactory {
	
	private static final Log log = LogFactory.getLog(TagListFactory.class);
	
	private static class TagLocationFactoryHolder {
		static TagLocationFactory instance = new TagLocationFactory();
	}

	public static TagLocationFactory getInstance() {
		return TagLocationFactoryHolder.instance;
	}
	
	private List<Map<String, Object>>  tag_list = new CopyOnWriteArrayList<Map<String, Object>> ();
	


};