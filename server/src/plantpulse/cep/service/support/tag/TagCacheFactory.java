package plantpulse.cep.service.support.tag;

import java.util.Map;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.eclipse.collections.impl.map.mutable.ConcurrentHashMap;

import plantpulse.cep.dao.TagDAO;
import plantpulse.domain.Tag;

/**
 * TagCacheFactory
 * @author leesa
 *
 */
public class TagCacheFactory  {
	
	private static final Log log = LogFactory.getLog(TagCacheFactory.class);
	
	private static class TagCacheFactoryHolder {
		static TagCacheFactory instance = new TagCacheFactory();
	}

	public static TagCacheFactory getInstance() {
		return TagCacheFactoryHolder.instance;
	}
	
	private Map<String, Tag> tag_map = new ConcurrentHashMap<String, Tag>();
	
	public void putTag(String tag_id, Tag tag){
		tag_map.put(tag_id, tag);
	}
	
	public void updateTag(String tag_id, Tag tag){
		tag_map.put(tag_id, tag);
		log.info("Tag cache updated : tag_id=[" + tag_id + "], data=[" + tag.toString() + "]");
	}
	
	public Tag getTag(String tag_id){
		if(tag_map.containsKey(tag_id)) {
			return tag_map.get(tag_id);
		}else {
			try {
				TagDAO dao = new TagDAO();
				Tag tag = dao.selectTag(tag_id);
				if(tag == null) log.warn("Tag is NULL. tag_id=[" + tag_id + "]");
				this.putTag(tag_id, tag);
				return tag;
			} catch (Exception e) {
				log.error("Tag object getting error in cache : " + e.getMessage(), e);
				return null;
			}
		}
	};
	

};