package plantpulse.cep.service.support.tag;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import plantpulse.domain.Tag;

/**
 * TagAndLocationCacheTask
 * @author leesa
 *
 */
public class TagAndLocationCacheTask implements Runnable {
	
	private static final Log log = LogFactory.getLog(TagAndLocationCacheTask.class);
	
	private Tag tag;
	
	public TagAndLocationCacheTask(Tag tag) {
		this.tag = tag;
	}

	@Override
	public void run() {
		
		//1. 태그 캐싱
		TagCacheFactory.getInstance().putTag(tag.getTag_id(), tag);
		
		//2. 태그 로케이션 캐싱
		TagLocation location = new TagLocation();
		String location_string = location.getLocation(tag.getTag_id(), true);
		TagLocationFactory.getInstance().putTagLocation(tag.getTag_id(), location_string);
		log.debug("Tag location load : " + tag.getTag_id());		
	}

	
}