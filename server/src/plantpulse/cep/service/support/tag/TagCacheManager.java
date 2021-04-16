package plantpulse.cep.service.support.tag;

import plantpulse.domain.Tag;

/**
 * TagCacheManager
 * @author leesa
 *
 */
public class TagCacheManager {
	
	public void updateCache(Tag tag) throws Exception {
		//1. 태그 캐싱
		TagCacheFactory.getInstance().updateTag(tag.getTag_id(), tag);
		//2. 태그 위치 변경 적용
		TagLocation location = new TagLocation();
		String location_string = location.getLocation(tag.getTag_id(), true);
		TagLocationFactory.getInstance().updateTagLocation(tag.getTag_id(), location_string);
	}

	public Tag getTag(String tag_id) throws Exception {
		return TagCacheFactory.getInstance().getTag(tag_id);
	}
	
	public String getTagLocation(String tag_id) throws Exception {
		return TagLocationFactory.getInstance().getTagLocation(tag_id);
	}
	
	public String getTagLocationWithoutTagName(String tag_id) throws Exception {
		return TagLocationFactory.getInstance().getTagLocationWithoutTagName(tag_id);
	}

}
