package plantpulse.cep.engine.storage.bucket;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import plantpulse.cep.service.support.tag.TagCacheManager;

/**
 * Bucket
 * @author leesa
 *
 */
public class Bucket {
	
	private static final Log log = LogFactory.getLog(Bucket.class);
	
	/**
	 * 태그의 데이터베이스 버켓 값을 반환한다.
	 * @param tag_id
	 * @return
	 */
	public static int get(String tag_id) {
		try {
			TagCacheManager manager = new TagCacheManager();
			return manager.getTag(tag_id).getDb_bucket();
		} catch (Exception e) {
			log.error("Database bucket value find error, return 0 : " + e.getMessage(), e);
			return 0;
		}
	}

}
