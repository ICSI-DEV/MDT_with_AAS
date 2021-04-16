package plantpulse.cep.service;

import java.util.List;
import java.util.Map;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.json.JSONArray;
import org.json.JSONObject;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import plantpulse.cep.service.client.StorageClient;
import plantpulse.domain.Site;
import plantpulse.domain.Tag;

/**
 * StorageService
 * 
 * TODO 메소드 네이밍 리팩토링 필요
 * @author lsb
 *
 */
@Service
public class StorageService {

	private static final Log log = LogFactory.getLog(StorageService.class);

	@Autowired
	private StorageClient storage_client;
	
	
	/**
	 * JSONArray
	 * 
	 * @param site
	 * @param tag_id
	 * @param data_type
	 * @param from
	 * @param to
	 * @param aggregation
	 * @return
	 * @throws Exception
	 */
	public List<Map<String, Object>> list(Tag tag, String from, String to, int limit) throws Exception {
		List<Map<String, Object>> list = storage_client.forSelect().selectPointList(tag, from, to, limit, null);
		return list;
	}

	/**
	 * JSONArray
	 * 
	 * @param site
	 * @param tag_id
	 * @param data_type
	 * @param from
	 * @param to
	 * @param aggregation
	 * @return
	 * @throws Exception
	 */
	public List<Map<String, Object>> list(Tag tag, String from, String to, int limit, String condition) throws Exception {
		List<Map<String, Object>> list = storage_client.forSelect().selectPointList(tag, from, to, limit, condition);
		return list;
	}
	
	/**
	 * JSONArray
	 * 
	 * @param site
	 * @param tag_id
	 * @param data_type
	 * @param from
	 * @param to
	 * @param aggregation
	 * @return
	 * @throws Exception
	 */
	public JSONArray search(Tag tag, String from, String to, String sampling, String term) throws Exception {
		JSONArray array = storage_client.forSelect().selectPointValueList(tag, from, to, sampling, term);
		return array;
	}

	/**
	 * JSONArray
	 * 
	 * @param site
	 * @param tag_id
	 * @param data_type
	 * @param from
	 * @param to
	 * @param aggregation
	 * @return
	 * @throws Exception
	 */
	public JSONArray search(Tag tag, String from, String to, String sampling, String term, int limit) throws Exception {
		JSONArray array = storage_client.forSelect().selectPointValueList(tag, from, to, sampling, term, limit);
		return array;
	}
	
	/**
	 * JSONArray
	 * 
	 * @param site
	 * @param tag_id
	 * @param data_type
	 * @param from
	 * @param to
	 * @param aggregation
	 * @return
	 * @throws Exception
	 */
	public JSONArray search(Tag tag, String from, String to) throws Exception {
		JSONArray array = storage_client.forSelect().selectPointValueList(tag, from, to, null, null);
		return array;
	}
	
	

	/**
	 * 
	 * @param site
	 * @param tag
	 * @param from
	 * @param to
	 * @param aggregation
	 * @param term
	 * @return
	 * @throws Exception
	 */
	public JSONObject aggregate(Tag tag, String from, String to, String sampling, String term) throws Exception {
		JSONObject object = storage_client.forSelect().selectPointAggregatation(tag, from, to, sampling, term);
		return object;
	}

	/**
	 * 
	 * @param site_id
	 * @param from
	 * @param to
	 * @param term
	 * @param limit
	 * @return
	 * @throws Exception
	 */
	public JSONArray snapshot(String site_id, String from, String to, String term, int limit) throws Exception {
		JSONArray array = storage_client.forSelect().selectPointSnapshot(site_id, from, to, term,  limit);
		return array;
	}

	/**
	 * 
	 * @param site
	 * @return
	 * @throws Exception
	 */
	public long getTotalDataCount() throws Exception {
		return storage_client.forSelect().countPointTotal();
	}

	/**
	 * 
	 * @param site
	 * @return
	 * @throws Exception
	 */
	public long getTotalDataCountBySite(Site site) throws Exception {
		return storage_client.forSelect().countPointTotalBySite(site);
	}

}
