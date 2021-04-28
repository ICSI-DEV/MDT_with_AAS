package plantpulse.cep.service;


import java.util.List;
import java.util.Map;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.json.JSONArray;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import plantpulse.cep.engine.utils.TimestampUtils;
import plantpulse.cep.service.client.StorageClient;
import plantpulse.domain.Asset;

@Service
public class TimelineService {

	private static final Log log = LogFactory.getLog(TimelineService.class);

	@Autowired
	private StorageClient storage_client;

	public JSONArray selectTimeline(Asset asset, String from, String to, int limit) throws Exception {
		try {
			List<Map<String, Object>> list =  storage_client.forSelect().selectAssetTimeline(asset,  from,  to,  limit);
			return JSONArray.fromObject(list);
		} catch (Exception e) {
			log.error("Select timeline data list error : " + e.getMessage(), e);
			throw e;
		}
	}
	
	public JSONArray selectAssetAlarmList(Asset asset, String from, String to, int limit) throws Exception {
		try {
			List<Map<String, Object>> list =  storage_client.forSelect().selectAssetAlarm(asset,  from,  to,  limit);
			return JSONArray.fromObject(list);
		} catch (Exception e) {
			log.error("Select timeline data list error : " + e.getMessage(), e);
			throw e;
		}
	}
	
	
	public JSONArray selectAssetDataList(Asset asset, String from, String to, int limit) throws Exception {
		try {
			int date = TimestampUtils.timestampToYYYYMMDD(System.currentTimeMillis());
			List<Map<String, Object>> list =  storage_client.forSelect().selectAssetData(asset, date,  from,  to,  limit);
			return JSONArray.fromObject(list);
		} catch (Exception e) {
			log.error("Select timeline data list error : " + e.getMessage(), e);
			throw e;
		}
	}

}
