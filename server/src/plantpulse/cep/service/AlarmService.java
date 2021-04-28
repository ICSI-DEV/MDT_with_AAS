package plantpulse.cep.service;

import java.util.Date;
import java.util.List;
import java.util.Map;

import org.json.JSONArray;
import org.json.JSONObject;

import plantpulse.cep.dao.AlarmDAO;
import plantpulse.cep.domain.Alarm;

public class AlarmService {

	private AlarmDAO dao = new AlarmDAO();

	
	public Alarm getAlarm(long alarm_seq, String insert_user_id) throws Exception {
		return dao.getAlarm(alarm_seq, insert_user_id);
	}

	public List<Alarm> getAlarmList(String insert_user_id) throws Exception {
		return dao.getAlarmList(insert_user_id);
	}

	public long getAlarmCount(Map<String, Object> params) throws Exception {
		return dao.getAlarmCount(params);
	}

	public long getAlarmCountByInsertUserId(String insert_user_id) throws Exception {
		return dao.getAlarmCountByInsertUserId(insert_user_id);
	}

	public JSONObject getAlarmTimestampListByTagId(String alarm_date_from, String alarm_date_to, String tag_id) throws Exception {
		return dao.getAlarmTimestampListByTagId(alarm_date_from, alarm_date_to, tag_id);
	}

	public JSONArray getAlarmEventDrops(String alarm_date_from, String alarm_date_to, String tag_id) throws Exception {
		return dao.getAlarmEventDrops(alarm_date_from, alarm_date_to, tag_id);
	}

	public List<Map<String, Object>> getAlarmPage(Map<String, Object> params, String limit, boolean use_location) throws Exception {
		return dao.getAlarmPage(params, limit, use_location);
	};
	
	public List<Map<String, Object>> getAlarmFlag(Map<String, Object> params, String limit) throws Exception {
		return dao.getAlarmFlag(params, limit);
	};
	
	public List<Map<String, Object>> getAlarmFlagByTags( String[] tag_ids, Map<String, Object> params, String limit) throws Exception {
		return dao.getAlarmFlag(params, limit);
	};
	
	public List<Map<String, Object>> getAlarmRank(Map<String, Object> params) throws Exception {
		return dao.getAlarmRank(params);
	};

	public List<Map<String, Object>> getAlarmRankByOPC(Map<String, Object> params) throws Exception {
		return dao.getAlarmRankByOPC(params);
	};

	public List<Map<String, Object>> getAlarmRankByAsset(Map<String, Object> params) throws Exception {
		return dao.getAlarmRankByAsset(params);
	};

	public List<Map<String, Object>> getRecentAlarmData(String insert_user_id) throws Exception {
		return dao.getRecentAlarmData(insert_user_id);
	}

	public int getTodayAlarmTotalCount(String insert_user_id) throws Exception {
		return dao.getTodayAlarmTotalCount(insert_user_id);
	}

	public List<Map<String, Object>> getTodayAlarmData(String insert_user_id) throws Exception {
		return dao.getTodayAlarmData(insert_user_id);
	}

	public int getAlarmCount(String insert_user_id) throws Exception {
		return dao.getAlarmCount(insert_user_id);
	}

	public List<Map<String, Object>> getAlarmCountByHour(Map<String, Object> params) throws Exception {
		return dao.getAlarmCountByHour(params);
	}

	public List<Map<String, Object>> getAlarmCountPriorityByDate(Map<String, Object> params) throws Exception {
		return dao.getAlarmCountPriorityByDate(params);
	}

	public int getAlarmCountByPriority(Map<String, Object> params, String priority) throws Exception {
		return dao.getAlarmCountByPriority(params, priority);
	}

	public List<Map<String, Object>> getAlarmCountPriorityByHour(Map<String, Object> params) throws Exception {
		return dao.getAlarmCountPriorityByHour(params);
	}

	public int getAlarmCountByUnread(String insert_user_id) throws Exception {
		return dao.getAlarmCountByUnread(insert_user_id);
	}

	public int getAlarmCountByPriority(String insert_user_id, String priority) throws Exception {
		return dao.getAlarmCountByPriority(insert_user_id, priority);
	}

	public int getAlarmCountByPriority(String alarm_date_from, String alarm_date_to, String insert_user_id, String priority) throws Exception {
		return dao.getAlarmCountByPriority(alarm_date_from, alarm_date_to, insert_user_id, priority);
	}

	public int getAlarmConfigCountByDate(String alarm_config_id, String insert_user_id, Date before_date, String priority) throws Exception {
		return dao.getAlarmConfigCountByDate(alarm_config_id, insert_user_id, before_date, priority);
	}

	public Map<String, Object> getTagAlarmAggregation(String insert_user_id, String tag_id) throws Exception {
		return dao.getTagAlarmAggregation(insert_user_id, tag_id);
	}
	
	public Map<String, Object> getTagAlarmStatistics(String insert_user_id, String tag_id, String conditionQuery) throws Exception {
		return dao.getTagAlarmStatistics(insert_user_id, tag_id, conditionQuery);
	}
	
	public void saveAlarm(Alarm alarm) throws Exception {
		dao.saveAlarm(alarm);
	}

	public void updateReadAllAlarm(String insert_user_id) throws Exception {
		dao.updateReadAllAlarm(insert_user_id);
	}

	public void deleteAlarm(long alarm_seq, String insert_user_id) throws Exception {
		dao.deleteAlarm(alarm_seq, insert_user_id);
	}

	public void deleteAllAlarm(String insert_user_id) throws Exception {
		dao.deleteAllAlarm(insert_user_id);
	}
	
	public void updateReadAlarm(long alarm_seq, String insert_user_id) throws Exception {
		dao.updateReadAlarm(alarm_seq, insert_user_id);
	}

	public void clear() throws Exception {
		dao.clear();
	}

}
