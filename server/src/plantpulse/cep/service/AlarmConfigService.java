package plantpulse.cep.service;

import java.util.ArrayList;
import java.util.List;

import org.apache.commons.lang.StringUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import plantpulse.cep.dao.AlarmConfigDAO;
import plantpulse.cep.domain.Query;
import plantpulse.cep.service.support.alarm.EQLAlarmEventListener;
import plantpulse.cep.service.support.alarm.TagBandAlarmEventListener;
import plantpulse.domain.AlarmConfig;
import plantpulse.domain.Tag;

/**
 * AlarmConfigService
 * 
 * @author leesa
 *
 */
public class AlarmConfigService {

	private static final Log log = LogFactory.getLog(AlarmConfigService.class);

	private AlarmConfigDAO dao;
	private QueryService query_service;

	public AlarmConfigService() {
		this.dao = new AlarmConfigDAO();
		this.query_service = new QueryService();
	}

	public List<AlarmConfig> getAlarmConfigListAll() throws Exception {
		return dao.getAlarmConfigListAll();
	}

	public List<AlarmConfig> getAlarmConfigListByInsertUserId(String insert_user_id) throws Exception {
		return dao.getAlarmConfigListByInsertUserId(insert_user_id);
	}
	
	public List<AlarmConfig> getAlarmConfigListByTagId(String tag_id) throws Exception {
		return dao.getAlarmConfigListByTagId(tag_id);
	}

	public AlarmConfig getAlarmConfig(String alarm_config_id) throws Exception {
		return dao.getAlarmConfig(alarm_config_id);
	}

	public void saveAlarmConfig(AlarmConfig alarm_config) throws Exception {
		dao.saveAlarmConfig(alarm_config);
	}

	public void updateAlarmConfig(AlarmConfig alarm_config) throws Exception {
		dao.updateAlarmConfig(alarm_config);
	}

	public void deleteAlarmConfig(String alarm_config_id) throws Exception {
		dao.deleteAlarmConfig(alarm_config_id);
	}

	/**
	 * deployEvent
	 * 
	 * @param event
	 * @throws Exception
	 */
	public void deployAlarmConfig(AlarmConfig alarm_config) throws Exception {
		Query query = new Query();
		query.setId(alarm_config.getAlarm_config_id());
		query.setEpl(alarm_config.getEpl());
		if(StringUtils.isNotEmpty(alarm_config.getEpl())){
			query_service.run(query, new EQLAlarmEventListener(alarm_config));
		}else{
			//EQL이 NULL이면 배치하지 않음 : 알람밴드 설정 후 제거하였을 경우
		};
		//
	}

	/**
	 * undeployEvent
	 * 
	 * @param event
	 * @throws Exception
	 */
	public void undeployAlarmConfig(AlarmConfig alarm_config) throws Exception {

		Query query = new Query();
		query.setId(alarm_config.getAlarm_config_id());
		//
		if (query_service.isRunning(query)) {
			query_service.remove(query);
		} else {
			log.warn("Undeployed alarm config = [" + alarm_config.getAlarm_config_id() + "]");
		}
		//
	}

	/**
	 * 
	 * @param event
	 * @throws Exception
	 */
	public void redeployAlarmConfig(AlarmConfig alarm_config) throws Exception {
		undeployAlarmConfig(alarm_config);
		deployAlarmConfig(alarm_config);
	}

	/**
	 * setEventStatus
	 * 
	 * @param event_list
	 * @return
	 */
	public List<AlarmConfig> setAlarmConfigStatus(List<AlarmConfig> alarm_config_list) {
		List<AlarmConfig> list = new ArrayList<AlarmConfig>();
		for (int i = 0; alarm_config_list != null && i < alarm_config_list.size(); i++) {
			AlarmConfig alarm_config = alarm_config_list.get(i);
			//
			Query query = new Query();
			query.setId(alarm_config.getAlarm_config_id());
			alarm_config.setStatus(query_service.getStatus(query));
			list.add(alarm_config);
		}
		return list;

	}

	public void deployAlarmConfigForTagBand(AlarmConfig alarm_config, Tag tag) throws Exception {
		Query query = new Query();
		query.setId(alarm_config.getAlarm_config_id());
		query.setEpl(alarm_config.getEpl());
		query_service.remove(query);
		if(StringUtils.isNotEmpty(alarm_config.getEpl())){
			query_service.run(query, new TagBandAlarmEventListener(alarm_config, tag));
		}
	}

	public void undeployAlarmConfigForTagBand(AlarmConfig alarm_config) throws Exception {
		Query query = new Query();
		query.setId(alarm_config.getAlarm_config_id());
		query.setEpl(alarm_config.getEpl());
		query_service.remove(query);
	}

	public void enableAlarmConfigForTagBand(AlarmConfig alarm_config, Tag tag) throws Exception {
		String alarm_config_id = dao.selectAlarmConfigIdById(alarm_config.getAlarm_config_id());
		if (StringUtils.isNotEmpty(alarm_config_id)) {
			Query query = new Query();
			query.setId(alarm_config_id);
			query.setEpl(alarm_config.getEpl());
			query_service.remove(query);
			alarm_config.setAlarm_config_id(alarm_config_id);
			updateAlarmConfig(alarm_config);
		} else {
			saveAlarmConfig(alarm_config);
		};
		
		deployAlarmConfigForTagBand(alarm_config, tag);
	}
	
	public void disableAlarmConfigForTagBand(AlarmConfig alarm_config) throws Exception {
		//
		updateAlarmConfig(alarm_config);
		undeployAlarmConfigForTagBand(alarm_config);
	}

	public void removeAlarmConfigForTagBand(AlarmConfig alarm_config) throws Exception {
		//
		deleteAlarmConfig(alarm_config.getAlarm_config_id());
		undeployAlarmConfigForTagBand(alarm_config);
	}
	
	
	public void rmoveAllAlarmByTag(String tag_id) throws Exception {
		List<AlarmConfig> config_list = dao.getAlarmConfigListByTagId(tag_id);
		for(int i=0; config_list !=null && i < config_list.size(); i++){
			AlarmConfig config = config_list.get(i);
			removeAlarmConfigForTagBand(config);
		}
	}


}
