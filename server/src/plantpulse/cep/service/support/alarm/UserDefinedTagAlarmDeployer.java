package plantpulse.cep.service.support.alarm;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import plantpulse.cep.dao.AlarmConfigDAO;
import plantpulse.cep.domain.Query;
import plantpulse.cep.service.QueryService;
import plantpulse.domain.AlarmConfig;
import plantpulse.domain.Tag;

public class UserDefinedTagAlarmDeployer {
	
	private static final Log log = LogFactory.getLog(UserDefinedTagAlarmDeployer.class);
	
	/**
	 * 
	 * @param tag
	 * @param alarm_config
	 * @throws Exception
	 */
	public void deploy(Tag tag, AlarmConfig alarm_config) throws Exception {
		
		try{ 
			//
			alarm_config.setAlarm_type("TAG");
			alarm_config.setAlarm_config_priority("BAND");
			
			//
			AlarmConfigDAO ac_dao = new AlarmConfigDAO();
			ac_dao.deleteAlarmConfig(alarm_config.getAlarm_config_id());
			ac_dao.saveAlarmConfig(alarm_config);
			
			//
			QueryService query_service = new QueryService();
			Query query = new Query();
			query.setId(alarm_config.getAlarm_config_id());
			query.setEpl(alarm_config.getEpl());
			query_service.remove(query);
			
			query_service.remove(query);
			query_service.run(query, new TagBandAlarmEventListener(alarm_config, tag));
			//query_service.run(query, new EQLAlarmEventListener(alarm_config));
			
			log.info("User deinfed tag alarm deploy completed for tag_id=[" + tag.getTag_id() + "], class=[" + tag.getUser_defined_alarm_class() + "]");
			
		//
		}catch(Exception ex){
			log.error("User deinfed tag alarm deploy error : tag_id=[" + tag.getTag_id() + "]" + ex.getMessage(), ex);
		}
	}

}
