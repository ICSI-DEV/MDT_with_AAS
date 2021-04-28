package plantpulse.cep.service.support.alarm;

import plantpulse.domain.AlarmConfig;
import plantpulse.domain.Tag;

/**
 * DefaultUserDefinedTagAlarm
 * @author lsb
 *
 */
public class DefaultUserDefinedTagAlarm implements UserDefinedTagAlarm {

	@Override
	public AlarmConfig configure(Tag tag) throws Exception {
		
		//
		StringBuffer eql = new StringBuffer();
		eql.append("SELECT *, '1' as set_value, 'HIGH' as band  "
				+ " FROM Point(tag_id = '" + tag.getTag_id() + "').win:time(10 sec) "
						+ " OUTPUT LAST EVERY  10 SEC ");

		//
		AlarmConfig alarm_config = new AlarmConfig();
		alarm_config.setAlarm_config_id("USER_DEFINED_ALARM_" + tag.getTag_id());
		alarm_config.setAlarm_config_name("USER_DEFINED_ALARM_" + tag.getTag_name());
		alarm_config.setAlarm_config_desc("");
		
		alarm_config.setMessage("사용자 정의 알람 [${band}] 발생 : 설정값 =[${set_value}], 현재값=[${value}]");
		alarm_config.setSend_email(false);
		alarm_config.setSend_sms(false);
		alarm_config.setRecieve_me(true);
		alarm_config.setRecieve_others(null);
		
		alarm_config.setDuplicate_check(true);
		alarm_config.setDuplicate_check_time(5);
		alarm_config.setEpl(eql.toString());
		alarm_config.setTag_id(tag.getTag_id());
		
		//
		alarm_config.setInsert_user_id("admin");
		

		
		return alarm_config;
		
	}

}
