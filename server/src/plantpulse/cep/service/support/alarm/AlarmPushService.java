package plantpulse.cep.service.support.alarm;

import java.util.Date;

/**
 * AlarmPushService
 * 
 * @author lhh
 *
 */
public interface AlarmPushService {

	public static final String PRIORITY_INFO = "INFO";
	public static final String PRIORITY_WARN = "WARN";
	public static final String PRIORITY_ERROR = "ERROR";

	public void push(long alarm_seq, Date alarm_date, String priority, String description, String insert_user_id, String alarm_config_id, String tag_id) throws Exception;

}
