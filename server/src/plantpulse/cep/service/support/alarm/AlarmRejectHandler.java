package plantpulse.cep.service.support.alarm;

import java.util.Calendar;
import java.util.Date;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.realdisplay.framework.util.CalendarUtils;

import plantpulse.cep.engine.config.ConfigurationManager;
import plantpulse.cep.service.AlarmService;

/**
 * AlarmRejectHandler
 * 
 * @author lsb
 *
 */
public class AlarmRejectHandler {

	private static final Log log = LogFactory.getLog(AlarmRejectHandler.class);

	public static final String BEFORE_DATETIME_FORMMAT = " minute ago";

	private AlarmService alarm_service = new AlarmService();

	public boolean aleadyAlarmed(String alarm_config_id, String insert_user_id, String priority) {
		try {
			String min   = ConfigurationManager.getInstance().getApplication_properties().getProperty("alarm.duplicate.check.minutes");
			Calendar cal = CalendarUtils.parse(new Date(), min + BEFORE_DATETIME_FORMMAT);
			int count = alarm_service.getAlarmConfigCountByDate(alarm_config_id, insert_user_id, cal.getTime(), priority);
			if (count > 0) {
				log.debug("Alarm rejected for de-duplication : before_minutes=[" + (min) + "], alarm_config_id=[" + alarm_config_id + "], insert_user_id=[" + insert_user_id + "]");
			}
			return (count > 0) ? true : false;
		} catch (Exception ex) {
			log.error("AlarmRejectHandler error : " + ex.getMessage(), ex);
			return false;
		}
	}

}
