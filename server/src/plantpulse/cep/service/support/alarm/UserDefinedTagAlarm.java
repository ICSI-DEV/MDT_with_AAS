package plantpulse.cep.service.support.alarm;

import plantpulse.domain.AlarmConfig;
import plantpulse.domain.Tag;

/**
 * User defined alarm configuration
 * @author lsb
 *
 */
public interface UserDefinedTagAlarm {
	
	public AlarmConfig configure(Tag tag) throws Exception;
	
}
