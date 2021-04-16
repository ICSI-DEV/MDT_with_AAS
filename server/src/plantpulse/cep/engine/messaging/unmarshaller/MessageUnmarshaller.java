package plantpulse.cep.engine.messaging.unmarshaller;

import org.json.JSONObject;

import plantpulse.event.opc.Point;

/**
 * MessageUnmarshaller
 * 
 * @author lsb
 *
 */
public interface MessageUnmarshaller {

	/**
	 * MessageUnmarshaller
	 * 
	 * @param message
	 * @return
	 * @throws Exception
	 */
	
	public Point unmarshal(JSONObject event) throws Exception;

}
