package plantpulse.cep.engine.messaging.publisher.mqtt;

import java.util.concurrent.atomic.AtomicBoolean;

/**
 * MQTT 연결 상태
 * @author leesa
 *
 */
public class MQTTConnectionStatus {
	
	public static AtomicBoolean CONNECTED = new AtomicBoolean(false);

}
