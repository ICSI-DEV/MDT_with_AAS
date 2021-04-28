package plantpulse.cep.engine.test;

import plantpulse.client.EventClient;
import plantpulse.client.EventClientFactory;

public class LoadTest {

	public static void main(String[] args) {
		load();
	}

	public static void load() {
		EventClient sender = EventClientFactory.getClient(EventClientFactory.MQTT);
		//
		try {

			/*
			 * // Properties propertie = new Properties();
			 * propertie.setProperty("host", "localhost");
			 * propertie.setProperty("port", "61000");
			 * propertie.setProperty("user", "admin");
			 * propertie.setProperty("password", "admin");
			 * 
			 * TestEvent test = new TestEvent();
			 * test.setInsert_date(System.currentTimeMillis());
			 * 
			 * for(int i=0; i < 1000; i++){ sender.send("/topic/event", test); }
			 */

		} catch (Exception ex) {
			ex.printStackTrace();
		} finally {
			sender.close();
		}
	}

}
