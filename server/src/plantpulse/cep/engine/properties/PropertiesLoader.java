package plantpulse.cep.engine.properties;

import java.util.Properties;

import plantpulse.cep.engine.utils.PropertiesUtils;

/**
 * PropertiesLoader
 * 
 * @author leesa
 *
 */
public class PropertiesLoader {
	
	private static Properties application_properties = null;
	private static Properties batch_properties = null;
	private static Properties db_properties = null;
	private static Properties engine_properties = null;
	private static Properties mail_properties = null;
	private static Properties mq_properties = null;
	private static Properties storage_properties = null;
	private static Properties websocket_properties = null;
	
	
	public static void load() {
		application_properties = PropertiesUtils.read("./application.properties");
		batch_properties = PropertiesUtils.read("./batch.properties");
		db_properties = PropertiesUtils.read("./db.properties");
		engine_properties = PropertiesUtils.read("./engine.properties");
		mail_properties = PropertiesUtils.read("./mail.properties");
		mq_properties = PropertiesUtils.read("./mq.properties");
		storage_properties = PropertiesUtils.read("./storage.properties");
		websocket_properties = PropertiesUtils.read("./websocket.properties");
	}


	public static Properties getApplication_properties() {
		return application_properties;
	}


	public static Properties getBatch_properties() {
		return batch_properties;
	}


	public static Properties getDb_properties() {
		return db_properties;
	}


	public static Properties getEngine_properties() {
		return engine_properties;
	}


	public static Properties getMail_properties() {
		return mail_properties;
	}


	public static Properties getMq_properties() {
		return mq_properties;
	}


	public static Properties getStorage_properties() {
		return storage_properties;
	}


	public static Properties getWebsocket_properties() {
		return websocket_properties;
	};

	
	
}
