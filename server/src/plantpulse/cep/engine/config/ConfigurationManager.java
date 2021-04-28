package plantpulse.cep.engine.config;

import java.util.Properties;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import plantpulse.cep.engine.utils.PropertiesUtils;

/**
 * ConfigurationManager
 * 
 * @author lsb
 *
 */
public class ConfigurationManager {

	private static final Log log = LogFactory.getLog(ConfigurationManager.class);

	private static class ConfigurationManagerHolder {
		static ConfigurationManager instance = new ConfigurationManager();
	}

	public static ConfigurationManager getInstance() {
		return ConfigurationManagerHolder.instance;
	}

	public static final String ENGINE_PROPERTIES_PATH = "/engine.properties";
	public static final String MQ_PROPERTIES_PATH = "/mq.properties";
	public static final String STORAGE_PROPERTIES_PATH = "/storage.properties";
	public static final String WEBSOCKET_PROPERTIES_PATH = "/websocket.properties";
	public static final String MAIL_PROPERTIES_PATH = "/mail.properties";
	public static final String APPLICATION_PROPERTIES_PATH = "/application.properties";

	private ServerConfiguration server_configuration = new ServerConfiguration();
	
	private Properties application_properties = new Properties();

	public ServerConfiguration getServer_configuration() {
		return server_configuration;
	}

	public void setServer_configuration(ServerConfiguration server_configuration) {
		this.server_configuration = server_configuration;
	}

	public Properties getApplication_properties() {
		return application_properties;
	}

	public void setApplication_properties(Properties application_properties) {
		this.application_properties = application_properties;
	}

	public void init() {
		Properties prop = null;

		// Engine
		prop = PropertiesUtils.read(ENGINE_PROPERTIES_PATH);
		server_configuration.setServer_home(prop.getProperty("server.home"));
		server_configuration.setHttp_host(prop.getProperty("http.host"));
		server_configuration.setHttp_port(prop.getProperty("http.port"));
		server_configuration.setHttp_user(prop.getProperty("http.user"));
		server_configuration.setHttp_password(prop.getProperty("http.password"));
		server_configuration.setHttp_destination(prop.getProperty("http.destination"));
		server_configuration.setNtp_host(prop.getProperty("engine.ntpdate.server.ip"));

		// MQ
		prop = PropertiesUtils.read(MQ_PROPERTIES_PATH);
		server_configuration.setMq_host(prop.getProperty("mq.host"));
		server_configuration.setMq_port(prop.getProperty("mq.port"));
		server_configuration.setMq_user(prop.getProperty("mq.user"));
		server_configuration.setMq_password(prop.getProperty("mq.password"));
		server_configuration.setMq_bind(prop.getProperty("mq.bind"));
		server_configuration.setMq_store(prop.getProperty("mq.store"));
		server_configuration.setMq_webadmin(prop.getProperty("mq.webadmin"));
		server_configuration.setMq_listener_count(Integer.parseInt(prop.getProperty("mq.listener.count")));
		server_configuration.setMq_use_kafka(Boolean.parseBoolean(prop.getProperty("mq.use_kafka")));

		// STORAGE
		prop = PropertiesUtils.read(STORAGE_PROPERTIES_PATH);
		server_configuration.setMetastore_driver(prop.getProperty("metastore.driver"));
		server_configuration.setMetastore_url(prop.getProperty("metastore.url"));
		server_configuration.setMetastore_user(prop.getProperty("metastore.user"));
		server_configuration.setMetastore_password(prop.getProperty("metastore.password"));
		server_configuration.setStorage_db_type(prop.getProperty("storage.db_type"));
		server_configuration.setStorage_db_version(prop.getProperty("storage.db_version"));
		server_configuration.setStorage_host(prop.getProperty("storage.host"));
		server_configuration.setStorage_port(prop.getProperty("storage.port"));
		server_configuration.setStorage_user(prop.getProperty("storage.user"));
		server_configuration.setStorage_password(prop.getProperty("storage.password"));
		server_configuration.setStorage_keyspace(prop.getProperty("storage.keyspace"));
		server_configuration.setStorage_append_columns(prop.getProperty("storage.append_columns"));
		server_configuration.setStorage_replication(prop.getProperty("storage.replication"));

		// WEBSOCKET
		prop = PropertiesUtils.read(WEBSOCKET_PROPERTIES_PATH);
		server_configuration.setWebsocket_server_baseurl(prop.getProperty("websocket.server.baseurl"));
		server_configuration.setWebsocket_server_host(prop.getProperty("websocket.server.host"));
		server_configuration.setWebsocket_server_port(prop.getProperty("websocket.server.port"));

		// MAIL
		prop = PropertiesUtils.read(MAIL_PROPERTIES_PATH);
		server_configuration.setMail_smtp_host(prop.getProperty("mail.smtp.host"));
		server_configuration.setMail_smtp_user(prop.getProperty("mail.smtp.user"));
		server_configuration.setMail_smtp_password(prop.getProperty("mail.smtp.password"));

		// APPLICATION
		application_properties = PropertiesUtils.read(APPLICATION_PROPERTIES_PATH);

		//

		log.info("ServerConfiguration manager initiallize successfully.");

	};

	public void update() {
		

	}

	//

}
