package plantpulse.cep.engine.config;

public class ServerConfiguration {

	// engine.properties
	private String server_home;
	private String http_host;
	private String http_port;
	private String http_user;
	private String http_password;
	private String http_destination;
	private String ntp_host;

	// mq.properties
	private String mq_host;
	private String mq_port;
	private String mq_user;
	private String mq_password;
	private String mq_bind;
	private String mq_store;
	private String mq_webadmin;
	private int     mq_listener_count;
	private boolean mq_use_kafka;

	// storage.properties
	private String metastore_driver;
	private String metastore_url;
	private String metastore_user;
	private String metastore_password;
			
	private String storage_db_type;
	private String storage_db_version;
	private String storage_host;
	private String storage_port;
	private String storage_user;
	private String storage_password;
	private String storage_keyspace;
	private String storage_append_columns;
	private String storage_replication;

	// websocket.properties
	private String websocket_server_baseurl;
	private String websocket_server_host;
	private String websocket_server_port;

	// mail.properties
	private String mail_smtp_host;
	private String mail_smtp_user;
	private String mail_smtp_password;

	public String getWebsocket_server_port() {
		return websocket_server_port;
	}

	public void setWebsocket_server_port(String websocket_server_port) {
		this.websocket_server_port = websocket_server_port;
	}

	public String getServer_home() {
		return server_home;
	}

	public void setServer_home(String server_home) {
		this.server_home = server_home;
	}

	public String getHttp_host() {
		return http_host;
	}

	public void setHttp_host(String http_host) {
		this.http_host = http_host;
	}

	public String getHttp_port() {
		return http_port;
	}

	public void setHttp_port(String http_port) {
		this.http_port = http_port;
	}

	public String getHttp_user() {
		return http_user;
	}

	public void setHttp_user(String http_user) {
		this.http_user = http_user;
	}

	public String getHttp_password() {
		return http_password;
	}

	public void setHttp_password(String http_password) {
		this.http_password = http_password;
	}

	public String getHttp_destination() {
		return http_destination;
	}

	public void setHttp_destination(String http_destination) {
		this.http_destination = http_destination;
	}

	public String getMq_host() {
		return mq_host;
	}

	public void setMq_host(String mq_host) {
		this.mq_host = mq_host;
	}

	public String getMq_port() {
		return mq_port;
	}

	public void setMq_port(String mq_port) {
		this.mq_port = mq_port;
	}

	public String getMq_user() {
		return mq_user;
	}

	public void setMq_user(String mq_user) {
		this.mq_user = mq_user;
	}

	public String getMq_password() {
		return mq_password;
	}

	public void setMq_password(String mq_password) {
		this.mq_password = mq_password;
	}


	public String getMq_bind() {
		return mq_bind;
	}

	public void setMq_bind(String mq_bind) {
		this.mq_bind = mq_bind;
	}

	public String getMq_store() {
		return mq_store;
	}

	public void setMq_store(String mq_store) {
		this.mq_store = mq_store;
	}

	public String getMq_webadmin() {
		return mq_webadmin;
	}

	public void setMq_webadmin(String mq_webadmin) {
		this.mq_webadmin = mq_webadmin;
	}

	public int getMq_listener_count() {
		return mq_listener_count;
	}

	public void setMq_listener_count(int mq_listener_count) {
		this.mq_listener_count = mq_listener_count;
	}

	public String getStorage_db_type() {
		return storage_db_type;
	}

	public void setStorage_db_type(String storage_db_type) {
		this.storage_db_type = storage_db_type;
	}

	public String getStorage_host() {
		return storage_host;
	}

	public void setStorage_host(String storage_host) {
		this.storage_host = storage_host;
	}

	public String getStorage_port() {
		return storage_port;
	}

	public void setStorage_port(String storage_port) {
		this.storage_port = storage_port;
	}

	public String getStorage_user() {
		return storage_user;
	}

	public void setStorage_user(String storage_user) {
		this.storage_user = storage_user;
	}

	public String getStorage_password() {
		return storage_password;
	}

	public void setStorage_password(String storage_password) {
		this.storage_password = storage_password;
	}

	public String getStorage_append_columns() {
		return storage_append_columns;
	}

	public void setStorage_append_columns(String storage_append_columns) {
		this.storage_append_columns = storage_append_columns;
	}

	public String getStorage_keyspace() {
		return storage_keyspace;
	}

	public void setStorage_keyspace(String storage_keyspace) {
		this.storage_keyspace = storage_keyspace;
	}

	public String getWebsocket_server_baseurl() {
		return websocket_server_baseurl;
	}

	public void setWebsocket_server_baseurl(String websocket_server_baseurl) {
		this.websocket_server_baseurl = websocket_server_baseurl;
	}

	public String getWebsocket_server_host() {
		return websocket_server_host;
	}

	public void setWebsocket_server_host(String websocket_server_host) {
		this.websocket_server_host = websocket_server_host;
	}

	public String getMail_smtp_host() {
		return mail_smtp_host;
	}

	public void setMail_smtp_host(String mail_smtp_host) {
		this.mail_smtp_host = mail_smtp_host;
	}

	public String getMail_smtp_user() {
		return mail_smtp_user;
	}

	public void setMail_smtp_user(String mail_smtp_user) {
		this.mail_smtp_user = mail_smtp_user;
	}

	public String getMail_smtp_password() {
		return mail_smtp_password;
	}

	public void setMail_smtp_password(String mail_smtp_password) {
		this.mail_smtp_password = mail_smtp_password;
	}

	public String getStorage_db_version() {
		return storage_db_version;
	}

	public void setStorage_db_version(String storage_db_version) {
		this.storage_db_version = storage_db_version;
	}

	public boolean isMq_use_kafka() {
		return mq_use_kafka;
	}

	public void setMq_use_kafka(boolean mq_use_kafka) {
		this.mq_use_kafka = mq_use_kafka;
	}

	public String getStorage_replication() {
		return storage_replication;
	}

	public void setStorage_replication(String storage_replication) {
		this.storage_replication = storage_replication;
	}

	public String getMetastore_driver() {
		return metastore_driver;
	}

	public void setMetastore_driver(String metastore_driver) {
		this.metastore_driver = metastore_driver;
	}

	public String getMetastore_url() {
		return metastore_url;
	}

	public void setMetastore_url(String metastore_url) {
		this.metastore_url = metastore_url;
	}

	public String getMetastore_user() {
		return metastore_user;
	}

	public void setMetastore_user(String metastore_user) {
		this.metastore_user = metastore_user;
	}

	public String getMetastore_password() {
		return metastore_password;
	}

	public void setMetastore_password(String metastore_password) {
		this.metastore_password = metastore_password;
	};
	
	

	

	public String getNtp_host() {
		return ntp_host;
	}

	public void setNtp_host(String ntp_host) {
		this.ntp_host = ntp_host;
	}

	@Override
	public String toString() {
		return "ServerConfiguration [server_home=" + server_home + ", http_host=" + http_host + ", http_port="
				+ http_port + ", http_user=" + http_user + ", http_password=" + http_password + ", http_destination="
				+ http_destination + ", ntp_host=" + ntp_host + ", mq_host=" + mq_host + ", mq_port=" + mq_port
				+ ", mq_user=" + mq_user + ", mq_password=" + mq_password + ", mq_bind=" + mq_bind + ", mq_store="
				+ mq_store + ", mq_webadmin=" + mq_webadmin + ", mq_listener_count=" + mq_listener_count
				+ ", mq_use_kafka=" + mq_use_kafka + ", metastore_driver=" + metastore_driver + ", metastore_url="
				+ metastore_url + ", metastore_user=" + metastore_user + ", metastore_password=" + metastore_password
				+ ", storage_db_type=" + storage_db_type + ", storage_db_version=" + storage_db_version
				+ ", storage_host=" + storage_host + ", storage_port=" + storage_port + ", storage_user=" + storage_user
				+ ", storage_password=" + storage_password + ", storage_keyspace=" + storage_keyspace
				+ ", storage_append_columns=" + storage_append_columns + ", storage_replication=" + storage_replication
				+ ", websocket_server_baseurl=" + websocket_server_baseurl + ", websocket_server_host="
				+ websocket_server_host + ", websocket_server_port=" + websocket_server_port + ", mail_smtp_host="
				+ mail_smtp_host + ", mail_smtp_user=" + mail_smtp_user + ", mail_smtp_password=" + mail_smtp_password
				+ "]";
	}


	
}
