package plantpulse.cep.engine.logging;

import java.io.IOException;
import java.net.InetAddress;
import java.util.Date;
import java.util.Map;
import java.util.Properties;
import java.util.TreeMap;

import org.apache.log4j.AppenderSkeleton;
import org.apache.log4j.Level;
import org.apache.log4j.LogManager;
import org.apache.log4j.helpers.LogLog;
import org.apache.log4j.spi.LocationInfo;
import org.apache.log4j.spi.LoggingEvent;

import com.datastax.driver.core.AuthProvider;
import com.datastax.driver.core.BoundStatement;
import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.ConsistencyLevel;
import com.datastax.driver.core.PlainTextAuthProvider;
import com.datastax.driver.core.PreparedStatement;
import com.datastax.driver.core.Session;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.base.Joiner;

import plantpulse.cep.engine.CEPEngineManager;
import plantpulse.cep.engine.config.ConfigurationManager;
import plantpulse.cep.engine.utils.PropertiesUtils;
import plantpulse.event.Log;

/**
 * Main class that uses Cassandra to store log entries into.
 * 
 */
public class LoggingAppender extends AppenderSkeleton {
	
	//TODO 변경
	public static final String PP_APPLICATION_NAME = "SERVER";
	public static final String PP_LOG_KEYSPACE     = "pp_system";
	public static final String PP_LOG_TABLE_NAME       = "tm_system_log";
	// Cassandra configuration
	private String hosts = "127.0.0.1";
	private int port = 9042; // for the binary protocol, 9160 is default for							// thrift
	private String username = "";
	private String password = "";
	
	private static final String ip = getIP();
	private static final String hostname = getHostName();

	// Encryption. sslOptions and authProviderOptions are JSON maps requiring
	// Jackson
	private static final ObjectMapper jsonMapper = new ObjectMapper();
	private Map<String, String> sslOptions = null;
	private Map<String, String> authProviderOptions = null;

	// Keyspace/ColumnFamily information
	private String keyspaceName =  PP_LOG_KEYSPACE;
	private String tableName    =  PP_LOG_TABLE_NAME;
	private String appName      =  PP_APPLICATION_NAME;
	private String replication = "{ 'class' : 'SimpleStrategy', 'replication_factor' : 1 }";
	private ConsistencyLevel consistencyLevelWrite = ConsistencyLevel.ONE;
	
	// TTL
	private long defaultTTL = 60 * 60 * 24 * 30; //30일 TTL

	// CF column names
	public static final String ID = "key";
	public static final String HOST_IP = "host_ip";
	public static final String HOST_NAME = "host_name";
	public static final String APP_NAME = "app_name";
	public static final String LOGGER_NAME = "logger_name";
	public static final String LEVEL = "level";
	public static final String CLASS_NAME = "class_name";
	public static final String FILE_NAME = "file_name";
	public static final String LINE_NUMBER = "line_number";
	public static final String METHOD_NAME = "method_name";
	public static final String MESSAGE = "message";
	public static final String NDC = "ndc";
	public static final String APP_START_TIME = "app_start_time";
	public static final String THREAD_NAME = "thread_name";
	public static final String THROWABLE_STR = "throwable_str_rep";
	public static final String TIMESTAMP = "timestamp";
	
	public static final String KEYSPACE_SUFFIX = "_LOG";

	// session state
	private PreparedStatement statement;
	private volatile boolean initialized = false;
	private volatile boolean initializationFailed = false;
	private Session session;
	private Cluster cluster;

	public LoggingAppender() {
		LogLog.debug("Creating LoggingAppender");
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	protected void append(LoggingEvent event) {
		// We have to defer initialization of the client because
		// TTransportFactory
		// references some Hadoop classes which can't safely be used until the
		// logging
		// infrastructure is fully set up. If we attempt to initialize the
		// client
		// earlier, it causes NPE's from the constructor of
		// org.apache.hadoop.conf.Configuration.
		if (!initialized)
			initClient();
		if (!initializationFailed)
			createAndExecuteQuery(event);
	}

	// Connect to cassandra, then setup the schema and preprocessed statement
	private synchronized void initClient() {

		Properties prop = PropertiesUtils.read(ConfigurationManager.STORAGE_PROPERTIES_PATH);

		
		//TODO 사용을 위해 연결 정보만 변경
		hosts = prop.getProperty("storage.host");
		port = Integer.parseInt(prop.getProperty("storage.port"));
		username = prop.getProperty("storage.user");
		password = prop.getProperty("storage.password");
		
	
		// We should be able to go without an Atomic variable here. There are
		// two potential problems:
		// 1. Multiple threads read intialized=false and call init client.
		// However, the method is
		// synchronized so only one will get the lock first, and the others will
		// drop out here.
		// 2. One thread reads initialized=true before initClient finishes. This
		// also should not
		// happen as the lock should include a memory barrier.
		if (initialized || initializationFailed)
			return;

		// Just while we initialise the client, we must temporarily
		// disable all logging or else we get into an infinite loop
		Level globalThreshold = LogManager.getLoggerRepository().getThreshold();
		LogManager.getLoggerRepository().setThreshold(Level.OFF);

		try {

			
		    Cluster.Builder clusterBuilder = Cluster.builder()
					.addContactPoints(hosts)
					.withPort(port)
					.withAuthProvider(new PlainTextAuthProvider(username, password));
					cluster = clusterBuilder.build();
					
					//Metadata metadata = cluster.getMetadata();
					//System.out.println("Connected to cassandra cluster: " +  metadata.getClusterName());
					//for (Host host: metadata.getAllHosts()) {
						//log.info("Datacenter: %s; Host: %s; Rack: %s\n", , host.getAddress(), host.getRack());
					//};
					
					session = cluster.connect(); 
						
			setupSchema();
			setupStatement();

			System.out.println("System logging adapter initiallized.");
			
		} catch (Exception e) {
			LogLog.error("Error ", e);
			errorHandler.error("Error setting up cassandra logging schema: " + e);

			// If the user misconfigures the port or something, don't keep
			// failing.
			initializationFailed = true;
		} finally {
			// Always reenable logging
			LogManager.getLoggerRepository().setThreshold(globalThreshold);
			initialized = true;
		}
	}

	/**
	 * Create Keyspace and CF if they do not exist.
	 */
	private void setupSchema() throws IOException {
	
		// Create keyspace if necessary
		System.out.println("Log storage : keyspaceName=[" + keyspaceName + "], tableName=[" + tableName + "], TTL=[" + defaultTTL + "]");
		String ksQuery = String.format("CREATE KEYSPACE IF NOT EXISTS \"%s\" WITH REPLICATION = %s;", keyspaceName, replication);
		session.execute(ksQuery);
		System.out.println("Log keyspace created in cassandra : CQL=[" + ksQuery + "], TTL=[" + defaultTTL + "]");

		// Create table if necessary
		String cfQuery = String.format(
				"CREATE TABLE IF NOT EXISTS \"%s\".\"%s\" (%s text, %s timestamp, %s text, %s text, %s text, %s text, %s text,"
						+ "%s text, %s text, %s timestamp, %s text, %s text, %s text, %s text," + "%s text, PRIMARY KEY(%s,  %s)) WITH CLUSTERING ORDER BY (%s DESC); ",
				        keyspaceName, tableName, APP_NAME, APP_START_TIME, CLASS_NAME, FILE_NAME, HOST_IP, HOST_NAME, LEVEL, LINE_NUMBER, METHOD_NAME, TIMESTAMP, LOGGER_NAME, MESSAGE, NDC, THREAD_NAME, THROWABLE_STR, 
				        APP_NAME, TIMESTAMP, 
				        TIMESTAMP);
		session.execute(cfQuery);
		
        //TimeWindowCompactionStrategy
		session.execute( " ALTER TABLE " + keyspaceName + "." + tableName  + " "
				+ " WITH bloom_filter_fp_chance = 0.01 "
				+ " AND caching = {'keys':'ALL', 'rows_per_partition':'NONE'}  "
				+ " AND gc_grace_seconds = 864000 "
				+ " AND default_time_to_live = " + defaultTTL + " "
				+ " AND compaction = {"
						+ "'compaction_window_size': '1', "
						+ "'compaction_window_unit': 'DAYS', "
						+ "'class': 'org.apache.cassandra.db.compaction.TimeWindowCompactionStrategy'"
						+ "}; ");
		
		
		String idxQuery1 = "CREATE INDEX IF NOT EXISTS T_IDX_TIMESTAMP ON " + keyspaceName + "." + tableName + " (" + TIMESTAMP + ") ";
		session.execute(idxQuery1);

		
		String idxQuery2 = "CREATE INDEX IF NOT EXISTS T_IDX_LEVEL ON " + keyspaceName + "." + tableName + " (" + LEVEL + ") ";
		session.execute(idxQuery2);
	
		
		// SASI 인덱스
		//if(StorageCompatibility.isCassandra()) {
			StringBuffer sasi_option = new StringBuffer();
			sasi_option.append(" USING 'org.apache.cassandra.index.sasi.SASIIndex' ");
			sasi_option.append("     WITH OPTIONS = {     ");
			sasi_option.append("    'analyzed': 'true',   ");
			sasi_option.append(" 	'mode': 'CONTAINS',    ");
			sasi_option.append(" 	'analyzer_class': 'org.apache.cassandra.index.sasi.analyzer.StandardAnalyzer',");
			sasi_option.append(" 	'case_sensitive': 'false' ");
			sasi_option.append(" } ");
		
			
			StringBuffer sasi_idx_cql = new StringBuffer();
			sasi_idx_cql.append(" CREATE CUSTOM INDEX IF NOT EXISTS T_IDX_MESSAGE on " + keyspaceName + "."  + tableName + " (" + MESSAGE + ") " + sasi_option.toString());
			session.execute(sasi_idx_cql.toString());
		//};
	
		
	}

	/**
	 * Setup and preprocess our insert query, so that we can just bind values
	 * and send them over the binary protocol
	 */
	private void setupStatement() {
		//
		String insertQuery = String.format(
				"INSERT INTO \"%s\".\"%s\" " + "( %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s) " 
		                                     + "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?); ", keyspaceName, tableName,  APP_NAME, HOST_IP, HOST_NAME, LOGGER_NAME, LEVEL, CLASS_NAME, FILE_NAME, LINE_NUMBER, METHOD_NAME, MESSAGE, NDC, APP_START_TIME, THREAD_NAME, THROWABLE_STR, TIMESTAMP);

		statement = session.prepare(insertQuery);
		statement.setConsistencyLevel(ConsistencyLevel.valueOf(consistencyLevelWrite.toString()));
	}

	/**
	 * Send one logging event to Cassandra. We just bind the new values into the
	 * preprocessed query built by setupStatement
	 */
	private void createAndExecuteQuery(LoggingEvent event) {
		
		
			
		if (session.isClosed()) {
			System.out.println("Session or Cluseter Closed : Reconnecting storage...");
			initClient();
		};

		BoundStatement bound = new BoundStatement(statement);

		// A primary key combination of timestamp/hostname/threadname should be
		// unique as long as the thread names
		// are set, but would not be backwards compatible. Do we care?
		
		int index = 0;
		//bound.setUUID(index++, UUIDs.timeBased());
		bound.setString(index++, appName);
		bound.setString(index++, ip);
		bound.setString(index++, hostname);
		bound.setString(index++, event.getLoggerName());
		bound.setString(index++, event.getLevel().toString());

		LocationInfo locInfo = event.getLocationInformation();
		if (locInfo != null) {
			bound.setString(index++, locInfo.getClassName());
			bound.setString(index++, locInfo.getFileName());
			bound.setString(index++, locInfo.getLineNumber());
			bound.setString(index++, locInfo.getMethodName());
		}

		bound.setString(index++, event.getRenderedMessage());
		bound.setString(index++, event.getNDC());
		bound.setTimestamp(index++, new Date(LoggingEvent.getStartTime()));
		bound.setString(index++, event.getThreadName());

		String[] throwableStrs = event.getThrowableStrRep();
		bound.setString(index++, throwableStrs == null ? null : Joiner.on(", ").join(throwableStrs));
		
		bound.setTimestamp(index++, new Date(System.currentTimeMillis()));

		session.executeAsync(bound);
		

		//
		if(CEPEngineManager.getInstance().isStarted()){
			   Log log = new Log();
			   log.setTimestamp(event.getTimeStamp());
			   log.setLevel(event.getLevel().toString());
			   log.setMessage(event.getMessage().toString());
			   if(CEPEngineManager.getInstance().getProvider() != null && !CEPEngineManager.getInstance().getProvider().isDestroyed()) {
				   CEPEngineManager.getInstance().getProvider().getEPRuntime().sendEvent(log);
			   };
			   //System.out.println("Log sened.");;
		};
		
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public void close() {
		if(session != null) session.close();
		if(cluster != null) cluster.close();
		//System.out.println("LoggingAppend closed.");
	}

	/**
	 * {@inheritDoc}
	 *
	 * @see org.apache.log4j.Appender#requiresLayout()
	 */
	@Override
	public boolean requiresLayout() {
		return false;
	}

	/**
	 * Called once all the options have been set. Starts listening for clients
	 * on the specified socket.
	 */
	@Override
	public void activateOptions() {
		// reset();
	}

	//
	// Boilerplate from here on out
	//

	public String getKeyspaceName() {
		return keyspaceName;
	}

	public void setKeyspaceName(String keyspaceName) {
		this.keyspaceName = keyspaceName;
	}

	public String getHosts() {
		return hosts;
	}

	public void setHosts(String hosts) {
		this.hosts = hosts;
	}

	public int getPort() {
		return port;
	}

	public void setPort(int port) {
		this.port = port;
	}

	public String getUsername() {
		return username;
	}

	public void setUsername(String username) {
		this.username = unescape(username);
	}

	public String getPassword() {
		return password;
	}

	public void setPassword(String password) {
		this.password = unescape(password);
	}

	public String getColumnFamily() {
		return tableName;
	}

	public void setColumnFamily(String columnFamily) {
		this.tableName = columnFamily;
	}

	public String getReplication() {
		return replication;
	}

	public void setReplication(String strategy) {
		replication = unescape(strategy);
	}

	private Map<String, String> parseJsonMap(String options, String type) throws Exception {
		if (options == null)
			throw new IllegalArgumentException(type + "Options can't be null.");

		return jsonMapper.readValue(unescape(options), new TreeMap<String, String>().getClass());
	}

	public void setAuthProviderOptions(String newOptions) throws Exception {
		authProviderOptions = parseJsonMap(newOptions, "authProvider");
	}

	public void setSslOptions(String newOptions) throws Exception {
		sslOptions = parseJsonMap(newOptions, "Ssl");
	}

	public String getConsistencyLevelWrite() {
		return consistencyLevelWrite.toString();
	}

	public void setConsistencyLevelWrite(String consistencyLevelWrite) {
		try {
			this.consistencyLevelWrite = ConsistencyLevel.valueOf(unescape(consistencyLevelWrite));
		} catch (IllegalArgumentException e) {
			throw new IllegalArgumentException("Consistency level " + consistencyLevelWrite + " wasn't found. Available levels: " + Joiner.on(", ").join(ConsistencyLevel.values()));
		}
	}

	public String getAppName() {
		return appName;
	}

	public void setAppName(String appName) {
		this.appName = appName;
	}

	private static String getHostName() {
		String hostname = "unknown";

		try {
			InetAddress addr = InetAddress.getLocalHost();
			hostname = addr.getHostName();
		} catch (Throwable t) {

		}
		return hostname;
	}

	private static String getIP() {
		String ip = "unknown";

		try {
			InetAddress addr = InetAddress.getLocalHost();
			ip = addr.getHostAddress();
		} catch (Throwable t) {

		}
		return ip;
	}

	/**
	 * Strips leading and trailing '"' characters
	 * 
	 * @param b
	 *            - string to unescape
	 * @return String - unexspaced string
	 */
	private static String unescape(String b) {
		if (b.charAt(0) == '\"' && b.charAt(b.length() - 1) == '\"')
			b = b.substring(1, b.length() - 1);
		return b;
	}

	// Create an SSLContext (a container for a keystore and a truststore and
	// their associated options)
	// Assumes sslOptions map is not null
	/*
	 * private SSLOptions getSslOptions() throws Exception { // init trust store
	 * TrustManagerFactory tmf = null; String truststorePath =
	 * sslOptions.get("ssl.truststore"); String truststorePassword =
	 * sslOptions.get("ssl.truststore.password"); if (truststorePath != null &&
	 * truststorePassword != null) { FileInputStream tsf = new
	 * FileInputStream(truststorePath); KeyStore ts =
	 * KeyStore.getInstance("JKS"); ts.load(tsf,
	 * truststorePassword.toCharArray()); tmf =
	 * TrustManagerFactory.getInstance(TrustManagerFactory.getDefaultAlgorithm()
	 * ); tmf.init(ts); }
	 * 
	 * // init key store KeyManagerFactory kmf = null; String keystorePath =
	 * sslOptions.get("ssl.keystore"); String keystorePassword =
	 * sslOptions.get("ssl.keystore.password"); if (keystorePath != null &&
	 * keystorePassword != null) { FileInputStream ksf = new
	 * FileInputStream(keystorePath); KeyStore ks = KeyStore.getInstance("JKS");
	 * ks.load(ksf, keystorePassword.toCharArray()); kmf =
	 * KeyManagerFactory.getInstance(KeyManagerFactory.getDefaultAlgorithm());
	 * kmf.init(ks, keystorePassword.toCharArray());
	 * 
	 * }
	 * 
	 * // init cipher suites String[] ciphers =
	 * SSLOptions.DEFAULT_SSL_CIPHER_SUITES; if
	 * (sslOptions.containsKey("ssl.ciphersuites")) ciphers =
	 * sslOptions.get("ssl.ciphersuits").split(",\\s*");
	 * 
	 * SSLContext ctx = SSLContext.getInstance("SSL"); ctx.init(kmf == null ?
	 * null : kmf.getKeyManagers(), tmf == null ? null : tmf.getTrustManagers(),
	 * new SecureRandom());
	 * 
	 * return new SSLOptions(ctx, ciphers); }
	 */

	// Load a custom AuthProvider class dynamically.
	public AuthProvider getAuthProvider() throws Exception {
		ClassLoader cl = ClassLoader.getSystemClassLoader();

		if (!authProviderOptions.containsKey("auth.class"))
			throw new IllegalArgumentException("authProvider map does not include auth.class.");
		Class dap = cl.loadClass(authProviderOptions.get("auth.class"));

		// Perhaps this should be a factory, but it seems easy enough to just
		// have a single string parameter
		// which can be encoded however, e.g. another JSON map
		if (authProviderOptions.containsKey("auth.options"))
			return (AuthProvider) dap.getConstructor(String.class).newInstance(authProviderOptions.get("auth.options"));
		else
			return (AuthProvider) dap.newInstance();
	}
	
	

}
