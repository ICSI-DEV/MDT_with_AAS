package plantpulse.cep.engine.monitoring.timer;

import java.time.Instant;
import java.util.Collections;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.Host;
import com.datastax.driver.core.Session;
import com.datastax.driver.core.SocketOptions;
import com.datastax.driver.core.policies.ConstantReconnectionPolicy;

import plantpulse.cep.engine.config.ConfigurationManager;
import plantpulse.cep.engine.logging.EngineLogger;
import plantpulse.cep.engine.storage.StorageConstants;

/**
 * CassandraHealthCheckTimer
 * 
 * @author leesa
 *
 */
public class CassandraHealthCheckTimer implements MonitoringTimer {

    private static final Log log = LogFactory.getLog(CassandraHealthCheckTimer.class);
	

    private String HEALTH_CHECK_CQL = "SELECT ... ";
    
    private String host     = "127.0.0.1";
    private int port = 9042;
    private String keyspace = "PP";
    private String username = "cassandra";
    private String password = "cassandra";
    
    private Cluster cluster = null;
    private Session session = null;
    
    private ScheduledExecutorService executor = Executors.newScheduledThreadPool(1);

    private volatile HealthStatus currentStatus = new HealthStatus("NOT_EXECUTED_YET", Status.UP);

    public CassandraHealthCheckTimer() {
    	
    	//
		String host = ConfigurationManager.getInstance().getServer_configuration().getStorage_host();
		int port = Integer.parseInt(ConfigurationManager.getInstance().getServer_configuration().getStorage_port());
		String username = ConfigurationManager.getInstance().getServer_configuration().getStorage_user();
		String password = ConfigurationManager.getInstance().getServer_configuration().getStorage_password();
		String keyspace = ConfigurationManager.getInstance().getServer_configuration().getStorage_keyspace();
		
		//
		this.host = host;
		this.port = port;
		this.username = username;
		this.password = password;
        this.keyspace = keyspace;
        

    	//
    	this.HEALTH_CHECK_CQL = "SELECT * FROM " + keyspace + "." + StorageConstants.SITE_TABLE+ " LIMIT 1"; 

    }

    public void start() throws Exception {
    	
    	//
        SocketOptions socketOptions = new SocketOptions();
        socketOptions.setConnectTimeoutMillis(1000 * 60 * 60);
        socketOptions.setReadTimeoutMillis(1000 * 60 * 1);
        socketOptions.setKeepAlive(true);

        cluster = Cluster.builder()
                .addContactPoints(host)
                .withPort(port)
                .withCredentials(username, password)
                .withSocketOptions(socketOptions)
                .withReconnectionPolicy(new ConstantReconnectionPolicy(10_000L))
                .withInitialListeners(Collections.singleton(new CassandraStateListener()))
                .build();
        session = cluster.connect();
        
       
        session.execute("USE " + keyspace);

        executor.scheduleAtFixedRate(() -> {
        	//
                    Status overall = checkStatus(host, session);
                    log.debug("ALL STATUS : " + overall.toString());
                    //
                    this.currentStatus = new HealthStatus(
                    		HEALTH_CHECK_CQL,
                            overall);
                    log.debug(" - " + this.currentStatus);

                },
                90, 10, TimeUnit.MINUTES);
    }


    private Status checkStatus(String host, Session session) {
        try {
            session.execute(HEALTH_CHECK_CQL);
            return Status.UP;
        } catch (Exception e) {
            log.error("Unable to execute query for cassandra host : " + host, e);
            EngineLogger.error("데이터베이스 [" + host.toString() + "] 의 연결 및 쿼리 실행 상태에 문제가 발생하였습니다 : ERROR=[" + e.getMessage() + "]");
            return Status.DOWN;
        }
    }

    public HealthStatus status() {
        return this.currentStatus;
    }
    
    public void stop() {
    	if(session != null) session.close();
    	if(cluster != null) cluster.close();
    	if(executor != null) executor.shutdown();
    }

    public static class HealthStatus {
        private final Instant lastExecuted;
        private final String query;
        private final Status overall;

        public HealthStatus(String query, Status overall) {
            this.lastExecuted = Instant.now();
            this.query = query;
            this.overall = overall;
        }

        public String getQuery() {
            return query;
        }

        public Status getOverall() {
            return overall;
        }

        public long getLastExecuted() {
            return lastExecuted.toEpochMilli();
        }

        @Override
        public String toString() {
            return "CASSANDRA_HEALTH_STATUS = {" +
                    "last_executed=" + lastExecuted +
                    ", query='" + query + '\'' +
                    ", overall=" + overall +
                    '}';
        }
    }

    public enum Status {
        UP, DOWN
    }

    private static class CassandraStateListener implements Host.StateListener {

    	private static final Log log = LogFactory.getLog(CassandraStateListener.class);
    	

		@Override
		public void onAdd(Host host) {
			log.info("Cassandra host added : " + host);
			EngineLogger.info("클러스터 서버 노드 [" + host.toString() + "]가 추가되었습니다.");
		}

		@Override
		public void onDown(Host host) {
			log.warn("Cassandra host down : " +  host);
			EngineLogger.warn("클러스터 서버 노드 [" + host.toString() + "]가 다운되었습니다.");
		}

		@Override
		public void onRegister(Cluster cluster) {
			log.info("Cassandra cluster registed : cluster_name=[" +  cluster.getClusterName() + "]");
		}
		
		@Override
		public void onUnregister(Cluster cluster) {
			log.info("Cassandra cluster unregisted : cluster_name=[" +  cluster.getClusterName() + "]");
		}

		@Override
		public void onRemove(Host host) {
			log.info("Cassandra cluster removed : " +  host);
			EngineLogger.warn("클러스터 서버 노드 [" + host.toString() + "]가 삭제되었습니다.");
		}

		@Override
		public void onUp(Host host) {
			log.info("Cassandra cluster up : " +  host);
			EngineLogger.info("클러스터 서버 노드 [" + host.toString() + "]가 활성화되었습니다.");
		}

    }
}