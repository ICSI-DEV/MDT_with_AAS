package plantpulse.cep.engine.monitoring.timer;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.Host;
import com.datastax.driver.core.HostDistance;
import com.datastax.driver.core.PoolingOptions;
import com.datastax.driver.core.Session;
import com.datastax.driver.core.policies.LoadBalancingPolicy;

import plantpulse.cep.engine.config.ConfigurationManager;
import plantpulse.cep.engine.timer.TimerExecuterPool;
import plantpulse.dbutils.cassandra.SessionManager;

/**
 * StorageSessionMonitorTimer
 * 
 * @author leesa
 *
 */
public class StorageSessionMonitorTimer  implements MonitoringTimer {

	private static final Log log = LogFactory.getLog(StorageSessionMonitorTimer.class);

	private static final int TIMER_PERIOD = (1000 * 10); //10초마다 한번씩 실행
	
	private static final int HEAVY_LOAD_COUNT = 10000; //헤비 로드 100건

	private ScheduledFuture<?> task;
	
	private List<String> hosts = new ArrayList<>();

	public void start() {
		
		task = TimerExecuterPool.getInstance().getPool().scheduleAtFixedRate(new Runnable() {

			@Override
			public void run() {
				//
				try {
					
					//
					String host = ConfigurationManager.getInstance().getServer_configuration().getStorage_host();
					int port = Integer.parseInt(ConfigurationManager.getInstance().getServer_configuration().getStorage_port());
					String user = ConfigurationManager.getInstance().getServer_configuration().getStorage_user();
					String password = ConfigurationManager.getInstance().getServer_configuration().getStorage_password();
					String keyspace = ConfigurationManager.getInstance().getServer_configuration().getStorage_keyspace();
					String replication = ConfigurationManager.getInstance().getServer_configuration().getStorage_replication();
					//
					
					Session session = SessionManager.getSession(host, port, user, password, keyspace);
		        	Session.State state = session.getState();
		        	Cluster cluster = session.getCluster();
		        	
		        	//
			        //int i=0;
			        // log.info("-- STORAGE CASSANDRA SESSION's --");
			        final LoadBalancingPolicy loadBalancingPolicy =  cluster.getConfiguration().getPolicies().getLoadBalancingPolicy();
			        final PoolingOptions poolingOptions = cluster.getConfiguration().getPoolingOptions();
			        	
					for (Host chost : state.getConnectedHosts()) {
						String ip = chost.getAddress().getHostAddress();
						if(!hosts.contains(ip)) {
							hosts.add(ip);
						};
						HostDistance distance = loadBalancingPolicy.distance(chost);
			            int connections = state.getOpenConnections(chost);
			            int trashed = state.getTrashedConnections(chost);
			            int inFlightQueries = state.getInFlightQueries(chost);
			            int maxLoad = poolingOptions.getMaxRequestsPerConnection(distance);
			            if(inFlightQueries >= HEAVY_LOAD_COUNT) {
			            log.info(String.format("Cassandra's in flight queries heavy load : host=[%s],"
			            		+ " connections=[%d], trashed=[%d],"
			            		+ " current_load=[%d], max_load=[%d]",  
			            		chost.getAddress(), connections, trashed, inFlightQueries, maxLoad));
			            }
			        };
					//
				} catch (Exception e) {
					log.error("StorageSessionMonitorTimer error : " + e.getMessage(), e);
				}
			}

		}, 1000 * 60 * 1, TIMER_PERIOD, TimeUnit.MILLISECONDS);
	}

	public void stop() {
		task.cancel(true);
	}

	
	public List<String> getConnectedHosts(){
		return hosts;
	}
	
	
}
