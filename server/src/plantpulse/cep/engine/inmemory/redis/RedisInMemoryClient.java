package plantpulse.cep.engine.inmemory.redis;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.redisson.Redisson;
import org.redisson.api.RAtomicLong;
import org.redisson.api.RMap;
import org.redisson.api.RQueue;
import org.redisson.api.RTimeSeries;
import org.redisson.api.RedissonClient;
import org.redisson.codec.FstCodec;
import org.redisson.codec.LZ4Codec;
import org.redisson.config.Config;
import org.redisson.config.TransportMode;

import plantpulse.buffer.db.DBDataPoint;
import plantpulse.cep.engine.stream.type.distruptor.PointDistruptorable;
import plantpulse.event.opc.Point;

/**
 * RedisInMemoryClient
 * 
 * @author leesa
 *
 */
public class RedisInMemoryClient {
	
	private static final Log log = LogFactory.getLog(RedisInMemoryClient.class);
	
	private static class RedisQueueClientHolder {
		static RedisInMemoryClient instance = new RedisInMemoryClient();
	}

	public static RedisInMemoryClient getInstance() {
		return RedisQueueClientHolder.instance;
	}

	public static final String    REDIS_ADDRESS    = "redis://127.0.0.1:6379";
	
	
	private static final int CORE_COUNT = Runtime.getRuntime().availableProcessors();
	
	private static final int CON_MIN_IDLE_SIZE = CORE_COUNT * 1;
	
	private static final int CON_POOL_SIZE = CORE_COUNT * 6;
	
	private static final int THREAD_SIZE = CORE_COUNT * 6;
	
	private static final int NETTY_SIZE = CORE_COUNT * 4;
	
	private RedissonClient client = null;
	
	
	private boolean connected = false;
	
	public void connect() {
		if(!connected) {
			Config config = new Config();
			
			config
			.useSingleServer()
			.setClientName("PP-IN-MEMORY-CLIENT")
			.setAddress(REDIS_ADDRESS)
			
			
			.setConnectionMinimumIdleSize(CON_MIN_IDLE_SIZE)
			.setConnectionPoolSize(CON_POOL_SIZE)
			.setConnectTimeout(20_000)
			.setIdleConnectionTimeout(10_000)
			
			.setSubscriptionConnectionMinimumIdleSize(10)
			.setSubscriptionConnectionPoolSize(32)
			.setSubscriptionsPerConnection(5)

			.setKeepAlive(true)
			.setTcpNoDelay(true)
			.setRetryAttempts(10)
			.setRetryInterval(10_000)
			
			;
			
			//
			FstCodec fst_codec = new FstCodec();
			LZ4Codec codec = new LZ4Codec(fst_codec);
		    //
			config.setCodec(codec);
			config.setThreads(THREAD_SIZE);
			config.setNettyThreads(NETTY_SIZE);
			config.setTransportMode(TransportMode.EPOLL);
			
			//
			client = Redisson.create(config);
			
			//
			connected = true;
			//
			log.info("Redis in-memory client connected. address=[" + REDIS_ADDRESS  + "], config=[" + config.toString() + "]");
		}
	};
	
	public RQueue<String> getMessaingTimeoutQueue(){
		if(!connected) {
			connect();
		};
		return client.getBlockingQueue("PP:MESSAGING-TIMEOUT-QUEUE");
	}
	
	public RQueue<DBDataPoint> getDBQueue(){
		if(!connected) {
			connect();
		};
		return client.getQueue("PP:DB-QUEUE");
	}
	
	public RQueue<PointDistruptorable> getCEPQueue(){
		if(!connected) {
			connect();
		};
		return client.getBlockingQueue("PP:CEP-QUEUE");
	}
	
	public RMap<String, Point> getPointMap(){
		if(!connected) {
			connect();
		};
		return client.getMap("PP:POINT-MAP");
	}
	
	public RTimeSeries<Point> getPointTimeSeries(){
		if(!connected) {
			connect();
		};
		return client.getTimeSeries("PP:POINT-TIME-SERIES");
	}
	
	public RAtomicLong getBatchStastics(String stats_name){
		if(!connected) {
			connect();
		};
		return client.getAtomicLong("PP:" + stats_name);
	}
	
	public void shutdown() {
		if (client != null && !client.isShutdown()) {
			client.shutdown();
		};
		connected = false;
		log.info("Redis in-memory client shutdown.");
	};
	
	public String getAddress() {
		return REDIS_ADDRESS;
	}
	
	
}
