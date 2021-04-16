package plantpulse.cep.engine.stream.queue.db;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.redisson.api.RQueue;

import plantpulse.buffer.db.DBDataPoint;
import plantpulse.cep.engine.inmemory.redis.RedisInMemoryClient;

/**
 * RedisDBDataPointQueue
 * 
 * @author leesa
 *
 */
public class RedisDBDataPointQueue implements DBDataPointQueue<RQueue<DBDataPoint>> {

	private static final Log log = LogFactory.getLog(RedisDBDataPointQueue.class);


	private String source = "";
	private RQueue<DBDataPoint> queue = null;
	public RedisDBDataPointQueue(String source) {
		this.source = source;
		//
		RedisInMemoryClient client = RedisInMemoryClient.getInstance();
		queue = client.getDBQueue();
		log.debug("Point queue initailized from source=[" + source + "], address=[" + client.getAddress() + "]");
	}

	
	@Override
	public String getSource() {
		return source;
	}


	@Override
	public RQueue<DBDataPoint> getQueue() {
		return queue;
	};
	


}
