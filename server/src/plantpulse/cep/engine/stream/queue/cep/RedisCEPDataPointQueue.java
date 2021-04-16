package plantpulse.cep.engine.stream.queue.cep;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.redisson.api.RQueue;

import plantpulse.cep.engine.inmemory.redis.RedisInMemoryClient;
import plantpulse.cep.engine.stream.queue.db.RedisDBDataPointQueue;
import plantpulse.cep.engine.stream.type.distruptor.PointDistruptorable;

public class RedisCEPDataPointQueue  implements CEPDataPointQueue<RQueue<PointDistruptorable>> {

	private static final Log log = LogFactory.getLog(RedisDBDataPointQueue.class);


	private String source = "";
	private RQueue<PointDistruptorable> queue = null;

	public RedisCEPDataPointQueue(String source) {
		this.source = source;
		//
		RedisInMemoryClient client = RedisInMemoryClient.getInstance();
		queue = client.getCEPQueue();
		log.debug("Point queue initailized from source=[" + source + "], address=[" + client.getAddress() + "]");
	}

	public RQueue<PointDistruptorable> getQueue() {
		return queue;
	};
	
	public String getSource() {
		return source;
	};



}
