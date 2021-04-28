package plantpulse.cep.engine.stream.type.queue;

import org.redisson.api.RQueue;

import plantpulse.buffer.db.DBDataPoint;
import plantpulse.cep.engine.stream.queue.cep.CEPDataPointQueue;
import plantpulse.cep.engine.stream.queue.cep.RedisCEPDataPointQueue;
import plantpulse.cep.engine.stream.queue.db.DBDataPointQueue;
import plantpulse.cep.engine.stream.queue.db.RedisDBDataPointQueue;
import plantpulse.cep.engine.stream.type.distruptor.PointDistruptorable;



/**
 * PointQueueFactory
 * @author leesa
 *
 */
public class PointQueueFactory {
	
	/**
	 * DBDataPointQueue
	 * @param client_id
	 * @return
	 */
	public static DBDataPointQueue<RQueue<DBDataPoint>> getDBDataPointQueue(String client_id) {
		return new RedisDBDataPointQueue(client_id);
	}
	
	
	/**
	 * CEPDataPointQueue
	 * @param client_id
	 * @return
	 */
	public static CEPDataPointQueue<RQueue<PointDistruptorable>> getCEPDataPointQueue(String client_id) {
		return new RedisCEPDataPointQueue(client_id);
	}

}
