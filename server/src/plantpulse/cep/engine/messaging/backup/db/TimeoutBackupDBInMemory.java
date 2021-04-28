package plantpulse.cep.engine.messaging.backup.db;

import java.io.File;
import java.util.concurrent.atomic.AtomicLong;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.redisson.api.RQueue;

import plantpulse.cep.engine.inmemory.redis.RedisInMemoryClient;
import plantpulse.cep.engine.messaging.backup.TimeoutBackupDB;
import plantpulse.cep.engine.messaging.listener.MessageListenerStatus;

public class TimeoutBackupDBInMemory implements TimeoutBackupDB {

	private static final Log log = LogFactory.getLog(TimeoutBackupDBInMemory.class);

	public static final String TYPE = "IN-MEMORY"; //

	private AtomicLong count = new AtomicLong();
	
	private RQueue<String> queue;

	public void init() {
		try {
			queue = RedisInMemoryClient.getInstance().getMessaingTimeoutQueue();
			log.info("Timeout message backup db inited. type=[IN_MEMORY]");

		} catch (Exception e) {
			log.error("Timeout message backup db init failed : " + e.getMessage(), e);
		}
	};

	public Object getDb() {
		return queue;
	}


	public void add(String in) {
		queue.add(in);
		MessageListenerStatus.TIMEOUT_BACKUPED_TOTAL_COUNT.incrementAndGet();
	}
	
	public String poll() {
		return queue.poll();
	}

	public File getFile() {
		return null;
	}
	
	public void close() {
		//
	}

	public String getType() {
		return TYPE;
	}
	
	public int size() {
		return queue.size();
	}
	
	public long diskSize() {
		return queue.sizeInMemory();
	}

}
