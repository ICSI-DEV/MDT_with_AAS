package plantpulse.cep.engine.eventbus;

import java.util.List;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

/**
 * DomainChangeQueue
 * 
 * @author lsb
 *
 */
public class DomainChangeQueue {

	private static Log log = LogFactory.getLog(DomainChangeQueue.class);

	private static final int _QUEUE_SIZE = 1024 * 1000 * 60;

	private BlockingQueue<String> queue = new LinkedBlockingQueue<String>(_QUEUE_SIZE);

	private static class DomainChangeQueueHolder {
		static DomainChangeQueue instance = new DomainChangeQueue();
	}

	public static DomainChangeQueue getInstance() {
		return DomainChangeQueueHolder.instance;
	}

	public void init() {
		queue = null;
		queue = new LinkedBlockingQueue <String>(_QUEUE_SIZE);
	}

	public String poll() throws Exception {
		return queue.poll();
	}

	public void add(String data) throws Exception {
		queue.add(data);
	}

	public void addList(List<String> list) throws Exception {
		queue.addAll(list);
	}

	public long size() {
		if (queue == null) {
			return 0;
		}
		return queue.size();
	}

	public BlockingQueue <String> getQueue() {
		return queue;
	}


}