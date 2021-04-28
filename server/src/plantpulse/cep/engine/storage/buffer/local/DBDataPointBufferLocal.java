
package plantpulse.cep.engine.storage.buffer.local;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.BlockingQueue;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import plantpulse.buffer.db.DBDataPoint;
import plantpulse.cep.engine.storage.buffer.DBDataPointBuffer;
import plantpulse.cep.engine.storage.insert.BatchProperties;
import plantpulse.fast.concurrent.DisruptorBlockingQueue;


/**
 * 로컬 캐쉬
 * @author leesa
 *
 */
public final class DBDataPointBufferLocal  implements DBDataPointBuffer  {

	private static Log log = LogFactory.getLog(DBDataPointBufferLocal.class);

	private static final int _QUEUE_SIZE = BatchProperties.getInt("batch.buffer_queue_size");

	private static final int DRAIN_LIMIT_SIZE = BatchProperties.getInt("batch.buffer_drain_limit_size");

	private BlockingQueue<DBDataPoint> db_data_buffer_queue = new DisruptorBlockingQueue<>(1024); //new LinkedBlockingQueue<DBDataPoint>(_QUEUE_SIZE);

	private static class DBDataBufferHolder {
		static DBDataPointBufferLocal instance = new DBDataPointBufferLocal();
	}

	public static DBDataPointBufferLocal getInstance() {
		return DBDataBufferHolder.instance;
	}

	public void init() {
		db_data_buffer_queue = null;
		db_data_buffer_queue = new DisruptorBlockingQueue<DBDataPoint>(_QUEUE_SIZE);
	}

	public List<DBDataPoint> getDBDataList() throws Exception {
		List<DBDataPoint> batch = new ArrayList<DBDataPoint>();
		//
		db_data_buffer_queue.drainTo(batch, DRAIN_LIMIT_SIZE);
		if (db_data_buffer_queue.size() >= _QUEUE_SIZE) {
			log.warn("DB data buffer queue is full : size=[" + _QUEUE_SIZE + "]");
		}
		return batch;
	}

	public void add(DBDataPoint dd) throws Exception {
		db_data_buffer_queue.add(dd);
	}

	public void addList(List<DBDataPoint> list) throws Exception {
		db_data_buffer_queue.addAll(list);
	}

	public long size() {
		if (db_data_buffer_queue == null) {
			return 0;
		}
		return db_data_buffer_queue.size();
	}
	
	public void stop() {
		db_data_buffer_queue.clear();
	}

}
