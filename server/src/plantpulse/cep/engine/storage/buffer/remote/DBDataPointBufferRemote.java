package plantpulse.cep.engine.storage.buffer.remote;

import java.util.ArrayList;
import java.util.List;
import java.util.Timer;
import java.util.TimerTask;
import java.util.concurrent.ArrayBlockingQueue;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.redisson.api.RFuture;
import org.redisson.api.RQueue;

import plantpulse.buffer.db.DBDataPoint;
import plantpulse.cep.engine.monitoring.metric.MetricNames;
import plantpulse.cep.engine.monitoring.metric.MonitoringMetrics;
import plantpulse.cep.engine.storage.buffer.DBDataPointBuffer;
import plantpulse.cep.engine.storage.insert.BatchProperties;
import plantpulse.cep.engine.stream.queue.db.DBDataPointQueue;
import plantpulse.cep.engine.stream.type.queue.PointQueueFactory;

/**
 * 원격 레디스 캐시
 * @author leesa
 *
 */
public class DBDataPointBufferRemote implements DBDataPointBuffer {

	private static Log log = LogFactory.getLog(DBDataPointBufferRemote.class);

	private static final int _QUEUE_SIZE = BatchProperties.getInt("batch.buffer_queue_size");

	private static final int DRAIN_LIMIT_SIZE = BatchProperties.getInt("batch.buffer_drain_limit_size");
	
	private static final int BATCH_SIZE = DRAIN_LIMIT_SIZE;
	
	private static final String CLENT_ID = "DB_DATA_BUFFER_REMOTE";
	
	private static final int __IN_JVM_QUEUE_BATCH_DELAY    = 1_000;
	private static final int __IN_JVM_QUEUE_BATCH_INTERVAL = 500;

	private DBDataPointQueue<RQueue<DBDataPoint>> db_data_queue;

	
	private Timer timer = new Timer();
	
	private ArrayBlockingQueue<DBDataPoint> jvm_queue = new ArrayBlockingQueue<>(10_000_000);
	
	private static class DBDataBufferRemoteHolder {
		static DBDataPointBufferRemote instance = new DBDataPointBufferRemote();
	}

	public static DBDataPointBufferRemote getInstance() {
		return DBDataBufferRemoteHolder.instance;
	}

	public void init() {
		  db_data_queue = PointQueueFactory.getDBDataPointQueue(CLENT_ID);
		  clearanceTimer();
	};
	
	private void clearanceTimer() {
		//500ms에 한번씩 남은 데이터 클리어
		timer = new Timer("DB_DATA_BUFFER_REMOTE_CLEAR_JOB");
	     timer.scheduleAtFixedRate(new TimerTask() {
			@Override
			public void run() {
				int pedding = jvm_queue.size();
				if(pedding > 0 && pedding < BATCH_SIZE) {
					addBatch();
					log.debug("Clearing organized the items waiting in the queue. pedding=[" + pedding +"]");
				};
			}
	     }, __IN_JVM_QUEUE_BATCH_DELAY, __IN_JVM_QUEUE_BATCH_INTERVAL);
	};

	public List<DBDataPoint> getDBDataList() throws Exception {
		if (size() >= _QUEUE_SIZE) {
			log.warn("DB data buffer queue is full warnnig : size=[" + _QUEUE_SIZE + "]");
		};
	
		//
		RFuture<List<DBDataPoint>> future = db_data_queue.getQueue().pollAsync(DRAIN_LIMIT_SIZE);
		//
		int retry = 0;
		while(!future.isDone()) {
		    Thread.sleep(100);
		    retry++;
		};
		if(retry > 10) {
			log.warn("DB data buffer polling slow in queue : polling_time=[" + (retry*10) + "]ms");
		}
		return future.get();
	}

	public void add(DBDataPoint dd) throws Exception {
		jvm_queue.add(dd);
		if(jvm_queue.size() >= BATCH_SIZE) {
			addBatch();
		}
		MonitoringMetrics.getCounter(MetricNames.STORAGE_DB_BUFFER_QUEUE_PEDDING).inc();
	};
	
	private void addBatch() {
		List<DBDataPoint> list = new ArrayList<>();
		jvm_queue.drainTo(list, BATCH_SIZE);
		db_data_queue.getQueue().addAllAsync(list);
		MonitoringMetrics.getCounter(MetricNames.STORAGE_DB_BUFFER_QUEUE_PEDDING).dec(list.size());
	}
	
	public void addList(List<DBDataPoint> list) throws Exception {
		if(list != null) {
			db_data_queue.getQueue().addAllAsync(list);
			MonitoringMetrics.getCounter(MetricNames.STORAGE_DB_BUFFER).inc(list.size());;
		}
	}
	
	public long size() {
		if (db_data_queue.getQueue() == null) {
			return 0;
		}
		long size = 0;
		try {
		RFuture<Integer> future = db_data_queue.getQueue().sizeAsync();
		while(!future.isDone()) {
		   Thread.sleep(100);
		};
		size = future.get().longValue();
		}catch(Exception ex) {
			log.warn("DB data point queue size getting failed : " + ex.getMessage(), ex);
		}finally {
			//
		}
		return size;
	}
	
	public void stop() {
		timer.cancel();
	}

}
