package plantpulse.cep.engine.storage;

import java.util.ArrayList;
import java.util.List;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import plantpulse.cep.engine.storage.buffer.DBDataPointBuffer;
import plantpulse.cep.engine.storage.buffer.DBDataPointBufferFactory;
import plantpulse.cep.engine.storage.insert.BatchTimer;
import plantpulse.cep.engine.storage.insert.BatchTimerStastics;
import plantpulse.cep.service.client.StorageClient;

public class BaseStorageProcessor implements StorageProcessor {

	private static final Log log = LogFactory.getLog(StorageProcessor.class);
	
	private StorageTableInitiallizer table = null;

	public static final int __BATCH_TIMER_SIZE = 1; // 배치 타이머 갯수

	private DBDataPointBuffer buffer = null;

	private List<BatchTimer> batch_timers = null;

	private StorageMonitor monitor = null;

	@Override
	public void init() {
		//
		try {
			
			//테이블 생성
			table = new StorageTableInitiallizer();
			table.init();
			
			//버퍼 생성
			buffer = DBDataPointBufferFactory.getDBDataBuffer();
			buffer.init();

			// 배치 통계 초기화
			StorageClient client = new StorageClient();
			
			//
			BatchTimerStastics.getInstance().init();
			BatchTimerStastics.getInstance().getTOTAL_SAVED_PROCESS_COUNT().set(client.forSelect().countPointTotal());
			BatchTimerStastics.getInstance().getTOTAL_BATCH_PROCESS_COUNT().set(0);
			BatchTimerStastics.getInstance().getBATCH_ACTIVE().set(0);
			BatchTimerStastics.getInstance().getTIMER_ACTIVE().set(0);
			BatchTimerStastics.getInstance().getBATCH_STARTED().set(0);
			BatchTimerStastics.getInstance().getBATCH_COMPLETED().set(0);

			//배치 시작
			batch_timers = new ArrayList<BatchTimer>();
			for (int i = 0; i < __BATCH_TIMER_SIZE; i++) {
				BatchTimer batch_timer = new BatchTimer(i);
				batch_timer.start();
				batch_timers.add(batch_timer);
			}
			;

			//모니터 시작
			monitor = new StorageMonitor();
			monitor.start();

			log.info("Storage processor has initialized.");
			
		} catch (Exception e) {
			log.info("Storage processor initiallize failed : " + e.getMessage(), e);
		}
	}

	@Override
	public DBDataPointBuffer getBuffer() {
		return buffer;
	}

	@Override
	public List<BatchTimer> getBatchs() {
		return batch_timers;
	};

	@Override
	public void stop() {
		if (monitor != null) monitor.stop();
		for (int i = 0; i < batch_timers.size(); i++) {
			batch_timers.get(i).stop();
		}
		buffer.stop();
	};

	@Override
	public StorageMonitor getMonitor() {
		return monitor;
	}

}
