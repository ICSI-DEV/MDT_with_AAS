package plantpulse.cep.engine.messaging.backup;

import plantpulse.cep.engine.messaging.backup.db.TimeoutBackupDBFileQueue;
import plantpulse.cep.engine.messaging.backup.db.TimeoutBackupDBInMemory;
import plantpulse.cep.engine.messaging.backup.timer.TimeoutBackupReProcessTimerFileQueue;
import plantpulse.cep.engine.messaging.backup.timer.TimeoutBackupReProcessTimerInMemory;
import plantpulse.cep.engine.monitoring.metric.MetricNames;
import plantpulse.cep.engine.monitoring.metric.MonitoringMetrics;
import plantpulse.cep.engine.properties.PropertiesLoader;

/**
 * <B> 메세지 백업 </B>
 * 
 * <pre>
 * 현재 쓰레드 풀링 시스트템이 BUSY 하거나, 스트리밍 타임오버 기준을 넘은 데이터 포인트를 백업한다.
 * 백업된 데이터 포인트는 타이머에 의해서 재-처리한다.
 * </pre>
 * @author leesangboo
 *
 */
public class TimeoutBackup {
	
	private TimeoutBackupDB bdb;
	private TimeoutBackupReProcessTimer reprocess_timer;
	
	public TimeoutBackup() {
		//
	};
	
	
	public void init() {
		//
		String store_type = PropertiesLoader.getEngine_properties().getProperty("engine.streaming.messaging.timeover.store.type", TimeoutBackupTypes.FILE_QUEUE);
		switch(store_type) {
	    case TimeoutBackupTypes.FILE_QUEUE: bdb = new TimeoutBackupDBFileQueue(); reprocess_timer = new TimeoutBackupReProcessTimerFileQueue(bdb);
	         break;
	    case TimeoutBackupTypes.IN_MEMORY: bdb = new TimeoutBackupDBInMemory(); reprocess_timer = new TimeoutBackupReProcessTimerInMemory(bdb);
	         break;
	    default: bdb = new TimeoutBackupDBFileQueue(); reprocess_timer = new TimeoutBackupReProcessTimerFileQueue(bdb);
	         break;
		};

		//백업 로직 초기화
		bdb.init();
	    //재처리 프로세서 타이머 시작
		reprocess_timer.start();
	}
	
	public void storeTimeoutMessage(String in) {
		try {
			bdb.add(in);
		} finally {
			MonitoringMetrics.getMeter(MetricNames.MESSAGE_TIMEOUT).mark();
		}
	}

}
