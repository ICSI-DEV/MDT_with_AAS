package plantpulse.cep.engine.storage.insert;

import org.redisson.api.RAtomicLong;

import plantpulse.cep.engine.inmemory.redis.RedisInMemoryClient;

/**
 * BatchTimerStastics
 * 
 * @author leesa
 *
 */
public class BatchTimerStastics {

	private static class BatchTimerStasticsHolder {
		static BatchTimerStastics instance = new BatchTimerStastics();
	}

	public static BatchTimerStastics getInstance() {
		return BatchTimerStasticsHolder.instance;
	}

	private RedisInMemoryClient client = RedisInMemoryClient.getInstance();

	public void init() {
		//
	}

	/**
	 * 타이머 활성화 건수
	 * 
	 * @return
	 */
	public RAtomicLong getTIMER_ACTIVE() {
		return client.getBatchStastics("BATCH_STASTICS:TIMER_ACTIVE");
	}

	/**
	 * 시작된 배치 건수
	 * 
	 * @return
	 */
	public RAtomicLong getBATCH_STARTED() {
		return client.getBatchStastics("BATCH_STASTICS:BATCH_STARTED");
	}

	/**
	 * 배치 활성화 건수
	 * 
	 * @return
	 */
	public RAtomicLong getBATCH_ACTIVE() {
		return client.getBatchStastics("BATCH_STASTICS:BATCH_ACTIVE");
	}

	/**
	 * 배치 완료 건수
	 * 
	 * @return
	 */
	public RAtomicLong getBATCH_COMPLETED() {
		return client.getBatchStastics("BATCH_STASTICS:BATCH_COMPLETED");
	}

	/**
	 * 배치 처리 시간 총합
	 * 
	 * @return
	 */
	public RAtomicLong getTOTAL_BATCH_PROCESS_TIME_MS() {
		return client.getBatchStastics("BATCH_STASTICS:TOTAL_BATCH_PROCESS_TIME_MS");
	}

	/**
	 * 전체 배치 저장 포인트 건수(시스템 부팅 시점 부터)
	 * 
	 * @return
	 */
	public RAtomicLong getTOTAL_BATCH_PROCESS_COUNT() {
		return client.getBatchStastics("BATCH_STASTICS:TOTAL_BATCH_PROCESS_COUNT");
	}

	/**
	 * 현재까지 저장된 전체 데이터 포인트 건수
	 * 
	 * @return
	 */
	public RAtomicLong getTOTAL_SAVED_PROCESS_COUNT() {
		return client.getBatchStastics("BATCH_STASTICS:TOTAL_SAVED_PROCESS_COUNT");
	}

	/**
	 * 이전 저장 데이터 포인트 건수
	 * 
	 * @return
	 */
	public RAtomicLong getPREV_BATCH_PROCESS_COUNT() {
		return client.getBatchStastics("BATCH_STASTICS:PREV_BATCH_PROCESS_COUNT");
	}

}
