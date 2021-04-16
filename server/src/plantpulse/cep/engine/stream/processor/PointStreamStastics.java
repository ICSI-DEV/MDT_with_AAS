package plantpulse.cep.engine.stream.processor;

import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.atomic.AtomicLong;

import com.google.common.util.concurrent.AtomicDouble;

/**
 * PointStreamStastics
 * 
 * @author leesa
 *
 */
public class PointStreamStastics {

	//전체 처리 건수 및 시간, 활동 쓰레드
	public static AtomicLong PROCESSED_COUNT = new AtomicLong(0);
	public static AtomicLong PROCESSED_TIME  = new AtomicLong(0);
	public static AtomicLong RUNNING_THREAD  = new AtomicLong(0);
		
	// 타임아웃 설정
	public static AtomicLong MESSAGING_LATENCY_WARNING_MS = new AtomicLong(0);
	public static AtomicLong MESSAGING_LATENCY_TIMEOUT_MS = new AtomicLong(0);
	public static AtomicLong CEP_TIMEOVER_MS = new AtomicLong(0);
	
	// CEP 타임아웃 포인트 건수
	public static LinkedBlockingQueue<String> TIMEOVER_POINT_LIST = new LinkedBlockingQueue<String>();
	public static AtomicLong TIMEOVER_COUNT = new AtomicLong(0);
	
	// DISTRUPTOR 모드일 경우
	public static AtomicLong REMAINING_CAPACITY = new AtomicLong(0);

	// QUEUE 모드일 경우
	public static AtomicLong PENDING_QUEUE_COUNT = new AtomicLong(0);
	
	//@Deprecated
	public static AtomicDouble PROCESSED_COUNT_BY_SEC = new AtomicDouble(0.0);

}