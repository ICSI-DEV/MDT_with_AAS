package plantpulse.cep.engine.monitoring.timer;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.json.JSONObject;

import plantpulse.cep.engine.logging.EngineLogger;
import plantpulse.cep.engine.monitoring.MonitorConstants;
import plantpulse.cep.engine.stream.processor.PointStreamStastics;
import plantpulse.cep.engine.timer.TimerExecuterPool;
import plantpulse.cep.engine.utils.TextUtils;
import plantpulse.cep.listener.ResultServiceManager;
import plantpulse.cep.listener.push.PushClient;

/**
 * PointStreamTimeoutMonitorTimer
 * 
 * @author lsb
 *
 */
public class PointStreamTimeoutMonitorTimer  {

	private static Log log = LogFactory.getLog(PointStreamTimeoutMonitorTimer.class);

	private ScheduledFuture<?> check_timer;
	private ScheduledFuture<?> log_timer;
	
	private AtomicLong timeout_count = new AtomicLong(0);
	
	public PointStreamTimeoutMonitorTimer() {
		
	}

	public void start() {

		//1. 타임아웃 건수 체크 타이머 사직
		check_timer = TimerExecuterPool.getInstance().getPool().scheduleAtFixedRate(new Runnable() {
			@Override
			public void run() {
				try {
					
					//1. 타임아웃 카운터 모니터링
					if(PointStreamStastics.TIMEOVER_COUNT.get() > 0){
						List<String> timeover_tag_list = new ArrayList<>();
						PointStreamStastics.TIMEOVER_POINT_LIST.drainTo(timeover_tag_list);
						log.warn("STREAMING_TIMOEVER_TAG_ARRAY [" + timeover_tag_list.size() + "] = " + TextUtils.min(timeover_tag_list.toString()));
						//
						timeout_count.addAndGet(timeover_tag_list.size());
					 };
					 
					 //2. 푸시
					JSONObject json = new JSONObject();
					json.put("timeout_count", PointStreamStastics.TIMEOVER_COUNT.get());
					PushClient client1 = ResultServiceManager.getPushService().getPushClient(MonitorConstants.PUSH_URL_STREAM_TIMEOUT);
					client1.sendJSON(json);
					 
					//3. 초기화
					PointStreamStastics.TIMEOVER_COUNT.set(0);
					PointStreamStastics.TIMEOVER_POINT_LIST = new LinkedBlockingQueue<>();
					 
					
				} catch (Exception e) {
					log.error("Point stream timeout monitoring failed : " + e.getMessage(), e);
				} finally {

				}
			}
		}, 60 * 1000, 1000 * 1, TimeUnit.MILLISECONDS); //
		
		
		//2. 타임아웃 로그 타이머 (로그는 1분에 한번씩 프린트)
		log_timer = TimerExecuterPool.getInstance().getPool().scheduleAtFixedRate(new Runnable() {
			@Override
			public void run() {
				try {
					//1. 타임아웃 카운터 모니터링
					if(timeout_count.get() > 0){
						EngineLogger.warn("스트리밍 타임오버 기준인 [" + PointStreamStastics.CEP_TIMEOVER_MS + "]밀리초를 초과한 포인트 [" + timeout_count.get() +"]개를 건너뛰기하였습니다.");
						timeout_count.set(0); //타임아웃 건수 초기화
					};
				} catch (Exception e) {
					log.error("Point stream timeout log print failed : " + e.getMessage(), e);
				} finally {

				}
			}
		}, 60 * 1000, 1000 * 60, TimeUnit.MILLISECONDS); //
		
	}

	public void stop() {
		if(log_timer != null) log_timer.cancel(true);
		if(check_timer != null) check_timer.cancel(true);
	}

}
