package plantpulse.cep.engine.monitoring.timer;

import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import plantpulse.cep.engine.logging.EngineLogger;
import plantpulse.cep.engine.monitoring.statistics.DataFlowErrorCount;
import plantpulse.cep.engine.timer.TimerExecuterPool;

/**
 * DataFlowErrorTimer
 * 
 * @author lsb
 *
 */
public class DataFlowErrorTimer implements MonitoringTimer  {

	private static final Log log = LogFactory.getLog(OSPerformanceAlarmTimer.class);

	private static final int TIMER_PERIOD = (1000 * 60) * 10; // 10분마다 실행

	private ScheduledFuture<?> task;

	public void start() {
		
		task = TimerExecuterPool.getInstance().getPool().scheduleAtFixedRate(new Runnable() {

			@Override
			public void run() {
				//
				try {
					
					if(DataFlowErrorCount.ERROR_KAFKA_SUBSCRIBE.get() > 0){
						EngineLogger.error(String.format("KAFKA 메세지 수신시 [%d]건의 오류가 발생하였습니다.", DataFlowErrorCount.ERROR_KAFKA_SUBSCRIBE.get()));
					};
					if(DataFlowErrorCount.ERROR_MQTT_SUBSCRIBE.get() > 0){
						EngineLogger.error(String.format("MQTT 메세지 수신시 [%d]건의 오류가 발생하였습니다.", DataFlowErrorCount.ERROR_MQTT_SUBSCRIBE.get()));
					};
					if(DataFlowErrorCount.ERROR_STOMP_SUBSCRIBE.get() > 0){
						EngineLogger.error(String.format("STOMP 메세지 수신시 [%d]건의 오류가 발생하였습니다.", DataFlowErrorCount.ERROR_STOMP_SUBSCRIBE.get()));
					};
					
					//
					if(DataFlowErrorCount.ERROR_MQTT_PUBLISH.get() > 0){
						EngineLogger.error(String.format("MQTT 메세지 발행시 [%d]건의 오류가 발생하였습니다.", DataFlowErrorCount.ERROR_MQTT_PUBLISH.get()));
					};
					if(DataFlowErrorCount.ERROR_STOMP_PUBLISH.get() > 0){
						EngineLogger.error(String.format("STOMP 메세지 발행시 [%d]건의 오류가 발생하였습니다.", DataFlowErrorCount.ERROR_STOMP_PUBLISH.get()));
					};
					
					
					//데이터 저장
					//
					if(DataFlowErrorCount.ERROR_STORE_POINT_INSERT.get() > 0){
						EngineLogger.error(String.format("태그 포인트 저장중 [%d]건의 오류가 발생하였습니다.", DataFlowErrorCount.ERROR_STORE_POINT_INSERT.get()));
					};
					if(DataFlowErrorCount.ERROR_STORE_POINT_MAP_INSERT.get() > 0){
						EngineLogger.error(String.format("태그 포인트 맵 저장중 [%d]건의 오류가 발생하였습니다.", DataFlowErrorCount.ERROR_STORE_POINT_MAP_INSERT.get()));
					};
					if(DataFlowErrorCount.ERROR_STORE_AGGREGATION_INSERT.get() > 0){
						EngineLogger.error(String.format("태그 포인트 집계 저장중 [%d]건의 오류가 발생하였습니다.", DataFlowErrorCount.ERROR_STORE_AGGREGATION_INSERT.get()));
					};
					if(DataFlowErrorCount.ERROR_STORE_SNAPSHOT_INSERT.get() > 0){
						EngineLogger.error(String.format("태그 포인트 스냅샷 저장중 [%d]건의 오류가 발생하였습니다.", DataFlowErrorCount.ERROR_STORE_SNAPSHOT_INSERT.get()));
					};
					if(DataFlowErrorCount.ERROR_STORE_TRIGGER_INSERT.get() > 0){
						EngineLogger.error(String.format("트리거 데이터 저장중 [%d]건의 오류가 발생하였습니다.", DataFlowErrorCount.ERROR_STORE_TRIGGER_INSERT.get()));
					};
					if(DataFlowErrorCount.ERROR_STORE_ASSET_INSERT.get() > 0){
						EngineLogger.error(String.format("에셋 데이터 저장중 [%d]건의 오류가 발생하였습니다.", DataFlowErrorCount.ERROR_STORE_ASSET_INSERT.get()));
					};
					if(DataFlowErrorCount.ERROR_STORE_ASSET_HEALTH_INSERT.get() > 0){
						EngineLogger.error(String.format("에셋 헬스 저장중 [%d]건의 오류가 발생하였습니다.", DataFlowErrorCount.ERROR_STORE_ASSET_HEALTH_INSERT.get()));
					};
					if(DataFlowErrorCount.ERROR_STORE_ALARM_INSERT.get() > 0){
						EngineLogger.error(String.format("알람 저장중 [%d]건의 오류가 발생하였습니다.", DataFlowErrorCount.ERROR_STORE_ALARM_INSERT.get()));
					};

					DataFlowErrorCount.rest();
					
					//
				} catch (Exception e) {
					log.error(e,e);
				}

			}

		}, 1000 * 60 * 1, TIMER_PERIOD, TimeUnit.MILLISECONDS);
	}

	public void stop() {
		task.cancel(true);
	}

}
