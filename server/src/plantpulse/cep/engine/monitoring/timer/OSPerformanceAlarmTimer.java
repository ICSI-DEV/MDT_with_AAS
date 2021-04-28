package plantpulse.cep.engine.monitoring.timer;

import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import plantpulse.cep.engine.diagnostic.DiagnosticHandler;
import plantpulse.cep.engine.monitoring.event.OSPerformance;
import plantpulse.cep.engine.os.OSPerformanceUtils;
import plantpulse.cep.engine.timer.TimerExecuterPool;
import plantpulse.cep.service.support.alarm.AlarmPushService;
import plantpulse.cep.service.support.alarm.AlarmPushServiceImpl;
import plantpulse.diagnostic.Diagnostic;

/**
 * OSPerformanceAlarmTimer
 * 
 * @author lenovo
 *
 */
public class OSPerformanceAlarmTimer  implements MonitoringTimer {

	private static final Log log = LogFactory.getLog(OSPerformanceAlarmTimer.class);

	private static final int TIMER_PERIOD = (1000 * 60) * 10; // 10분마다 실행
	
	//쓰레드 갯수 제한
	private static final int LIMIT_THREAD_COUNT = 5_000;

	private ScheduledFuture<?> task;

	public void start() {
		
		task = TimerExecuterPool.getInstance().getPool().scheduleAtFixedRate(new Runnable() {

			@Override
			public void run() {

				//
				try {
					//
					AlarmPushService alarm_service = new AlarmPushServiceImpl();

					//
					OSPerformance performance = OSPerformanceUtils.getOSPerformance();
					String used_cpu = Math.round(performance.getUsed_cpu_percent()) + "%";
					String used_memory = Math.round(performance.getUsed_memory_percent()) + "%";

					// CPU 과다 사용 경고
					/*
					 * if(Math.round(performance.getUsed_cpu_percent()) >= 50 &&
					 * Math.round(performance.getUsed_cpu_percent()) < 80){
					 * alarm_service.push( new Date(),
					 * AlarmPushService.PRIORITY_WARN,
					 * "[시스템 모니터] 현재 시스템에서 CPU를 " + used_cpu + " 이상 사용중입니다.",
					 * "admin"); }
					 * 
					 * if(Math.round(performance.getUsed_cpu_percent()) >= 90){
					 * alarm_service.push( new Date(),
					 * AlarmPushService.PRIORITY_ERROR,
					 * "[시스템 모니터] 현재 시스템에서 CPU를 " + used_cpu + " 이상 사용중입니다.",
					 * "admin"); }
					 * 
					 * //메모리 과다 사용 경고
					 * if(Math.round(performance.getUsed_memory_percent()) >=
					 * 90){ alarm_service.push( new Date(),
					 * AlarmPushService.PRIORITY_WARN,
					 * "[시스템 모니터] 현재 시스템에서 메모리를 " + used_memory + " 이상 사용중입니다.",
					 * "admin"); }
					 * 
					 * //자바 힙 메모리 경고 if(JavaMemoryUtils.getUsedJavaRamPercent()
					 * >= 90) { alarm_service.push( new Date(),
					 * AlarmPushService.PRIORITY_WARN,
					 * "[시스템 모니터] 현재 자바 가상 머신에서 메모리를 " +
					 * JavaMemoryUtils.getUsedJavaRamPercent() + "% 이상 사용중입니다.",
					 * "admin"); }
					 */

					/*
					 * PooledConnectionManager manager =
					 * PooledConnectionManager.getInstance(); //커넥션 누수 경고
					 * if(manager.getPool().getNumberOfBusyConnections() > 10){
					 * alarm_service.push( new Date(),
					 * AlarmPushService.PRIORITY_ERROR,
					 * "[시스템 모니터] 데이터베이스 커넥션 풀에 사용중인 커넥션이 " +
					 * manager.getPool().getNumberOfBusyConnections() +
					 * "개가 넘습니다. 애플리케이션 코드에 커넥션 누수가 있는지 확인하십시오.", "admin"); }
					 * 
					 * //가용 커넥션 확인 경고
					 * if(manager.getPool().getNumberOfAvailableConnections() <
					 * 10){ alarm_service.push( new Date(),
					 * AlarmPushService.PRIORITY_WARN,
					 * "[시스템 모니터] 데이터베이스 커넥션 풀에 가용한 커넥션이 " +
					 * manager.getPool().getNumberOfAvailableConnections() +
					 * "개 밖에 남지않았습니다.", "admin"); }
					 */
					
					 long active_thread_count = java.lang.Thread.activeCount();
					 if(active_thread_count > LIMIT_THREAD_COUNT){
						 DiagnosticHandler.handleFormLocal(
								 System.currentTimeMillis(), 
								 Diagnostic.LEVEL_WARN, 
								 "현재 활동중인 쓰레드가 [" + active_thread_count + "] > 제한 " + LIMIT_THREAD_COUNT + "개 이상입니다."
								 );
					 }
					
					//
				} catch (Exception e) {
					log.error(e);
				}

			}

		}, 1000 * 60 * 1, TIMER_PERIOD, TimeUnit.MILLISECONDS);
	}

	public void stop() {
		task.cancel(true);
	}

}
