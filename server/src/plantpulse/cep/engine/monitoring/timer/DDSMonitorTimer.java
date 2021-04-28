package plantpulse.cep.engine.monitoring.timer;

import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.json.JSONObject;

import com.codahale.metrics.Meter;

import plantpulse.cep.engine.monitoring.MonitorConstants;
import plantpulse.cep.engine.monitoring.metric.MetricNames;
import plantpulse.cep.engine.monitoring.metric.MonitoringMetrics;
import plantpulse.cep.engine.timer.TimerExecuterPool;
import plantpulse.cep.listener.ResultServiceManager;
import plantpulse.cep.listener.push.PushClient;

/**
 * DDSMonitorTimer
 * @author leesa
 *
 */
public class DDSMonitorTimer implements MonitoringTimer {

	private static final Log log = LogFactory.getLog(DDSMonitorTimer.class);

	private static final int TIMER_PERIOD = (1000 * 1); // 1초마다 실행
	
	private ScheduledFuture<?> task;

	public void start() {
		
		task = TimerExecuterPool.getInstance().getPool().scheduleAtFixedRate(new Runnable() {

			@Override
			public void run() {

				try {
					//
					Meter dds_processed = MonitoringMetrics.getMeter(MetricNames.DDS_PROCESSED);
					
					//
					JSONObject json = new JSONObject();
					json.put("timestamp",    System.currentTimeMillis());
					json.put("processed",   (int) Math.round(dds_processed.getOneMinuteRate()));

					//
					PushClient client1 = ResultServiceManager.getPushService().getPushClient(MonitorConstants.PUSH_URL_DDS);
					client1.sendJSON(json);
					
					
				} catch (Exception e) {
					log.error(e, e);
				}

			}

		}, 1000 * 60 * 1, TIMER_PERIOD, TimeUnit.MILLISECONDS);
	}

	public void stop() {
		if(task != null) task.cancel(true);
	}

}
