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
 * PointStreamCountMonitorTimer
 * @author leesa
 *
 */
public class PointStreamCountMonitorTimer implements MonitoringTimer {

	private static final Log log = LogFactory.getLog(PointStreamCountMonitorTimer.class);

	private static final int TIMER_PERIOD = (1000 * 1); // 1초마다 실행
	
	private ScheduledFuture<?> task;

	public void start() {
		
		task = TimerExecuterPool.getInstance().getPool().scheduleAtFixedRate(new Runnable() {

			@Override
			public void run() {

				try {
					//
					Meter stream_processed = MonitoringMetrics.getMeter(MetricNames.STREAM_PROCESSED);
					Meter message_timeout = MonitoringMetrics.getMeter(MetricNames.MESSAGE_TIMEOUT);
					
					//
					JSONObject json = new JSONObject();
					json.put("timestamp",    System.currentTimeMillis());
					json.put("point_count",   (int) Math.round(stream_processed.getOneMinuteRate()));
					json.put("timeout_count", (int) Math.round(message_timeout.getOneMinuteRate()));
					json.put("cpu_latency",   0);

					//
					PushClient client1 = ResultServiceManager.getPushService().getPushClient(MonitorConstants.PUSH_URL_TAG_POINT_METRIC);
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
