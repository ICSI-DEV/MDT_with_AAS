package plantpulse.cep.engine.monitoring.timer;

import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.json.JSONObject;

import com.codahale.metrics.Histogram;
import com.codahale.metrics.Snapshot;

import plantpulse.cep.Metadata;
import plantpulse.cep.engine.monitoring.MonitorConstants;
import plantpulse.cep.engine.monitoring.metric.MetricNames;
import plantpulse.cep.engine.monitoring.metric.MonitoringMetrics;
import plantpulse.cep.engine.network.Latency;
import plantpulse.cep.engine.timer.TimerExecuterPool;
import plantpulse.cep.listener.ResultServiceManager;
import plantpulse.cep.listener.push.PushClient;

/**
 * NetworkLatencyTimer
 * @author leesa
 *
 */
public class NetworkLatencyTimer implements MonitoringTimer {

	private static final Log log = LogFactory.getLog(NetworkLatencyTimer.class);

	private static final int TIMER_PERIOD = (1000 * 1); // 1초마다 실행
	
	private ScheduledFuture<?> task;

	public void start() {
		
		task = TimerExecuterPool.getInstance().getPool().scheduleAtFixedRate(new Runnable() {

			@Override
			public void run() {

				try {
					
					Histogram histogram = MonitoringMetrics.getHistogram(MetricNames.MEESSAGE_LATENCY);
					Snapshot snap = histogram.getSnapshot();
					
					//
					JSONObject json = new JSONObject();
				
					json.put("avg_latency",  snap.getMean());
					Latency.AVG.set(snap.getMedian());
					
					json.put("min_latency",  snap.getMin());
					Latency.MIN.set(snap.getMin());
					
					json.put("max_latency",  snap.getMax());
					Latency.MAX.set(snap.getMax());
					
					json.put("percentile_75_latency",  snap.get75thPercentile());
					Latency.MAX.set(snap.get75thPercentile());
					
					json.put("percentile_99_latency",  snap.get99thPercentile());
					Latency.MAX.set(snap.get99thPercentile());
					
					json.put("percentile_999_latency",  snap.get999thPercentile());
					Latency.MAX.set(snap.get999thPercentile());
						
				    
					//
					PushClient client1 = ResultServiceManager.getPushService().getPushClient(
							MonitorConstants.PUSH_URL_NETWORK_LATENCY + "/" + Metadata.CEP_SERVICE_ID);
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
