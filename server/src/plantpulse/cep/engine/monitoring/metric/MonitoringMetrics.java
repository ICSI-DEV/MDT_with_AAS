package plantpulse.cep.engine.monitoring.metric;

import com.codahale.metrics.Counter;
import com.codahale.metrics.Histogram;
import com.codahale.metrics.Meter;
import com.codahale.metrics.Timer;

import plantpulse.cep.engine.CEPEngineManager;

/**
 * MonitoringMetrics
 * @author leesa
 *
 */
public class MonitoringMetrics {
	
	/**
	 * getCounter
	 * @param name
	 * @return
	 */
	public static Counter getCounter(String name) {
		return CEPEngineManager.getInstance().getPipeline_metric_registry().getRegistry().getCounters().get(name);
	}
	
	/**
	 * getMeter
	 * @param name
	 * @return
	 */
	public static Meter getMeter(String name) {
		return CEPEngineManager.getInstance().getPipeline_metric_registry().getRegistry().getMeters().get(name);
	}
	
	/**
	 * getTimer
	 * @param name
	 * @return
	 */
	public static Timer getTimer(String name) {
		return CEPEngineManager.getInstance().getPipeline_metric_registry().getRegistry().getTimers().get(name);
	}
	
	/**
	 * getHistogram
	 * @param name
	 * @return
	 */
	public static Histogram getHistogram(String name) {
		return CEPEngineManager.getInstance().getPipeline_metric_registry().getRegistry().getHistograms().get(name);
	}

}
