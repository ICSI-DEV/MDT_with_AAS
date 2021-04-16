package plantpulse.cep.engine.monitoring.jmx;

import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import plantpulse.cep.engine.CEPEngineManager;
import plantpulse.cep.engine.storage.timeseries.TimeSeriesDatabase;
import plantpulse.cep.engine.timer.TimerExecuterPool;

/**
 * JMXToTimeseriesDBSyncTimer
 * 
 * @author leesa
 *
 */
public class JMXToTimeseriesDBSyncTimer{

	private static final Log log = LogFactory.getLog(JMXToTimeseriesDBSyncTimer.class);

	private static final int TIMER_PERIOD = (1000); // 10분마다 실행

	private ScheduledFuture<?> task;
	
	
	public JMXToTimeseriesDBSyncTimer() {
		//
		//timeseries_sync.connect();
	};

	public void start() {
		
		task = TimerExecuterPool.getInstance().getPool().scheduleAtFixedRate(new Runnable() {

			@Override
			public void run() {
				//
				try {
					
					TimeSeriesDatabase tsdb = CEPEngineManager.getInstance().getTimeseries_database();
					PlantPulse jmx  = new PlantPulse();
					tsdb.syncJMX(jmx);
					
				} catch (Exception e) {
					log.error("JMX system performance mectrics to timeseries db insert failed : " + e.getMessage(), e);
				}
			}

		}, 1000 * 10, TIMER_PERIOD, TimeUnit.MILLISECONDS);
	}

	public void stop() {
		task.cancel(true);
	}

}
