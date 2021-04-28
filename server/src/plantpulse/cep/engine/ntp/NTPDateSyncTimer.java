package plantpulse.cep.engine.ntp;

import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import plantpulse.cep.engine.properties.PropertiesLoader;
import plantpulse.cep.engine.timer.TimerExecuterPool;

/**
 * NTPDateSyncTimer
 * 
 * @author leesa
 *
 */
public class NTPDateSyncTimer {

	private static final Log log = LogFactory.getLog(NTPDateSyncTimer.class);

	private static final int TIMER_PERIOD = (1000 * 60 * 10); // 10분 마다 실행

	private ScheduledFuture<?> timer;

	public void start() {
		
		boolean USE_NTP_SYNC = Boolean.parseBoolean(PropertiesLoader.getEngine_properties().getProperty("engine.ntpdate.sync", "false"));
		String NTP_SERVER_IP = PropertiesLoader.getEngine_properties().getProperty("engine.ntpdate.server.ip", "time.windows.com");
		
		if(USE_NTP_SYNC) {
			timer = TimerExecuterPool.getInstance().getPool().scheduleAtFixedRate(new Runnable() {
			@Override
			public void run() {
				NTPSync sync = new NTPSync();
				sync.sync(NTP_SERVER_IP);
				log.debug("NTP Date sync complated.");
			}

		}, 0, TIMER_PERIOD, TimeUnit.MILLISECONDS);
		};
	}

	public void stop() {
		timer.cancel(true);
	};

}
