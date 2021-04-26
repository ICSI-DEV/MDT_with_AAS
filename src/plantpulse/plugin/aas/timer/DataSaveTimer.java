package plantpulse.plugin.aas.timer;

import java.util.List;
import java.util.Timer;
import java.util.TimerTask;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import plantpulse.domain.Asset;
import plantpulse.plugin.aas.server.AASServer;

public class DataSaveTimer  {
	
	private static final Log log = LogFactory.getLog(DataSaveTimer.class);
	
	private AASServer server;
	private List<Asset> asset_list;
	
	private Timer timer;
	
	private static final int _TIER_DELAY    = 1 * 1000; //
	private static final int _TIER_INTERVAL = 10 * 1000; //
	
	public DataSaveTimer(AASServer server, List<Asset> asset_list) {
		this.server = server;
		this.asset_list = asset_list;
	}
	
	/**
	 * start
	 * @throws Exception
	 */
	public void start() throws Exception {
		timer = new Timer("PP_DATA_SAVE_FIND_TIMER");
		timer.scheduleAtFixedRate(new TimerTask() {
			@Override
			public void run() {
				try {
				
				}catch(Exception ex) {
					log.error(ex.getMessage(), ex);
				}
			}
		}, _TIER_DELAY, _TIER_INTERVAL);
	}
	
	/**
	 * stop
	 */
	public void stop() {
		if(timer != null) timer.cancel();
	}
	
}
