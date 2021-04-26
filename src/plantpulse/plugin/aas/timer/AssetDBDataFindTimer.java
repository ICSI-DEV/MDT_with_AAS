package plantpulse.plugin.aas.timer;

import java.util.List;
import java.util.Timer;
import java.util.TimerTask;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import plantpulse.domain.Asset;
import plantpulse.json.JSONArray;
import plantpulse.plugin.aas.dao.cassandra.CassandraDAO;
import plantpulse.plugin.aas.server.AASServer;

/**
 * AssetDBDataFindTimer
 * 
 * @author leesa
 *
 */
public class AssetDBDataFindTimer {
	
	private static final Log log = LogFactory.getLog(AssetDBDataFindTimer.class);
	
	private AASServer server;
	private List<Asset> asset_list;
	
	
	private Timer timer;
	
	private static final int _TIER_DELAY    = 1 * 1000; //
	private static final int _TIER_INTERVAL = 10 * 1000; //
	
	public AssetDBDataFindTimer(AASServer server, List<Asset> asset_list) {
		this.server = server;
		this.asset_list = asset_list;
	}
	
	/**
	 * start
	 * @throws Exception
	 */
	public void start() throws Exception {
		timer = new Timer("PP_CASSANDRA_ASSET_DB_DATA_FIND_TIMER");
		timer.scheduleAtFixedRate(new TimerTask() {
			@Override
			public void run() {
				try {
					long start = System.currentTimeMillis();
					CassandraDAO dao = new CassandraDAO();
					for(int i=0; i < asset_list.size(); i++) {
						long timestamp = System.currentTimeMillis();
						Asset asset = asset_list.get(i);
						JSONArray asset_alarm_list = dao.getAlarmList(asset.getAsset_id());
						JSONArray asset_timeline_list = dao.getTimelineList(asset.getAsset_id());
						//
						server.updateAlarm(asset, timestamp, asset_alarm_list);
						server.updateTimeline(asset, timestamp, asset_timeline_list);
					};
					long end = System.currentTimeMillis() - start;
					log.debug("Cassandra db data find timer execute completed : process_time=[" + end + "]ms");
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
