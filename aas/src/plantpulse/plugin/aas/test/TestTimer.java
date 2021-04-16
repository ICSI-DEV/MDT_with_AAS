package plantpulse.plugin.aas.test;

import java.util.Timer;
import java.util.TimerTask;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import plantpulse.plugin.aas.Main;
import plantpulse.plugin.aas.server.AASServer;

public class TestTimer {
	
	private static final Log log = LogFactory.getLog(TestTimer.class);
	
	private Timer timer;
	
	private AASServer server;
	
	public TestTimer(AASServer server) {
		this.server = server;
		
	}
	
	public void start() {
		timer = new Timer("TEST");
		timer.scheduleAtFixedRate(new TimerTask() {
			@Override
			public void run() {
				//
				//SITE_01012.ASSET_01031
				//plantpulse.domain.Asset passet= new plantpulse.domain.Asset();
				//passet.setSite_id("SITE_01012");
				//passet.setAsset_id("ASSET_01031");
				//log.info(passet.getAsset_id() + " = " + server.getDataMap(passet));
				
				//log.info(LastAssetDataMap.getInstance().getMap().toString());
				
			}
			
		}, 1*1000, 1*1000);
	}

}
