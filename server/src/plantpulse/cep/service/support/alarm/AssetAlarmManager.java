package plantpulse.cep.service.support.alarm;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.json.JSONObject;

import plantpulse.cep.engine.CEPEngineManager;
import plantpulse.event.asset.AssetAlarm;

/**
 * AssetAlarmManager
 * @author lsb
 *
 */
public class AssetAlarmManager {
	
	private static final Log log = LogFactory.getLog(AssetAlarmManager.class);
	
	public void insertAssetAlarm(JSONObject data){
		try{
			
			if(CEPEngineManager.getInstance().isStarted()){
				//
				AssetAlarm aa = new AssetAlarm();
				aa.setAsset_id(data.getString("asset_id"));
				aa.setAlias_name(data.getString("alias_name"));
				aa.setTag_id(data.getString("tag_id"));
				aa.setTag_name(data.getString("tag_name"));
				aa.setTimestamp(Long.parseLong(data.getString("timestamp")));
				aa.setPriority(data.getString("priority"));
				aa.setDescription(data.getString("description"));
				aa.setAlarm_seq(data.getLong("alarm_seq"));
				aa.setAlarm_config_id(data.getString("alarm_config_id"));
				
				CEPEngineManager.getInstance().getProvider().getEPRuntime().sendEvent(aa);
				CEPEngineManager.getInstance().getData_distribution_service().sendAssetAlarm(data.getString("asset_id"), JSONObject.fromObject(aa));
		    };
		    
		}catch(Exception ex){
			log.error("AssetAlarmManager manager insert asset alarm error : " + ex.getMessage(), ex);
		}
	};
	

}
