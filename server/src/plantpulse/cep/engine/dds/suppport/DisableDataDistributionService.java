package plantpulse.cep.engine.dds.suppport;

import org.json.JSONObject;

import plantpulse.cep.engine.dds.DataDistributionService;

/**
 * DisableDataDistributionService
 * @author leesa
 *
 */
public class DisableDataDistributionService  implements DataDistributionService {

	@Override
	public void sendTagPoint(String tag_id, JSONObject data) {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void sendTagAlarm(String tag_id, JSONObject data) {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void sendAssetData(String asset_id, JSONObject data) {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void sendAssetAlarm(String asset_id, JSONObject data) {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void sendAssetEvent(String asset_id, JSONObject data) {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void sendAssetContext(String asset_id, JSONObject data) {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void sendAssetAggregation(String asset_id, JSONObject data) {
		// TODO Auto-generated method stub
		
	}

}
