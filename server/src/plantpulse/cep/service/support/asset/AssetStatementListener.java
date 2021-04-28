package plantpulse.cep.service.support.asset;

import java.util.HashMap;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.json.JSONObject;

import com.espertech.esper.client.EventBean;
import com.espertech.esper.client.UpdateListener;
import com.google.gson.Gson;
import com.google.gson.reflect.TypeToken;

import plantpulse.cep.domain.AssetStatement;
import plantpulse.cep.engine.CEPEngineManager;
import plantpulse.cep.engine.logging.EngineLogger;
import plantpulse.domain.Asset;
import plantpulse.event.asset.AssetAggregation;
import plantpulse.event.asset.AssetContext;
import plantpulse.event.asset.AssetEvent;

/**
 * AssetStatementListener
 * @author lsb
 *
 */
public class AssetStatementListener  implements UpdateListener {

	private static final Log log = LogFactory.getLog(AssetStatementListener.class);

	private AssetStatement asset_statement;
	private Asset asset;
	;

	public AssetStatementListener(AssetStatement asset_statement, Asset asset) {
		this.asset_statement = asset_statement;
		this.asset = asset;
		
	}

	@Override
	public void update(EventBean[] newEvents, EventBean[] arg1) {

		try {

			if (newEvents != null && newEvents.length > 0) {

				for (int i = 0; i < newEvents.length; i++) {
					EventBean event = newEvents[i];
					//
					if (event != null) {
						String json_text = CEPEngineManager.getInstance().getProvider().getEPRuntime().getEventRenderer().renderJSON("json", event);
						JSONObject json = JSONObject.fromObject(JSONObject.fromObject(json_text).getJSONObject("json").toString());
						log.debug("Asset statement event underlying : asset_statement=[" + asset_statement.toString() + "], json=[" + json.toString() + "]");

						AssetStatementResultService result_service = new AssetStatementResultService();
					
						if(asset_statement.getType().equals(AssetStatement.EVENT_TYPE)){
						    if(CEPEngineManager.getInstance().isStarted()){
						    	
						    	HashMap<String,String> data = new Gson().fromJson( json.toString(), new TypeToken<HashMap<String, String>>(){}.getType());
						    	if(data == null) data = new HashMap<String,String>();
						    	data.remove("event");
						    	data.remove("timestamp");
						    	data.remove("from_timestamp");
						    	data.remove("to_timestamp");
						    	data.remove("event");
						    	data.remove("color");
						    	data.remove("note");
						    	
						    	AssetEvent ae = new AssetEvent();
						    	ae.setAsset_id(asset_statement.getAsset_id());
						    	ae.setTimestamp(json.getLong("timestamp"));
						    	ae.setEvent(asset_statement.getStatement_name());
						    	ae.setColor(json.getString("color"));
						    	ae.setFrom_timestamp(json.getLong("from_timestamp"));
						    	ae.setTo_timestamp(json.getLong("to_timestamp"));
						    	ae.setData(data);
						    	ae.setNote(json.getString("note"));
						    	
						    	result_service.insertAssetEvent(ae);
						    	
						    }
						}else if(asset_statement.getType().equals(AssetStatement.CONTEXT_TYPE)){
							if(CEPEngineManager.getInstance().isStarted()){
								
								AssetContext ac = new AssetContext();
								ac.setAsset_id(asset_statement.getAsset_id());
								ac.setTimestamp(json.getLong("timestamp"));
								ac.setContext(asset_statement.getStatement_name());
								ac.setKey(json.getString("key"));
								ac.setValue(json.getString("value"));
						    	
								result_service.insertAssetContext(ac);
						    }
							
						}else if(asset_statement.getType().equals(AssetStatement.AGGREGATION_TYPE)){
							if(CEPEngineManager.getInstance().isStarted()){
								
								AssetAggregation aa = new AssetAggregation();
								aa.setAsset_id(asset_statement.getAsset_id());
								aa.setTimestamp(json.getLong("timestamp"));
								aa.setAggregation(asset_statement.getStatement_name());
								aa.setKey(json.getString("key"));
								aa.setValue(json.getString("value"));
								
								result_service.insertAssetAggregation(aa);
					        }
						};
						//
					}
				}
			}
			
			log.debug("AssetStatement execute successfully : statement_name=[" + asset_statement.getStatement_name() + "]");

		} catch (Exception ex) {
			EngineLogger.error("에셋[" + asset_statement.getAsset_id() + "] 데이터 생성[" + asset_statement.getStatement_name() + "] 업데이트 리스너에서 오류가 발생하였습니다.");
			log.error("AssetStatement update listener failed : " + asset_statement.getStatement_name() , ex);
		}
	}

}
