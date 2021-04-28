package plantpulse.cep.service.support.asset;

import org.json.JSONObject;

import plantpulse.cep.dao.AssetDAO;
import plantpulse.cep.dao.AssetStatementDAO;
import plantpulse.cep.domain.AssetStatement;
import plantpulse.cep.engine.CEPEngineManager;
import plantpulse.cep.service.client.StorageClient;
import plantpulse.domain.Asset;
import plantpulse.event.asset.AssetAggregation;
import plantpulse.event.asset.AssetContext;
import plantpulse.event.asset.AssetEvent;

/**
 * 에셋 데이터 결과 서비스
 * 
 * @author lsb
 *
 */
public class AssetStatementResultService {
	
	private StorageClient client;
	private AssetDAO dao; 
	private AssetStatementDAO stmt_dao;
	
	public AssetStatementResultService(){
		 client = new StorageClient();
		 dao = new AssetDAO();
		 stmt_dao = new AssetStatementDAO();
	};

	public void insertAssetEvent(AssetEvent event) throws Exception {
		if(CEPEngineManager.getInstance().isStarted()){
			Asset asset = dao.selectAssetOrSite(event.getAsset_id());
			AssetStatement stmt = stmt_dao.selectAssetStatement(event.getAsset_id(), event.getEvent()) ;
			event.setStatement_name(stmt.getStatement_name());
			event.setStatement_description(stmt.getDescription());
			//
			client.forInsert().insertAssetTimeline(stmt, event.getTimestamp(), asset, "EVENT", JSONObject.fromObject(event).toString());
	    	client.forInsert().insertAssetEvent(stmt, event.getTimestamp(), asset, event.getEvent(), event.getFrom_timestamp(), event.getTo_timestamp(),  event.getColor(), event.getData(), event.getNote());
	    	CEPEngineManager.getInstance().getProvider().getEPRuntime().sendEvent(event);
	    	//
	    	CEPEngineManager.getInstance().getData_distribution_service().sendAssetEvent(event.getAsset_id(), JSONObject.fromObject(event));
		}
	};
	
	public void insertAssetContext(AssetContext context) throws Exception {
		if(CEPEngineManager.getInstance().isStarted()){
			Asset asset = dao.selectAssetOrSite(context.getAsset_id());
			AssetStatement stmt = stmt_dao.selectAssetStatement(context.getAsset_id(), context.getContext()) ;
			context.setStatement_name(stmt.getStatement_name());
			context.setStatement_description(stmt.getDescription());
			//
			client.forInsert().insertAssetTimeline(stmt, context.getTimestamp(), asset, "CONTEXT", JSONObject.fromObject(context).toString());
			client.forInsert().insertAssetContext(stmt, context.getTimestamp(), asset, context.getContext(), context.getKey(), context.getValue());
	    	CEPEngineManager.getInstance().getProvider().getEPRuntime().sendEvent(context);
	    	//
	    	CEPEngineManager.getInstance().getData_distribution_service().sendAssetContext(context.getAsset_id(), JSONObject.fromObject(context));
	    }
	};
	
	
	public void insertAssetAggregation(AssetAggregation aggregation) throws Exception {
		if(CEPEngineManager.getInstance().isStarted()){
			Asset asset = dao.selectAssetOrSite(aggregation.getAsset_id());
			AssetStatement stmt = stmt_dao.selectAssetStatement(aggregation.getAsset_id(), aggregation.getAggregation()) ;
			aggregation.setStatement_name(stmt.getStatement_name());
			aggregation.setStatement_description(stmt.getDescription());
			//
			client.forInsert().insertAssetTimeline(stmt, aggregation.getTimestamp(), asset, "AGGREGATION", JSONObject.fromObject(aggregation).toString());
			client.forInsert().insertAssetAggregation(stmt, aggregation.getTimestamp(), asset, aggregation.getAggregation(), aggregation.getKey(), aggregation.getValue());
	    	CEPEngineManager.getInstance().getProvider().getEPRuntime().sendEvent(aggregation);
	    	//
	    	CEPEngineManager.getInstance().getData_distribution_service().sendAssetAggregation(aggregation.getAsset_id(), JSONObject.fromObject(aggregation));
        }
	};


}
