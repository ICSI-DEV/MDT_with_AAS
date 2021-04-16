package plantpulse.cep.engine.eventbus;

import java.util.List;

import plantpulse.cep.dao.AlarmConfigDAO;
import plantpulse.cep.dao.AssetDAO;
import plantpulse.cep.dao.MetadataDAO;
import plantpulse.cep.dao.OPCDAO;
import plantpulse.cep.dao.SiteDAO;
import plantpulse.cep.dao.TagDAO;
import plantpulse.cep.engine.asset.AssetCacheManager;
import plantpulse.cep.service.client.StorageClient;
import plantpulse.cep.service.support.tag.TagCacheManager;
import plantpulse.domain.AlarmConfig;
import plantpulse.domain.Asset;
import plantpulse.domain.Metadata;
import plantpulse.domain.OPC;
import plantpulse.domain.Site;
import plantpulse.domain.Tag;
import plantpulse.json.JSONArray;
import plantpulse.json.JSONObject;

/**
 * DefaultDomainChangeEventBus
 * 
 * @author lsb
 *
 */
public class DefaultDomainChangeEventBus  implements DomainChangeEventBus {
	
	
	private StorageClient storage_client = new StorageClient();
	
	
	private SiteDAO site_dao = new SiteDAO();
	private AssetDAO asset_dao = new AssetDAO();
	private OPCDAO opc_dao = new OPCDAO();
	private TagDAO tag_dao = new TagDAO();
	private AlarmConfigDAO alarm_config_dao = new AlarmConfigDAO();
	private MetadataDAO metadata_dao = new MetadataDAO();
	
	
	
	/*
	 * (non-Javadoc)
	 * @see plantpulse.cep.engine.bus.DomainChangeEventBus#onSiteChanged(java.lang.String)
	 */
	public void onSiteChanged(String site_id)  throws Exception{
		
		Site site = site_dao.selectSite(site_id);
		//
		if(site != null) {
			long current_timestamp = System.currentTimeMillis();
			String domain_type = "SITE";
			String object_id   = site_id;
			String json = JSONObject.fromObject(site).toString();
			storage_client.forInsert().insertDomainChangeHistory(current_timestamp, domain_type, object_id, json);
		};
		
		DomainChangeQueue.getInstance().add(site_id);
	}
	
	/*
	 * (non-Javadoc)
	 * @see plantpulse.cep.engine.bus.DomainChangeEventBus#onOPCChanged(java.lang.String)
	 */
	public void onOPCChanged(String opc_id)   throws Exception{
		OPC opc = opc_dao.selectOpc(opc_id);
		//
		if(opc != null) {
			long current_timestamp = System.currentTimeMillis();
			String domain_type = "OPC";
			String object_id   = opc_id;
			String json = JSONObject.fromObject(opc).toString();
			storage_client.forInsert().insertDomainChangeHistory(current_timestamp, domain_type, object_id, json);
		};
		
		DomainChangeQueue.getInstance().add(opc_id);
	}
	
	
	/*
	 * (non-Javadoc)
	 * @see plantpulse.cep.engine.bus.DomainChangeEventBus#onAssetChanged(java.lang.String)
	 */
	public void onAssetChanged(String asset_id) throws Exception{
		Asset asset = asset_dao.selectAsset(asset_id);
		//
		if(asset != null) {
			long current_timestamp = System.currentTimeMillis();
			String domain_type = "ASSET";
			String object_id   = asset_id;
			String json = JSONObject.fromObject(asset).toString();
			storage_client.forInsert().insertDomainChangeHistory(current_timestamp, domain_type, object_id, json);
			
			//에셋 캐시 업데이트
			AssetCacheManager manager = new AssetCacheManager();
			manager.updateCache(asset);
		};
		
		DomainChangeQueue.getInstance().add(asset_id);
	}
	
	
	/*
	 * (non-Javadoc)
	 * @see plantpulse.cep.engine.bus.DomainChangeEventBus#onTagChanged(java.lang.String)
	 */
	public void onTagChanged(String tag_id)   throws Exception{
		Tag tag = tag_dao.selectTag(tag_id);
		//
		if(tag != null) {
			long current_timestamp = System.currentTimeMillis();
			String domain_type = "TAG";
			String object_id   = tag_id;
			String json = JSONObject.fromObject(tag).toString();
			storage_client.forInsert().insertDomainChangeHistory(current_timestamp, domain_type, object_id, json);
			
			//태크 및 위치 캐싱
			TagCacheManager manager = new TagCacheManager();
			manager.updateCache(tag);
		};
		
		DomainChangeQueue.getInstance().add(tag_id);
	}
	
	/*
	 * (non-Javadoc)
	 * @see plantpulse.cep.engine.bus.DomainChangeEventBus#onAlarmConfigChanged(java.lang.String)
	 */
	public void onAlarmConfigChanged(String alarm_config_id)   throws Exception{
		AlarmConfig alarm_config = alarm_config_dao.getAlarmConfig(alarm_config_id);
		//
		if(alarm_config != null) {
			long current_timestamp = System.currentTimeMillis();
			String domain_type = "ALARM";
			String object_id   = alarm_config_id;
			String json = JSONObject.fromObject(alarm_config).toString();
			storage_client.forInsert().insertDomainChangeHistory(current_timestamp, domain_type, object_id, json);
		};
		
		DomainChangeQueue.getInstance().add(alarm_config_id);
	}
	
	/*
	 * (non-Javadoc)
	 * @see plantpulse.cep.engine.bus.DomainChangeEventBus#onMetadataChanged(java.lang.String)
	 */
	@Override
	public void onMetadataChanged(String object_id) throws Exception {
		List<Metadata> metadata_list = metadata_dao.selectMetadataList(object_id);
		//
		if(metadata_list != null) {
			long current_timestamp = System.currentTimeMillis();
			String domain_type = "METADATA";
			String json = JSONArray.fromObject(metadata_list).toString();
			storage_client.forInsert().insertDomainChangeHistory(current_timestamp, domain_type, object_id, json);
		};
		
		DomainChangeQueue.getInstance().add(object_id);
	}

	
	/*
	 * (non-Javadoc)
	 * @see plantpulse.cep.engine.bus.DomainChangeEventBus#onUserChanged(java.lang.String)
	 */
	public void onUserChanged(String user_id)   throws Exception{
		DomainChangeQueue.getInstance().add(user_id);
	}
	
	/*
	 * (non-Javadoc)
	 * @see plantpulse.cep.engine.bus.DomainChangeEventBus#onSecurityChanged(java.lang.String)
	 */
    public void onSecurityChanged(String security_id)   throws Exception{
    	DomainChangeQueue.getInstance().add(security_id);
	};
	
	/*
	 * (non-Javadoc)
	 * @see plantpulse.cep.engine.eventbus.DomainChangeEventBus#onTokenChanged(java.lang.String)
	 */
	public void onTokenChanged(String token)   throws Exception{
	    	DomainChangeQueue.getInstance().add(token);
	}

	
    
    
}
