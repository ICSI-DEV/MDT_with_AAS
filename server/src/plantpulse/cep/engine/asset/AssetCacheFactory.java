package plantpulse.cep.engine.asset;

import java.util.Map;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.eclipse.collections.impl.map.mutable.ConcurrentHashMap;

import plantpulse.cep.dao.AssetDAO;
import plantpulse.domain.Asset;

/**
 * AssetCacheFactory
 * 
 * @author leesa
 *
 */
public class AssetCacheFactory  {
	
	private static final Log log = LogFactory.getLog(AssetCacheFactory.class);
	
	private static class AssetCacheFactoryHolder {
		static AssetCacheFactory instance = new AssetCacheFactory();
	}

	public static AssetCacheFactory getInstance() {
		return AssetCacheFactoryHolder.instance;
	}
	
	private Map<String, Asset> asset_map = new ConcurrentHashMap<String, Asset>();
	
	public void putAsset(String asset_id, Asset asset){
		asset_map.putIfAbsent(asset_id, asset);
	}
	
	public void updateAsset(String asset_id, Asset Asset){
		asset_map.putIfAbsent(asset_id, Asset);
		log.info("Asset cache updated : asset_id=[" + asset_id + "]");
	}
	
	public Asset getAsset(String asset_id){
		if(asset_map.containsKey(asset_id)) {
			return asset_map.get(asset_id);
		}else {
			try {
				AssetDAO dao = new AssetDAO();
				Asset asset = dao.selectAsset(asset_id);
				if(asset == null) log.warn("Asset is NULL. asset_id=[" + asset_id + "]");
				this.putAsset(asset_id, asset);
				return asset;
			} catch (Exception e) {
				log.error("Asset object getting error in cache : " + e.getMessage(), e);
				return null;
			}
		}
	};
	

};