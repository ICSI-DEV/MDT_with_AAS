package plantpulse.cep.engine.asset;

import plantpulse.domain.Asset;

public class AssetCacheManager {
	
	public void updateCache(Asset asset) throws Exception {
		AssetCacheFactory.getInstance().updateAsset(asset.getAsset_id(), asset);
	}

	public Asset getAsset(String asset_id) throws Exception {
		return AssetCacheFactory.getInstance().getAsset(asset_id);
	}
	
}
