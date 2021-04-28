package plantpulse.plugin.aas.asset;

import java.util.ArrayList;
import java.util.List;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import plantpulse.domain.Asset;
import plantpulse.domain.Metadata;
import plantpulse.domain.Site;
import plantpulse.domain.Tag;
import plantpulse.plugin.aas.dao.AssetDAO;
import plantpulse.plugin.aas.server.AASServer;

public class PlantPulseAssetList implements AssetList {
	

	private static final Log log = LogFactory.getLog(PlantPulseAssetList.class);
	
	private AASServer server;
	
	public PlantPulseAssetList(AASServer server ) {
		this.server = server;
	}

	@Override
	public List<Asset> totalAssets() throws Exception {
		List<Asset> total_asset_list = new ArrayList<>();
		AssetDAO dao = new AssetDAO();
		List<Site> site_list = dao.selectSiteList();
		for(int si=0;si < site_list.size(); si++) {
			Site site = site_list.get(si);
			server.createSite(site);
			//
			List<Asset> asset_list = dao.selectAssetList(site.getSite_id());
			for(int ai=0; ai < asset_list.size(); ai++) {
				Asset asset = asset_list.get(ai);
				List<Tag> tag_list = dao.selectTagList(asset.getAsset_id());
				List<Metadata> metadata_list = dao.selectMetadataList(asset.getAsset_id());
				server.createAsset(asset, tag_list, metadata_list);
				total_asset_list.add(asset);
				log.debug("에셋 모델 생성 : ID=[" + asset.getAsset_id() + "]");
			}
		};
		return total_asset_list;
	}

}
