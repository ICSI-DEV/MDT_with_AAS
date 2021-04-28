package plantpulse.cep.engine.asset;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import plantpulse.domain.Asset;
import plantpulse.domain.Tag;

/**
 * 에셋 테이블 레이아웃 팩토리
 * 
 * @author leesa
 *
 */
public class AssetTableLayoutFactory {
	
	private static final Log log = LogFactory.getLog(AssetTableLayoutFactory.class);
	
	private static class AssetTableLayoutFactoryHolder {
		static AssetTableLayoutFactory instance = new AssetTableLayoutFactory();
	}

	public static AssetTableLayoutFactory getInstance() {
		return AssetTableLayoutFactoryHolder.instance;
	}
	
	
	private List<Asset> asset_list = new ArrayList<Asset>();
	private Map<String, List<Tag>> tag_list_in_asset = new ConcurrentHashMap<String, List<Tag>>();
	
	public void setAssetList(List<Asset> asset_list ) {
		this.asset_list = asset_list;
	}
	
	public List<Asset> getAssetList() {
		return this.asset_list;
	}
	
	
	public void putTagList(String asst_id, List<Tag> tag_list){
		tag_list_in_asset.put(asst_id, tag_list);
	}
	
	public List<Tag> getTagList(String asst_id){
		return tag_list_in_asset.get(asst_id);
	}

}
