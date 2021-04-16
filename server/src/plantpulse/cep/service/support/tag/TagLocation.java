package plantpulse.cep.service.support.tag;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.realdisplay.framework.util.StringUtils;

import plantpulse.cep.dao.TagDAO;
import plantpulse.cep.engine.asset.AssetCacheFactory;
import plantpulse.domain.Asset;
import plantpulse.domain.Tag;

/**
 * TagLocation
 * 
 * @author leesa
 *
 */
public class TagLocation {
	
	private static final Log log = LogFactory.getLog(TagLocation.class);
	
	public static final String LOCATION_SPLIT_STR = " / "; 
	
	/**
	 * getLocation
	 * 
	 * @param tag_id
	 * @return
	 */
	public String getLocation(String tag_id, boolean is_append_tag_name) {
		String location = "-";
		try {
			log.debug("Find tag location for " + tag_id);
			TagDAO tag_dao = new TagDAO();
			Tag tag = tag_dao.selectTagInfoWithSiteAndOpc(tag_id);

			// 에셋 경로로 안내
			if (StringUtils.isNotEmpty(tag.getLinked_asset_id())) {

				String tag_name = tag.getTag_name();
				tag_name = tag_name.replaceAll("\\.", " / ");
				
				String site_name = tag.getSite_name();
				
				Asset level_3 = AssetCacheFactory.getInstance().getAsset(tag.getLinked_asset_id());
				if(level_3 == null) {
					log.warn("Bad asset level 3 ID mapping tag_id = [" + tag.getTag_id() + "], asset_linked_id=[" + tag.getLinked_asset_id() + "]");
					level_3  = new Asset();
					level_3.setAsset_id("ASSET_3_ERROR");
					level_3.setAsset_name("ASSET_LEVEL_3_MAPPING_ERROR");
				}
				Asset level_2 = AssetCacheFactory.getInstance().getAsset(level_3.getParent_asset_id());
				if(level_2 == null) {
					log.warn("Bad asset level 2 ID mapping tag_id = [" + tag.getTag_id() + "], asset_linked_id=[" + tag.getLinked_asset_id() + "]");
					level_2  = new Asset();
					level_2.setAsset_id("ASSET_2_ERROR");
					level_2.setAsset_name("ASSET_LEVEL_2_MAPPING_ERROR");
				}
						
				//
				location = site_name + " / " + level_2.getAsset_name() + " / " + level_3.getAsset_name();
				if (is_append_tag_name) {
					location += " / " + tag_name;
				}

			} else {
				String site_name = tag.getSite_name();
				String opc_name  = tag.getOpc_name();
				String tag_name  = tag.getTag_name();
				tag_name=tag_name.replaceAll("\\.", " / ");
				
				//
				location = site_name + " / " + opc_name;
				if (is_append_tag_name) {
					location += " / " + tag_name;
				}
			}
			log.debug("Find tag location for " + tag_id + ", completed.");
		} catch (Exception ex) {
			log.error("Tag location find error : " + ex.getMessage(), ex);
		}

		return location;
	}

}
