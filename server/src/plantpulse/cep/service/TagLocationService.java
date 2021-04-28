package plantpulse.cep.service;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.realdisplay.framework.util.StringUtils;
import org.springframework.beans.factory.annotation.Autowired;

import plantpulse.cep.dao.AssetDAO;
import plantpulse.cep.dao.TagDAO;
import plantpulse.domain.Asset;
import plantpulse.domain.Tag;

public class TagLocationService {

	private static final Log log = LogFactory.getLog(AssetService.class);

	@Autowired
	private AssetDAO asset_dao;

	@Autowired
	private TagDAO tag_dao;

	public String getLocation(String tag_id) {
		String location = "-";
		try {
			Tag tag = tag_dao.selectTag(tag_id);
			// 에셋 경로로 안내
			if (StringUtils.isNotEmpty(tag.getLinked_asset_id())) {
				// String tag_name = tag.getTag_name();
				// String opc_name = tag.getOpc_name();
				String site_name = tag.getSite_name();

				Asset level_3 = asset_dao.selectAsset(tag.getLinked_asset_id());
				Asset level_2 = asset_dao.selectAsset(level_3.getParent_asset_id());
				// Asset level_1 =
				// asset_dao.selectAsset(level_2.getParent_asset_id());
				//

				location = site_name + "/" + level_3.getAsset_name() + "/" + level_2.getAsset_name();

			} else {
				String site_name = tag.getSite_name();
				String opc_name = tag.getOpc_name();
				String tag_name = tag.getTag_name();
				//
				location = site_name + "/" + opc_name + "/" + tag_name;
			}
		} catch (Exception ex) {
			log.error("Point location find error : " + ex.getMessage(), ex);
		}

		return location;
	}
}
