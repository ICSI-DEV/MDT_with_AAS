package plantpulse.cep.engine.deploy;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import plantpulse.cep.dao.AssetDAO;
import plantpulse.cep.dao.TagDAO;
import plantpulse.cep.engine.logging.EngineLogger;
import plantpulse.cep.engine.utils.AliasUtils;
import plantpulse.cep.service.client.StorageClient;
import plantpulse.domain.Asset;
import plantpulse.domain.Tag;

@Deprecated
public class AssetTemplateToTableDeployer implements Deployer  {

	private static final Log log = LogFactory.getLog(AssetTemplateToTableDeployer.class);

	public void deploy() {

		try {
			//
		long start = System.currentTimeMillis();

		final StorageClient client = new StorageClient();
		AssetDAO dao = new AssetDAO();
		List<Asset> list = dao.selectModels();
		//
		for (int i = 0; list != null && i < list.size(); i++) {
			Asset model = list.get(i);

			TagDAO td = new TagDAO();
			List<Tag> tags = td.selectTagsByLinkAssetId(model.getAsset_id());

			if (tags != null && tags.size() > 0) {
				Map<String, Tag> tag_map = new HashMap<String, Tag>();
				for (int x = 0; x < tags.size(); x++) {
					Tag tag = tags.get(x);
					String filed_name = AliasUtils.getAliasName(tag);
					tag_map.put(filed_name, tag);
				}
				;
				client.forInsert().createTableForAssetTemplateData(model, tag_map);
				log.info("Asset template table created : asset_id=[" + model.getAsset_id() + "], asset_name=[" + model.getAsset_name() + "]");
				
			}
			//
		}
		long end = System.currentTimeMillis() - start;
		log.debug("Asset table all created : asset_size=[" + list.size() + "], exec_time=[" + end + "]ms");
		//
		} catch (Exception ex) {
			EngineLogger.error("에셋 템플릿 테이블을 생성하는도중 오류가 발생하였습니다 : 에러=" + ex.getMessage());
			log.warn("AssetModelToTable deploy error : " + ex.getMessage(), ex);
		}

	}
}
