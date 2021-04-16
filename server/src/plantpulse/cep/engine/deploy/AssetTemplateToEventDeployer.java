package plantpulse.cep.engine.deploy;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

@Deprecated
public class AssetTemplateToEventDeployer implements Deployer  {

	private static final Log log = LogFactory.getLog(AssetTemplateToEventDeployer.class);

	public void deploy() {

		/*
		//
		try {
			AssetDAO dao = new AssetDAO();
			List<Asset> list = dao.selectModels();
			//
			for (int i = 0; list != null && i < list.size(); i++) {
				Asset model = list.get(i);

				// log.info("model=" + model);

				TagDAO td = new TagDAO();
				List<Tag> tags = td.selectTagsByLinkAssetId(model.getAsset_id());

				if (tags != null) {
					Map<String, Object> def = new HashMap<String, Object>();
					def.put("timestamp", Long.class);
					def.put("site_id", String.class);
					def.put("asset_id", String.class);
					if (tags != null && tags.size() > 0) {
						for (int x = 0; x < tags.size(); x++) {
							Tag tag = tags.get(x);
							String filed_name = OPCDataUtils.getSubstringTagName(tag.getTag_name(), tag.getAlias_name());
							Class<?> java_type = OPCDataUtils.getJavaTypeClass(tag.getJava_type());
							def.put(filed_name, java_type);
						};

						CEPEngineManager.getInstance().getProvider().getEPAdministrator().getConfiguration().removeEventType(model.getAsset_name(), true);
						CEPEngineManager.getInstance().getProvider().getEPAdministrator().getConfiguration().addEventType(model.getAsset_name(), def);
						if(StringUtils.isNotEmpty(model.getTable_type())){
							CEPEngineManager.getInstance().getProvider().getEPAdministrator().getConfiguration().removeEventType(model.getTable_type(), true);
							CEPEngineManager.getInstance().getProvider().getEPAdministrator().getConfiguration().addEventType(model.getTable_type(), def);
						};
						
						//
						

						log.info("Asset template event registed : asset_name=[" + model.getAsset_name() + "]");
					}
				}
				//

			}

			//
		} catch (Exception ex) {
			EngineLogger.error("에셋 템플릿 이벤트를 배치하는도중 오류가 발생하였습니다 : 에러=" + ex.getMessage());
			log.warn("AssetModelEvent deploy error : " + ex.getMessage(), ex);
		}
		
		*/

	}

}
