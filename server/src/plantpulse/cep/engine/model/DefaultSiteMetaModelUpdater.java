package plantpulse.cep.engine.model;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import plantpulse.cep.dao.AlarmConfigDAO;
import plantpulse.cep.dao.AssetDAO;
import plantpulse.cep.dao.MetadataDAO;
import plantpulse.cep.dao.OPCDAO;
import plantpulse.cep.dao.SecurityDAO;
import plantpulse.cep.dao.SiteDAO;
import plantpulse.cep.dao.TagDAO;
import plantpulse.cep.dao.UserDAO;
import plantpulse.cep.service.client.StorageClient;
import plantpulse.domain.AlarmConfig;
import plantpulse.domain.Asset;
import plantpulse.domain.MetaModel;
import plantpulse.domain.Metadata;
import plantpulse.domain.OPC;
import plantpulse.domain.Security;
import plantpulse.domain.Site;
import plantpulse.domain.Tag;
import plantpulse.domain.User;

/**
 * DefaultSiteMetaModelUpdater
 * @author lsb
 *
 */
public class DefaultSiteMetaModelUpdater  implements SiteMetaModelUpdater {
	

	private static final Log log = LogFactory.getLog(SiteMetaModelManager.class);
	
	
	private StorageClient clinent = new StorageClient();
	
	
	/**
	 *  사이트, 에셋, 태그, 유저, 보안, 추가 JSON 모델 등의 메타 테이블을 카산드라로 복제
	 *  
	 * @throws Exception
	 */
	public void update() throws Exception {
		
		try{ 
			
			long start = System.currentTimeMillis();
			
			//---------------------------------------------------------------------------------------//
			//TODO 기본 테이블 이관
			//---------------------------------------------------------------------------------------//
			List<Site> sites = new SiteDAO().selectSites();
			Map<String, Site> site_map = new HashMap<>();
			for(int i=0; i < sites.size(); i++){
				site_map.put(sites.get(i).getSite_id(), sites.get(i));
			};
			
			//
			List<OPC> opcs = new OPCDAO().selectOpcList();
			Map<String, OPC> opc_map = new HashMap<>();
			for(int i=0; i < opcs.size(); i++){
				opc_map.put(opcs.get(i).getOpc_id(), opcs.get(i));
			};
			
			//
			List<Tag> tags = new TagDAO().selectTagAll();
			Map<String, Tag> tag_map = new HashMap<>();
			for(int i=0; i < tags.size(); i++){
				tag_map.put(tags.get(i).getTag_id(), tags.get(i));
			};
			
			//
			List<AlarmConfig> alarms = new AlarmConfigDAO().getAlarmConfigListAll();
			Map<String, AlarmConfig> alarm_map = new HashMap<>();
			for(int i=0; i < alarms.size(); i++){
				alarm_map.put(alarms.get(i).getAlarm_config_id(), alarms.get(i));
			};
			
			//
			List<Asset> assets = new AssetDAO().selectAssets();
			Map<String, Asset> asset_map = new HashMap<>();
			for(int i=0; i < assets.size(); i++){
				asset_map.put(assets.get(i).getAsset_id(), assets.get(i));
			};
			
			//
			List<User> users = new UserDAO().getUserList();
			Map<String, User> user_map = new HashMap<>();
			for(int i=0; i < users.size(); i++){
				user_map.put(users.get(i).getUser_id(), users.get(i));
			};
			
			//
			List<Security> securities = new SecurityDAO().getSecurityList();
			Map<String, Security> seucrity_map = new HashMap<>();
			for(int i=0; i < securities.size(); i++){
				seucrity_map.put(securities.get(i).getSecurity_id(), securities.get(i));
			};
			
			//
			List<Metadata> metadatas = new MetadataDAO().selectMetadataList();
			Map<String, Metadata> metadata_map = new HashMap<>();
			for(int i=0; i < metadatas.size(); i++){
				metadata_map.put(metadatas.get(i).getObject_id() + ":" + metadatas.get(i).getKey(), metadatas.get(i));
			};
			
			
			MetaModel model = new MetaModel();
			model.setSite_map(site_map);
			model.setOpc_map(opc_map);
			model.setTag_map(tag_map);
			model.setAlarm_map(alarm_map);
			model.setAsset_map(asset_map);
			model.setUser_map(user_map);
			model.setSeucrity_map(seucrity_map);
			model.setMetadata_map(metadata_map);
			
			
			//---------------------------------------------------------------------------------------//
			//TODO JSON 트리 모델 생성, 외부 인터페이스 빼면 좋겠네.
			//---------------------------------------------------------------------------------------//
			/*
			for(int i=0; i < sites.size(); i++){
				
				Site site = sites.get(i);
				String site_id = sites.get(i).getSite_id();
				
				JSONObject site_json = JSONObject.fromObject(site);
				
				List<Asset> process_list = new AssetDAO().selectAssetsBySiteIdAndAssetType(site_id, "A");
				JSONArray process_json_array = JSONArray.fromObject(process_list);
				site_json.put("process_list", process_json_array);
				model.addJSONModel(site_id + ".PROCESS.LIST",  process_json_array.toString());
				
				for(int j=0; j < process_json_array.size(); j++){
					JSONObject process = process_json_array.getJSONObject(j);
					
					List<Asset> equipment_list = new AssetDAO().selectAssetsByParentAssetId(site_id, process.getString("asset_id"));
					JSONArray equipment_json_array = JSONArray.fromObject(equipment_list);
					process.put("equipment_list", equipment_json_array);
					
					site_json.getJSONArray("process_list").getJSONObject(j).put("equipment_list", equipment_json_array);
					
					model.addJSONModel(site_id + "." + process.getString("asset_id") + ".EQUIPMENT.LIST",  equipment_json_array.toString());
					
					for(int k=0; k < equipment_json_array.size(); k++){
						JSONObject equipment =  equipment_json_array.getJSONObject(k);
						
						List<Tag> tag_list = new TagDAO().selectTagListByAssetId(equipment.getString("asset_id"));
						JSONArray tag_json_array = JSONArray.fromObject(tag_list);
						equipment.put("tag_list", tag_json_array);
						
						site_json.getJSONArray("process_list").getJSONObject(j).getJSONArray("equipment_list").getJSONObject(k).put("tag_list", tag_json_array);
						
						model.addJSONModel(site_id + "." + process.getString("asset_id") + "." + equipment.getString("asset_id") +  ".TAG.LIST",  tag_json_array.toString());
					}
				};
				
				//log.info(site_json.toString());
				model.addJSONModel(site_id + ".OBJECT.TREE",  site_json.toString());
				
				//OPC 네비게이션 트리 JSON
				OPC opc = new OPC();
				opc.setSite_id(site_id);
				List<OPC> opcList = new OPCDAO().selectOpcs(opc);
				model.addJSONModel(site_id + ".OPC.NAV.TREE.BY_NAME",  JSONArray.fromObject(opcList).toString());
				
				//에셋 네비게이션 트리 JSON (NAME)
				Asset asset = new Asset();
				asset.setSite_id(site_id);
				asset.setShow_description(true);
				List<Asset> assetList = new AssetDAO().selectAssets(asset);
				model.addJSONModel(site_id + ".ASSET.NAV.TREE.BY_NAME",  JSONArray.fromObject(assetList).toString());
				
				//에셋 네비게이션 트리 JSON (DESCRIPTION)
				asset.setShow_description(false);
				assetList = new AssetDAO().selectAssets(asset);
				model.addJSONModel(site_id + ".ASSET.NAV.TREE.BY_DESCRIPTION",  JSONArray.fromObject(assetList).toString());
				
			};
			
			*/
			
			//
			//BackupDAO dao = new BackupDAO();
			//dao.backup();
			clinent.forInsert().updateMetadata(model);
			
			log.info("Site metadata model updated : exec_time=[" + (System.currentTimeMillis()-start)+ "]");
			
			
		}catch(Exception ex){
			log.error("Site metadata model update failed : " + ex.getMessage(),  ex);
		}
		
	}


}
