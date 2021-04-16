package plantpulse.cep.service;

import java.util.List;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.json.JSONArray;
import org.json.JSONObject;

import plantpulse.cep.dao.AssetDAO;
import plantpulse.cep.dao.SiteDAO;
import plantpulse.cep.dao.TagDAO;
import plantpulse.cep.service.client.StorageClient;
import plantpulse.domain.Asset;
import plantpulse.domain.Site;
import plantpulse.domain.Tag;


/**
 * ArchitectureService
 * @author leesa
 *
 */
public class ArchitectureService {
	
	private static final Log log = LogFactory.getLog(ArchitectureService.class);
	
	private AssetDAO asset_dao;
	private TagDAO tag_dao;
	private SiteDAO site_dao;
	
	private StorageClient storage_client;
	
	public ArchitectureService() {
		asset_dao = new AssetDAO();
		tag_dao = new TagDAO();
		site_dao = new SiteDAO();
		storage_client = new StorageClient();
	}
	
	
	/**
	 * 사이트 아키텍처 JSON 반환
	 * @param site_id
	 * @return
	 * @throws Exception
	 */
	public JSONObject getSiteArchitecture(String site_id) throws Exception {
			JSONObject root = new JSONObject();
			
			try {
				
			Site site = site_dao.selectSiteInfo(site_id);
				
			//
			int area_size = 0;
			int equipment_size = 0;
			int tag_size = 0;
			
			//
			List<Asset> area_list = asset_dao.selectAreaList(site.getSite_id());
			 for(int i=0; area_list != null && i < area_list.size(); i++){
				 Asset area = area_list.get(i);
				 area_size++;
				List<Asset> equipment_list = asset_dao.selectEquipmentList(area.getAsset_id());
		        for(int x=0; equipment_list != null && x < equipment_list.size(); x++){
		        	Asset equipment = equipment_list.get(x);
		        	equipment_size++;
		        	List<Tag> tag_list = tag_dao.selectTagListByAssetId(equipment.getAsset_id());
	                for(int z=0; tag_list != null && z < tag_list.size(); z++){
	                	tag_size++;
	                }
		        }
			 }
			;
			root.put("site_id", site.getSite_id());
			root.put("site_name", site.getSite_name());
			root.put("description", site.getDescription());
			root.put("area_size", area_size);
			root.put("equipment_size", equipment_size);
			root.put("tag_size", tag_size);
			
			Asset site_asset = new Asset();
			site_asset.setAsset_id(site.getSite_id());
			JSONArray e0_array = storage_client.forSelect().selectAssetEvent(site_asset, 1);
			root.put("event", (e0_array.size() > 0) ?  e0_array.getJSONObject(0) : new JSONObject());
			return root;
			
		}catch(Exception ex) {
			log.error("Site architecture load failed : " + ex.getMessage(), ex);
			throw ex;
		}
	}

	/**
	 * 사이트 아키텍처 트리 JSON 반환
	 * @param site_id
	 * @return
	 * @throws Exception
	 */
	public JSONObject getSiteArchitectureTree(String site_id) throws Exception {
		
		JSONObject root = new JSONObject();
		
		try {
		Site site = site_dao.selectSiteInfo(site_id);
		
		root.put("id", site.getSite_id());
		root.put("name", site.getSite_name());
		root.put("type", "SITE");
		root.put("desc", site.getDescription());
		root.put("symbol", "image:///resources/images/a_1.png");
		
		Asset site_asset = new Asset();
		site_asset.setAsset_id(site.getSite_id());
		JSONArray e0_array = storage_client.forSelect().selectAssetEvent(site_asset, 1);
		root.put("event", (e0_array.size() > 0) ?  e0_array.getJSONObject(0) : new JSONObject());
    	
		root.put("children", new JSONArray());
		
		List<Asset> area_list = asset_dao.selectAreaList(site.getSite_id());
        for(int i=0; i < area_list.size(); i++){
        	Asset area = area_list.get(i);
        	JSONObject level_1 = new JSONObject();
        	level_1.put("id", area.getAsset_id());
        	level_1.put("name", area.getAsset_name());
        	level_1.put("type", "AREA");
        	level_1.put("desc", area.getDescription());
        	level_1.put("symbol", "image:///resources/images/a_2.png");
        	
        	level_1.put("asset", JSONObject.fromObject(area));
        	
        	JSONArray e1_array = storage_client.forSelect().selectAssetEvent(area, 1);
        	level_1.put("event", (e1_array.size() > 0) ?  e1_array.getJSONObject(0) : new JSONObject());
        	
        	level_1.put("children", new JSONArray());
        	//
        	
        	List<Asset> equipment_list = asset_dao.selectEquipmentList(area.getAsset_id());
            for(int x=0; x < equipment_list.size(); x++){
            	Asset equipment = equipment_list.get(x);
            	JSONObject level_2 = new JSONObject();
            	level_2.put("id", equipment.getAsset_id());
            	level_2.put("name", equipment.getAsset_name());
            	level_2.put("type", "ASSET");
            	level_2.put("desc", equipment.getDescription());
            	level_2.put("symbol", "image:///resources/images/a_3.png");
            	
            	level_2.put("asset", JSONObject.fromObject(equipment));
            	JSONArray e2_array = storage_client.forSelect().selectAssetEvent(equipment, 1);
            	level_2.put("event", (e2_array.size() > 0) ?  e2_array.getJSONObject(0) : new JSONObject());
            	
            	level_2.put("children", new JSONArray());
            	//
            	
            	List<Tag> tag_list = tag_dao.selectTagListByAssetId(equipment.getAsset_id());
                for(int z=0; z < tag_list.size(); z++){
                	Tag tag = tag_list.get(z);
                	JSONObject level_3 = new JSONObject();
                	level_3.put("id", tag.getTag_id());
                	level_3.put("name", tag.getTag_name());
                	level_3.put("type", "TAG");
                	level_3.put("desc", tag.getDescription());
                	level_3.put("symbol", "image:///resources/images/a_4.png");
                	//level_3.put("children", new JSONArray());
                	
                	level_3.put("tag", JSONObject.fromObject(tag));
                	//
                	
                	//
                	level_2.getJSONArray("children").add(level_3);
                };
            	//
               level_1.getJSONArray("children").add(level_2);
            };
            //
        	root.getJSONArray("children").add(level_1);
        };
		}catch(Exception ex) {
			log.error("Site architecture tree load failed : " + ex.getMessage(), ex);
			throw ex;
		}
        return root;
	}

}
