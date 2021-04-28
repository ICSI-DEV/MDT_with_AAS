package plantpulse.cep.service;

import java.util.List;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import plantpulse.cep.dao.AssetDAO;
import plantpulse.cep.dao.MetadataDAO;
import plantpulse.cep.dao.OPCDAO;
import plantpulse.cep.dao.SiteDAO;
import plantpulse.cep.dao.TagDAO;
import plantpulse.domain.Asset;
import plantpulse.domain.Metadata;
import plantpulse.domain.OPC;
import plantpulse.domain.Site;
import plantpulse.domain.Tag;
import plantpulse.json.JSONArray;
import plantpulse.json.JSONObject;

/**
 * MetadataService
 * @author leesa
 *
 */

@Service
public class MetadataService {
	
	private static final Log log = LogFactory.getLog(MetadataService.class);

	@Autowired
	private MetadataDAO metadata_dao;
	
	@Autowired
	private SiteDAO site_dao;
	@Autowired
	private OPCDAO opc_dao;
	@Autowired
	private TagDAO tag_dao;
	@Autowired
	private AssetDAO asset_dao;
	
	public JSONArray selectObjectListForMetadata() throws Exception {
		JSONArray array = new JSONArray();
		List<Site> site_list = site_dao.selectSites();
		List<OPC> opc_list = opc_dao.selectOpcList();
		List<Asset> asset_list = asset_dao.selectAssets();
		List<Tag> tag_list = tag_dao.selectTagAll();
		
		for(Site site : site_list) {
			JSONObject json = new JSONObject();
			json.put("id", site.getSite_id());
			json.put("name", site.getSite_name());
			json.put("description", site.getDescription());
			array.add(json);
		}
		
		for(OPC opc : opc_list) {
			JSONObject json = new JSONObject();
			json.put("id", opc.getOpc_id());
			json.put("name", opc.getOpc_name());
			json.put("description", opc.getDescription());
			array.add(json);
		}
		
		for(Asset asset : asset_list) {
			JSONObject json = new JSONObject();
			json.put("id", asset.getAsset_id());
			json.put("name", asset.getAsset_name());
			json.put("description", asset.getDescription());
			array.add(json);
		}
		
		for(Tag tag : tag_list) {
			JSONObject json = new JSONObject();
			json.put("id", tag.getTag_id());
			json.put("name", tag.getTag_name());
			json.put("description", tag.getDescription());
			array.add(json);
		}
		
		return array;
	}
	
	public List<Metadata> selectMetadataList() throws Exception {
		return metadata_dao.selectMetadataList();
	}

	public List<Metadata> selectMetadataList(String object_id) throws Exception {
		return metadata_dao.selectMetadataList(object_id);
	}

	public Metadata selectMetadata(String object_id, String key) throws Exception {
		return metadata_dao.selectMetadata(object_id, key);
	}
	
	public void updateMetadataList(String object_id, List<Metadata> metadata_list) throws Exception {
		metadata_dao.deleteMetadataList(object_id); 
		metadata_dao.insertMetadataList(metadata_list);
	}

	public void insertMetadataList(List<Metadata> metadata_list) throws Exception {
		metadata_dao.insertMetadataList(metadata_list);
	}

	public void updateMetadata(Metadata metadata) throws Exception {
		metadata_dao.updateMetadata(metadata);
	}

	public void deleteMetadataList(String object_id) throws Exception {
		metadata_dao.deleteMetadataList(object_id);  
	}


}
