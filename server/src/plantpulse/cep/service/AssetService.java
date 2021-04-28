package plantpulse.cep.service;

import java.util.ArrayList;
import java.util.List;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import com.espertech.esper.client.EPServiceProvider;
import com.espertech.esper.client.EPStatement;

import plantpulse.cep.dao.AssetDAO;
import plantpulse.cep.dao.AssetStatementDAO;
import plantpulse.cep.dao.SiteDAO;
import plantpulse.cep.domain.AssetStatement;
import plantpulse.cep.engine.CEPEngineManager;
import plantpulse.cep.service.support.asset.AssetStatementListener;
import plantpulse.cep.service.support.security.SecurityTools;
import plantpulse.domain.Asset;

@Service
public class AssetService {

	private static final Log log = LogFactory.getLog(AssetService.class);

	@Autowired
	private SiteDAO site_dao;
	
	@Autowired
	private AssetDAO asset_dao;
	
	@Autowired
	private AssetStatementDAO asset_statement_dao;
	
	
	public List<Asset> selectAreaList(String site_id) throws Exception {
		return asset_dao.selectAreaList(site_id);
	}
	
	public List<Asset> selectEquipmentList(String parent_asset_id) throws Exception {
		return asset_dao.selectEquipmentList(parent_asset_id);
	}
	
	public List<Asset> selectModels() throws Exception {
		return asset_dao.selectModels() ;
	}
	
	public List<Asset> selectParts() throws Exception {
		return asset_dao.selectParts();
	}

	public List<Asset> selectAssets(Asset asset) throws Exception {
		List<Asset> result = new ArrayList<Asset>();
		try {
			List<Asset> list = asset_dao.selectAssets(asset);
			//
			for (int i = 0; list != null && i < list.size(); i++) {
				Asset one = list.get(i);
				if (SecurityTools.hasPermission(one.getId())) {
					result.add(one);
				}
			}
			return result;

		} catch (Exception e) {
			log.error("" + e.getMessage(), e);
			return null;
		}
	};
	
	public List<Asset> assetListNoTags(Asset asset) throws Exception {
		List<Asset> result = new ArrayList<Asset>();
		try {
			List<Asset> list = asset_dao.selectAssetsNoTags(asset);
			//
			for (int i = 0; list != null && i < list.size(); i++) {
				Asset one = list.get(i);
				if (SecurityTools.hasPermission(one.getId())) {
					result.add(one);
				}
			}
			return result;

		} catch (Exception e) {
			log.error("" + e.getMessage(), e);
			return null;
		}
	};
	

	public Asset getAsset(String asset_id) throws Exception {
		return asset_dao.selectAsset(asset_id);
	}
	
	public Asset selectAsset(String asset_id) throws Exception {
		return asset_dao.selectAsset(asset_id);
	}
	
	public Asset selectAssetOrSite(String asset_id) throws Exception {
		return asset_dao.selectAssetOrSite(asset_id);
	};
	
	
	public void insertAsset(Asset asset) throws Exception {
		asset_dao.insertAsset(asset);
	}

	public void updateAsset(Asset asset) throws Exception {
		asset_dao.updateAsset(asset);
	}
	
	public void updateAssetWidhEquipment(Asset asset) throws Exception {
		asset_dao.updateAssetWidhEquipment(asset);
	}
	
	public void deleteAsset(Asset asset) throws Exception {
		asset_dao.deleteAsset(asset);
	}

	public void updateMoveAsset(Asset asset) throws Exception {
		asset_dao.updateMoveAsset(asset);
	}

	public void updateMoveAssetOrder(Asset asset) throws Exception {
		asset_dao.updateMoveAssetOrder(asset);
	}
	
	
	public AssetStatement selectAssetStatement(String asset_id, String statement_name) throws Exception {
		return asset_statement_dao.selectAssetStatement(asset_id, statement_name);
	}
	
	
	public List<AssetStatement> selectAssetStatementByAssetId(String asset_id) throws Exception {
		return asset_statement_dao.selectAssetStatementByAssetId(asset_id);
	}
	
	public void insertAssetStatement(AssetStatement asset_statement) throws Exception {
		asset_statement_dao.insertAssetStatement(asset_statement);
	}
	
	public void updateAssetStatement(AssetStatement asset_statement) throws Exception {
		asset_statement_dao.updateAssetStatement(asset_statement);
	}
	
	
	public void deleteAssetStatement(AssetStatement asset_statement) throws Exception {
		asset_statement_dao.deleteAssetStatement(asset_statement);
	}
	
	
	public void deployAssetStatement(AssetStatement asset_statement) {
		try {
			Asset asset = selectAssetOrSite(asset_statement.getAsset_id());
			EPServiceProvider provider = CEPEngineManager.getInstance().getProvider();
			EPStatement stmt = null;
			stmt = provider.getEPAdministrator().createEPL(asset_statement.getEql(), asset_statement.getAsset_id() + "_" + asset_statement.getStatement_name());
			stmt.addListener(new AssetStatementListener(asset_statement, asset));
			log.info("Deployed AssetStatement = [" + asset_statement.getAsset_id() + "_" + asset_statement.getStatement_name() + "]");
		}catch(Exception ex){
			log.error("AssetStatement deploy error : " + ex.getMessage(), ex);
		}
	}

	public void undeployAssetStatement(AssetStatement asset_statement) {
		EPServiceProvider provider = CEPEngineManager.getInstance().getProvider();
		if (provider.getEPAdministrator().getStatement(asset_statement.getAsset_id() + "_" + asset_statement.getStatement_name()) != null) {
			provider.getEPAdministrator().getStatement(asset_statement.getAsset_id() + "_" + asset_statement.getStatement_name()).destroy();
		} else {
			log.warn("Undeployed AssetStatement = [" + asset_statement.getAsset_id() + "_" + asset_statement.getStatement_name() + "]");
		}
		//
	}



}
