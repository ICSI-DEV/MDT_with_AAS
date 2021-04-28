package plantpulse.cep.engine.deploy;

import java.util.List;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import com.espertech.esper.client.EPServiceProvider;
import com.espertech.esper.client.EPStatement;

import plantpulse.cep.dao.AssetDAO;
import plantpulse.cep.dao.AssetStatementDAO;
import plantpulse.cep.dao.SiteDAO;
import plantpulse.cep.domain.AssetStatement;
import plantpulse.cep.engine.CEPEngineManager;
import plantpulse.cep.engine.logging.EngineLogger;
import plantpulse.cep.service.support.asset.AssetStatementListener;
import plantpulse.domain.Asset;

/**
 * AssetStatementDeployer
 * @author lsb
 *
 */
public class AssetStatementDeployer implements Deployer  {

	private static final Log log = LogFactory.getLog(AssetStatementDeployer.class);

	public void deploy() {

		//
		try {
			
			SiteDAO site_dao = new SiteDAO();
			AssetDAO asset_dao = new AssetDAO();
			AssetStatementDAO dao = new AssetStatementDAO();
			List<AssetStatement> list = dao.selectAssetStatements();
			//
			for (int i = 0; list != null && i < list.size(); i++) {
				AssetStatement asset_statement = list.get(i);
				try{
					Asset asset = asset_dao.selectAssetOrSite(asset_statement.getAsset_id());
					EPServiceProvider provider = CEPEngineManager.getInstance().getProvider();
					EPStatement stmt = null;
					stmt = provider.getEPAdministrator().createEPL(asset_statement.getEql(), asset_statement.getAsset_id() + "_" + asset_statement.getStatement_name());
					stmt.addListener(new AssetStatementListener(asset_statement, asset));
					log.info("Deployed Asset's statement = [" + asset_statement.getAsset_id() + "_" + asset_statement.getStatement_name() + "]");
				}catch(Exception ex){
					EngineLogger.error("에셋 스테이트먼트[" + asset_statement.getAsset_id() + "_" + asset_statement.getStatement_name() + "]를 배치하는도중 오류가 발생하였습니다 : 에러=" + ex.getMessage());
					log.warn("AssetModelEvent deploy error : " + ex.getMessage(), ex);
				}

			}

			//
		} catch (Exception ex) {
			EngineLogger.error("에셋 스테이트먼트를 배치하는도중 오류가 발생하였습니다 : 에러=" + ex.getMessage());
			log.warn("AssetStatement deploy error : " + ex.getMessage(), ex);
		}

	}

}
