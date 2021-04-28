package plantpulse.cep.engine.deploy;

import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import plantpulse.cep.dao.AssetDAO;
import plantpulse.cep.engine.asset.AssetCacheFactory;
import plantpulse.cep.engine.async.AsynchronousExecutor;
import plantpulse.cep.engine.async.Worker;
import plantpulse.cep.engine.logging.EngineLogger;
import plantpulse.domain.Asset;

/**
 * AssetCacheDeployer
 * 
 * @author leesa
 *
 */
public class AssetCacheDeployer implements Deployer {

	private static final Log log = LogFactory.getLog(AssetCacheDeployer.class);
	
	private AtomicInteger counter = new AtomicInteger(0);

	public void deploy() {
		
		AsynchronousExecutor exec = new AsynchronousExecutor();
		exec.execute(new Worker() {
			public String getName() { return "AssetCacheDeployer"; };
			public void execute() {
					//
					try {
						AssetDAO asset_dao = new AssetDAO();
						long start = System.currentTimeMillis();
						List<Asset> list = asset_dao.selectAssets();
						if(list != null && list.size() > 0) {
							log.info("Caching deployed asset ... : count=[" + list.size() + "]");
							for (int i = 0; list != null && i < list.size(); i++) {
								Asset asset = list.get(i);
								AssetCacheFactory.getInstance().putAsset(asset.getAsset_id(), asset);
							};
						}else {
							log.info("Deployed asset does not exist. asset_size=[0]");
						}
						long end = System.currentTimeMillis() - start; 
					} catch (Exception ex) {
						EngineLogger.error("전체 에셋을 캐싱하는 도중 오류가 발생하였습니다 : 에러=" + ex.getMessage());
						log.warn("Asset cache deploy error : " + ex.getMessage(), ex);
					}
			}
		});
			
	}

}
