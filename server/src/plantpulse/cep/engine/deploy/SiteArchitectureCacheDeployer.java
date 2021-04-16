package plantpulse.cep.engine.deploy;



import java.util.List;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import plantpulse.cep.dao.SiteDAO;
import plantpulse.cep.engine.logging.EngineLogger;
import plantpulse.cep.service.ArchitectureService;
import plantpulse.domain.Site;
import plantpulse.server.cache.UICache;

/**
 * SiteArchitectureCacheDeployer
 * @author leesa
 *
 */
public class SiteArchitectureCacheDeployer implements Deployer {

	private static final Log log = LogFactory.getLog(SiteArchitectureCacheDeployer.class);
	
	private SiteDAO site_dao = new SiteDAO();
	private ArchitectureService architecture_service = new ArchitectureService();
	

	/*
	 * 
	 */
	public void deploy() {
		UICache cache = UICache.getInstance();
		try {
			long start = System.currentTimeMillis();
			List<Site>  site_list = site_dao.selectSites();
			for(int i=0; i < site_list.size(); i++) {
				String site_id = site_list.get(i).getSite_id();
				cache.putJSON("ARCHITECTURE:" + site_id, architecture_service.getSiteArchitecture(site_id));
		        cache.putJSON("ARCHITECTURE_TREE:"  + site_id , architecture_service.getSiteArchitectureTree(site_id));
			};
			long end = System.currentTimeMillis() - start;
			log.info("Site architecture JSON cache completed : process_time=[" + end + "]ms");
		}catch(Exception ex) {
			EngineLogger.error("사이트 아키텍처 JSON을 캐쉬하는 도중 오류가 발생하였습니다 : 에러=" + ex.getMessage());
			log.warn("Site architecture JSON cache error : " + ex.getMessage(), ex);
		}
	}

}
