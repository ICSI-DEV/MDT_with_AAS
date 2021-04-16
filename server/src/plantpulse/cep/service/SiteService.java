package plantpulse.cep.service;

import java.util.ArrayList;
import java.util.List;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.json.JSONObject;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import plantpulse.cep.dao.CompanyDAO;
import plantpulse.cep.dao.SiteDAO;
import plantpulse.cep.service.client.StorageClient;
import plantpulse.cep.service.support.security.SecurityTools;
import plantpulse.domain.Company;
import plantpulse.domain.Site;

/**
 * SiteService
 * @author lsb
 *
 */
@Service
public class SiteService {

	private static final Log log = LogFactory.getLog(SiteService.class);

	@Autowired
	private CompanyDAO company_dao;
	
	@Autowired
	private SiteDAO siteDAO;

	@Autowired
	private StorageClient storage_client;

	public SiteService() {

	}

	public Company getCompany() throws Exception {
		return company_dao.selectCompany();
	}
	
	public List<Site> selectSites() {
		//
		List<Site> result = new ArrayList<Site>();

		try {
			List<Site> list = siteDAO.selectSites();
			for (int i = 0; i < list.size(); i++) {
				Site site = list.get(i);
				long total_data_count = storage_client.forSelect().countPointTotalBySite(site);
				site.setTotal_data_count(total_data_count);
				if (SecurityTools.hasPermission(site.getSite_id())) {
					result.add((site));
				}
			};
		} catch (Exception e) {
			log.error("Site list getting exception : " + e.getMessage(), e);
		}
		//
		//세션에 기본 사이트를 0번으로 선택하여 준다.
		return result;
	}

	public Site selectSiteInfo(String siteId) {
		//
		try {
			return (siteDAO.selectSiteInfo(siteId));
		} catch (Exception e) {
			log.error("Site info list getting exception : " + e.getMessage(), e);
			return new Site();
		}
	}

	public void insertSite(Site site) throws Exception {
		siteDAO.insertSite(site);
		//storage_client.getInsertDAO().createKeyspace(); // 카산드라 키스페이스 생성
	}

	public void updateSite(Site site) throws Exception {
		siteDAO.updateSite(site);
		//storage_client.getInsertDAO().createKeyspace();
	}

	public JSONObject selectSiteSummary(Site site, int year, int month, int day) throws Exception {
		return storage_client.forSelect().selectSiteSummary(site, year, month, day);
	}

	public void deleteSite(Site site) throws Exception {
		siteDAO.deleteSite(site);
	}

	public void updateMoveSite(Site site) {
		siteDAO.updateMoveSite(site);
	}

	public boolean companyIsDuplicated(String company_name) throws Exception {
		return siteDAO.companyIsDuplicated(company_name);
	}

	public boolean companyHasSite(String company_id) throws Exception {
		return siteDAO.companyHasSite(company_id);
	}

	public boolean siteHasAssetOrOPC(String site_id) throws Exception {
		return siteDAO.siteHasAssetOrOPC(site_id);
	}

	public boolean assetHasModel(String asset_id) throws Exception {
		return siteDAO.assetHasModel(asset_id);
	}

	public boolean modelHasTag(String asset_id) throws Exception {
		return siteDAO.modelHasTag(asset_id);
	}

	public boolean opcHasTag(String opc_id) throws Exception {
		return siteDAO.opcHasTag(opc_id);
	}
	
	public Site getDefaultSite() throws Exception{
		return siteDAO.overrideSiteDefautInfo(new Site());
	}

	
}
