package plantpulse.server.mvc.architecture;

import java.util.List;

import javax.servlet.http.HttpServletRequest;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Controller;
import org.springframework.ui.Model;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.ResponseBody;

import plantpulse.cep.service.ArchitectureService;
import plantpulse.cep.service.AssetService;
import plantpulse.cep.service.SiteService;
import plantpulse.cep.service.TagService;
import plantpulse.cep.service.client.StorageClient;
import plantpulse.domain.Site;
import plantpulse.server.cache.UICache;
import plantpulse.server.mvc.assets.AssetsController;

@Controller
@RequestMapping(value = "/architecture")
public class ArchitectureController {

	private static final Log log = LogFactory.getLog(AssetsController.class);

	@Autowired
	private AssetService asset_service;
	
	@Autowired
	private TagService tag_service;

	@Autowired
	private SiteService site_service;
	
	@Autowired
	private StorageClient storage_client;
	
	private ArchitectureService architecture_service = new ArchitectureService();

	@RequestMapping(value = "/index", method = RequestMethod.GET)
	public String index(Model model) throws Exception {
		List<Site> site_list = site_service.selectSites();
		model.addAttribute("site_list", site_list);
		model.addAttribute("selected_site_id", (site_list != null && site_list.size() > 0) ? site_list.get(0).getSite_id() : "");
		return "architecture/index";
	}
	
	@RequestMapping(value = "/get/{site_id}", headers = "Accept=*/*", produces = "application/json;charset=utf-8")
	public @ResponseBody String get(@PathVariable("site_id") String site_id, Model model, HttpServletRequest request) throws Exception {
		String _CACHE_PREFIX = "ARCHITECTURE:";
		UICache cache = UICache.getInstance();
		if( !cache.hasJSON(_CACHE_PREFIX + site_id) ) { //캐쉬되지 않았다면 캐쉬 처리
			cache.putJSON(_CACHE_PREFIX + site_id, architecture_service.getSiteArchitecture(site_id));
		};
		return cache.getJSON(_CACHE_PREFIX + site_id).toString();
	};
	
	@RequestMapping(value = "/tree/{site_id}", method = RequestMethod.GET, headers = "Accept=*/*", produces = "application/json;charset=utf-8")
	public @ResponseBody String tree(@PathVariable("site_id") String site_id, Model model, HttpServletRequest request) throws Exception {
		Site site = site_service.selectSiteInfo(site_id);
		String _CACHE_PREFIX = "ARCHITECTURE_TREE:";
		UICache cache = UICache.getInstance();
		if( !cache.hasJSON(_CACHE_PREFIX + site_id) ) { //캐쉬되지 않았다면 캐쉬 처리
			cache.putJSON(_CACHE_PREFIX + site_id, architecture_service.getSiteArchitectureTree(site_id));
		};
		return cache.getJSON(_CACHE_PREFIX + site_id).toString();
	};
	
}
