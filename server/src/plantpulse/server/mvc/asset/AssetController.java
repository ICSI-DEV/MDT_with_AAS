package plantpulse.server.mvc.asset;

import java.util.List;

import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.json.JSONArray;
import org.json.JSONObject;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Controller;
import org.springframework.ui.Model;
import org.springframework.web.bind.annotation.ModelAttribute;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.ResponseBody;
import org.springframework.web.servlet.mvc.support.RedirectAttributes;

import com.google.common.base.Preconditions;

import plantpulse.cep.dao.BackupDAO;
import plantpulse.cep.engine.model.SiteMetaModelManager;
import plantpulse.cep.service.AssetService;
import plantpulse.cep.service.SiteService;
import plantpulse.cep.service.client.StorageClient;
import plantpulse.cep.service.support.security.SecurityTools;
import plantpulse.domain.Asset;
import plantpulse.domain.Site;
import plantpulse.server.cache.UICache;
import plantpulse.server.mvc.JSONResult;
import plantpulse.server.mvc.util.DateUtils;

@Controller
@RequestMapping(value = "/asset")
public class AssetController {

	private static final Log log = LogFactory.getLog(AssetController.class);

	@Autowired
	private AssetService assetService;

	@Autowired
	private SiteService siteService;
	
	@RequestMapping(value = "/config", method = RequestMethod.GET)
	public String config(Model model) throws Exception {

		List<Site> site_list = siteService.selectSites();
		model.addAttribute("site_list", site_list);

		return "asset/index";
	}

	@RequestMapping(value = "/index", method = RequestMethod.GET)
	public String index(Model model) throws Exception {

		List<Site> site_list = siteService.selectSites();
		model.addAttribute("site_list", site_list);

		return "asset/index";
	}
	
	@RequestMapping(value = "/view", method = RequestMethod.GET)
	public String view(Model model, HttpServletRequest request) throws Exception {
		String search_date_from = DateUtils.currDateByMinus10Minutes();
		String search_date_to = DateUtils.currDateBy24();
		model.addAttribute("total_log_count", 0);
		model.addAttribute("search_date_from", search_date_from);
		model.addAttribute("search_date_to", search_date_to);

		List<Site> site_list = siteService.selectSites();
		model.addAttribute("site_list", site_list);
		model.addAttribute("selected_site_id", (site_list != null && site_list.size() > 0) ? site_list.get(0).getSite_id() : "");
		return "asset/view";
	}

	@SuppressWarnings("unchecked")
	@RequestMapping(value = "/assetList")
	public @ResponseBody List<Asset> loadAssetList(@ModelAttribute Asset asset) throws Exception {
		Preconditions.checkNotNull(asset, "Asset is null.");
		//
		UICache cache = UICache.getInstance();
		String _CACHE_KEY = "NAV_ASSET_LIST" + ":" + asset.getSite_id() + ":" + asset.isShow_description() + ":" + SecurityTools.getRoleAndSecurity().getUser_id();
		if( !cache.hasObject(_CACHE_KEY) ) { //캐쉬되지 않았다면 캐쉬 처리
			List<Asset> assetList = assetService.selectAssets(asset);
			cache.putObject(_CACHE_KEY, assetList);
		};
		return (List<Asset>) cache.getObject(_CACHE_KEY);
	}
	
	@SuppressWarnings("unchecked")
	@RequestMapping(value = "/assetListNoCache")
	public @ResponseBody List<Asset> loadAssetListNoCache(@ModelAttribute Asset asset) throws Exception {
		Preconditions.checkNotNull(asset, "Asset is null.");
		//
		List<Asset> assetList = assetService.selectAssets(asset);
		return assetList;
	}
	
	@SuppressWarnings("unchecked")
	@RequestMapping(value = "/assetListNoTags")
	public @ResponseBody List<Asset> assetListNoTags(@ModelAttribute Asset asset) throws Exception {

		Preconditions.checkNotNull(asset, "Asset is null.");
		//
		UICache cache = UICache.getInstance();
		String _CACHE_KEY = "NAV_ASSET_LIST_NO_TAGS" + ":" + asset.getSite_id() + ":"  + asset.isShow_description() + ":" + SecurityTools.getRoleAndSecurity().getUser_id();
		if( !cache.hasObject(_CACHE_KEY) ) { //캐쉬되지 않았다면 캐쉬 처리
			List<Asset> assetList = assetService.assetListNoTags(asset);
			cache.putObject(_CACHE_KEY, assetList);
		};
		return (List<Asset>) cache.getObject(_CACHE_KEY);
	}

	@RequestMapping(value = "/insertAsset", method = RequestMethod.POST)
	public @ResponseBody JSONResult insertAsset(@ModelAttribute Asset asset) throws Exception {
		//
		Preconditions.checkNotNull(asset, "Asset is null.");
		try {
			assetService.insertAsset(asset);
			return new JSONResult(JSONResult.SUCCESS, "에셋을 저장하였습니다.");
		} catch (Exception e) {
			log.error("\n CompanyController.insertAsset() Exception : " + e.getMessage(), e);
			return new JSONResult(JSONResult.ERROR, "에셋를 저장할 수 없습니다. : 메세지 = [" + e.getMessage() + "]");
		}
	}

	@RequestMapping(value = "/updateAsset", method = RequestMethod.POST)
	public @ResponseBody JSONResult updateAsset(@ModelAttribute Asset asset) throws Exception {
		//
		Preconditions.checkNotNull(asset, "Asset is null.");
		try {
			assetService.updateAsset(asset);
			return new JSONResult(JSONResult.SUCCESS, "에셋을 수정하였습니다.");
		} catch (Exception e) {
			log.error("\n CompanyController.updateAsset() Exception : " + e.getMessage(), e);
			return new JSONResult(JSONResult.ERROR, "에셋을 수정할 수 없습니다. : 메세지 = [" + e.getMessage() + "]");
		}
	};
	
	@RequestMapping(value = "/updateAssetWidhEquipment", method = RequestMethod.POST)
	public @ResponseBody JSONResult updateAssetWidhEquipment(@ModelAttribute Asset asset) throws Exception {
		//
		Preconditions.checkNotNull(asset, "Asset is null.");
		try {
			assetService.updateAssetWidhEquipment(asset);
			return new JSONResult(JSONResult.SUCCESS, "에셋을 수정하였습니다.");
		} catch (Exception e) {
			log.error("\n CompanyController.updateAssetWidhEquipment() Exception : " + e.getMessage(), e);
			return new JSONResult(JSONResult.ERROR, "에셋을 수정할 수 없습니다. : 메세지 = [" + e.getMessage() + "]");
		}
	};
	

	@RequestMapping(value = "/deleteAsset", method = RequestMethod.POST)
	public @ResponseBody JSONResult deleteAsset(@ModelAttribute Asset asset, RedirectAttributes redirectAttr, Model model) throws Exception {
		//
		Preconditions.checkNotNull(asset, "Asset is null.");
		try {
			assetService.deleteAsset(asset);
			return new JSONResult(JSONResult.SUCCESS, "에셋을 삭제하였습니다.");
		} catch (Exception e) {
			log.error("\n CompanyController.deleteAsset() Exception : " + e.getMessage(), e);
			return new JSONResult(JSONResult.ERROR, "에셋을 삭제할 수 없습니다. : 메세지 = [" + e.getMessage() + "]");
		}
	}

	@RequestMapping(value = "/moveAsset.do", method = RequestMethod.POST)
	public void moveAsset(HttpServletResponse response, @ModelAttribute Asset asset) throws Exception {

		Preconditions.checkNotNull(asset, "Asset is null.");

		assetService.updateMoveAssetOrder(asset);
		assetService.updateMoveAsset(asset);

	}

	@RequestMapping(value = "/deployAssetToEvent", method = RequestMethod.POST)
	public @ResponseBody JSONResult deployAssetToEvent(HttpServletResponse response, @ModelAttribute Asset asset) throws Exception {
		
		//
		SiteMetaModelManager smmu = new SiteMetaModelManager();
		smmu.updateModel();
		
		JSONResult result = new JSONResult();
		result.setStatus(JSONResult.SUCCESS);
		return result;
	}

	@RequestMapping(value = "/get/{asset_id}", method = RequestMethod.GET, headers = "Accept=*/*", produces = "application/json")
	public @ResponseBody JSONResult get(@PathVariable String asset_id, Model model, HttpServletRequest request) throws Exception {
		JSONResult json = new JSONResult();
		json.getData().put("asset", assetService.getAsset(asset_id)); //
		json.setStatus("SUCCESS");
		//
		return json;
	}
	
	@RequestMapping(value = "/get/last/event/{asset_id}", method = RequestMethod.GET, headers = "Accept=*/*", produces = "application/json")
	public @ResponseBody JSONResult getLastEvent(@PathVariable String asset_id, Model model, HttpServletRequest request) throws Exception {
		
		StorageClient client = new StorageClient();
		Asset asset = assetService.getAsset(asset_id);
		JSONArray array = client.forSelect().selectAssetEvent(asset, "2000-01-01 00:00:00", "3000-01-01 00:00:00", 1);
		JSONResult json = new JSONResult();
		json.getData().put("asset", asset); //
		if(array != null && array.size() > 0){
			json.getData().put("event", array.getJSONObject(0)); //
		}else{
			json.getData().put("event", new JSONObject()); //
		}
		json.setStatus("SUCCESS");
		//
		return json;
	};
	
	
	@RequestMapping(value = "/backup", method = RequestMethod.POST)
	public @ResponseBody JSONResult backup(HttpServletResponse response, @ModelAttribute Asset asset) throws Exception {
		
		//
		BackupDAO dao = new BackupDAO();
		dao.backup();
		
		JSONResult result = new JSONResult();
		result.setStatus(JSONResult.SUCCESS);
		return result;
	}

}