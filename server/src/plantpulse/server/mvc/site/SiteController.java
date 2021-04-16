package plantpulse.server.mvc.site;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.json.JSONObject;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Controller;
import org.springframework.ui.Model;
import org.springframework.web.bind.annotation.ModelAttribute;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.ResponseBody;
import org.springframework.web.servlet.mvc.support.RedirectAttributes;

import com.google.common.base.Preconditions;

import plantpulse.cep.engine.model.SiteMetaModelManager;
import plantpulse.cep.service.CompanyService;
import plantpulse.cep.service.SiteService;
import plantpulse.domain.Company;
import plantpulse.domain.Site;
import plantpulse.server.mvc.JSONResult;
import plantpulse.server.mvc.Result;

@Controller
@RequestMapping(value = "/site")
public class SiteController {

	private static final Log log = LogFactory.getLog(SiteController.class);

	@Autowired
	private SiteService siteService;

	@Autowired
	private CompanyService companyService;
	
	
	@RequestMapping(value = "/get/{site_id}", method = RequestMethod.GET)
	public @ResponseBody String get(@PathVariable("site_id") String site_id, Model model, HttpServletRequest request) throws Exception {
		Site site = siteService.selectSiteInfo(site_id);
		Preconditions.checkNotNull(site, "Site is null.");
		return JSONObject.fromObject(site).toString();
	}

	@RequestMapping(value = "/siteList")
	public @ResponseBody List<Site> siteList() {
		List<Site> siteList = siteService.selectSites();
		Preconditions.checkNotNull(siteList, "Site list is null.");
		return siteList;
	}

	@RequestMapping(value = "/addSite", method = RequestMethod.GET)
	public String addSite(Model model) throws Exception {
		//
		model.addAttribute("mode", "I"); // I:insert
		model.addAttribute("site", siteService.getDefaultSite());
		model.addAttribute("companyList", companyService.selectCompany());
		return "asset/siteForm";
	}

	@RequestMapping(value = "/editSite", method = RequestMethod.GET)
	public String editSite(Model model, HttpServletRequest request) throws Exception {
		//
		String siteId = request.getParameter("siteID");
		model.addAttribute("mode", "U"); // U:update
		model.addAttribute("site", siteService.selectSiteInfo(siteId));
		model.addAttribute("companyList", companyService.selectCompany());
		return "asset/siteForm";
	}

	@RequestMapping(value = "/insertCompany", method = RequestMethod.POST)
	public @ResponseBody JSONResult insertCompany(@ModelAttribute Company company, Model model) throws Exception {

		Preconditions.checkNotNull(company, "Company is null.");
		JSONResult jsonResult = new JSONResult();

		try {
			companyService.insertCompany(company);

			Map<String, Object> map = new HashMap<String, Object>();
			map.put("companyList", companyService.selectCompany());

			jsonResult.setStatus(JSONResult.SUCCESS);
			jsonResult.setData(map);
		} catch (Exception e) {
			log.error("\n SiteController.deleteSite() Exception : " + e.getMessage(), e);
			jsonResult.setStatus(JSONResult.ERROR);
		}

		return jsonResult;
	}

	@RequestMapping(value = "/updateCompany", method = RequestMethod.POST)
	public @ResponseBody JSONResult updateCompany(@ModelAttribute Company company, Model model) throws Exception {

		Preconditions.checkNotNull(company, "Company is null.");
		JSONResult jsonResult = new JSONResult();
		try {
			companyService.updateCompany(company);

			Map<String, Object> map = new HashMap<String, Object>();
			map.put("companyList", companyService.selectCompany());

			jsonResult.setStatus(JSONResult.SUCCESS);
			jsonResult.setData(map);
		} catch (Exception e) {
			log.error("\n SiteController.deleteSite() Exception : " + e.getMessage(), e);
			jsonResult.setStatus(JSONResult.ERROR);
		}

		return jsonResult;
	}

	@RequestMapping(value = "/deleteCompany", method = RequestMethod.POST)
	public @ResponseBody JSONResult deleteCompany(HttpServletRequest request, HttpServletResponse response, @ModelAttribute Company company, Model model) {

		JSONResult jsonResult = new JSONResult();
		Preconditions.checkNotNull(company, "Company is null.");
		try {
			companyService.deleteCompany(company);

			Map<String, Object> map = new HashMap<String, Object>();
			map.put("companyList", companyService.selectCompany());

			jsonResult.setStatus(JSONResult.SUCCESS);
			jsonResult.setData(map);
		} catch (Exception e) {
			log.error("\n SiteController.deleteSite() Exception : " + e.getMessage(), e);
			jsonResult.setStatus(JSONResult.ERROR);
		}

		return jsonResult;
	}

	@RequestMapping(value = "/insertSite", method = RequestMethod.POST)
	public String insertSite(@ModelAttribute Site site, RedirectAttributes redirectAttr, Model model) throws Exception {
		//
		Preconditions.checkNotNull(site, "Site is null.");
		try {
			siteService.insertSite(site);
			redirectAttr.addFlashAttribute("result", new Result(Result.SUCCESS, "사이트를 저장하였습니다."));
			return "redirect:/asset/index";
		} catch (Exception e) {
			log.error("\n SiteController.insertSite() Exception : " + e.getMessage(), e);
			model.addAttribute("result", new Result(Result.ERROR, "사이트를 저장할 수 없습니다. : 메세지 = [" + e.getMessage() + "]"));
			return "asset/siteForm";
		}
	}

	@RequestMapping(value = "/updateSite", method = RequestMethod.POST)
	public String updateSite(@ModelAttribute Site site, RedirectAttributes redirectAttr, Model model) throws Exception {
		//
		Preconditions.checkNotNull(site, "Site is null.");
		try {
			siteService.updateSite(site);
			redirectAttr.addFlashAttribute("result", new Result(Result.SUCCESS, "사이트를 수정하였습니다."));
			return "redirect:/asset/index";
		} catch (Exception e) {
			log.error("\n SiteController.updateSite() Exception : " + e.getMessage(), e);
			model.addAttribute("result", new Result(Result.ERROR, "사이트를 수정할 수 없습니다. : 메세지 = [" + e.getMessage() + "]"));
			return "asset/siteForm";
		}
	}

	@RequestMapping(value = "/deleteSite", method = RequestMethod.POST)
	public String deleteSite(@ModelAttribute Site site, RedirectAttributes redirectAttr, Model model) {
		//
		Preconditions.checkNotNull(site, "Site is null.");
		try {
			siteService.deleteSite(site);
			redirectAttr.addFlashAttribute("result", new Result(Result.SUCCESS, "사이트를 삭제하였습니다."));
			return "redirect:/asset/index";
		} catch (Exception e) {
			log.error("\n SiteController.deleteSite() Exception : " + e.getMessage(), e);
			model.addAttribute("result", new Result(Result.ERROR, "사이트를 삭제할 수 없습니다. : 메세지 = [" + e.getMessage() + "]"));
			return "asset/siteForm";
		}
	}

	@RequestMapping(value = "/moveSite.do", method = RequestMethod.POST)
	public void moveSite(HttpServletResponse response, @ModelAttribute Site site) {
		Preconditions.checkNotNull(site, "Site is null.");

		siteService.updateMoveSite(site);
	}

	@RequestMapping(value = "/companyIsDuplicated", method = RequestMethod.POST)
	public @ResponseBody JSONResult companyIsDuplicated(@RequestParam("company_name") String company_name) throws Exception {

		JSONResult jsonResult = new JSONResult();

		try {
			if (siteService.companyIsDuplicated(company_name)) {
				jsonResult.setMessage("Y");
			} else {
				jsonResult.setMessage("N");
			}
		} catch (Exception e) {
			log.error("\n SiteController.deleteSite() Exception : " + e.getMessage());
			jsonResult.setMessage("ERROR");
		}

		return jsonResult;
	}

	@RequestMapping(value = "/companyHasSite", method = RequestMethod.POST)
	public @ResponseBody JSONResult companyHasSite(@RequestParam("company_id") String company_id) throws Exception {

		JSONResult jsonResult = new JSONResult();

		try {
			if (siteService.companyHasSite(company_id)) {
				jsonResult.setMessage("Y");
			} else {
				jsonResult.setMessage("N");
			}
		} catch (Exception e) {
			log.error("\n SiteController.companyHasSite() Exception : " + e.getMessage());
			jsonResult.setMessage("ERROR");
		}

		return jsonResult;
	}

	@RequestMapping(value = "/siteHasAssetOrOPC", method = RequestMethod.POST)
	public @ResponseBody JSONResult siteHasAssetOrOPC(@RequestParam("site_id") String site_id) throws Exception {

		JSONResult jsonResult = new JSONResult();

		try {
			if (siteService.siteHasAssetOrOPC(site_id)) {
				jsonResult.setMessage("Y");
			} else {
				jsonResult.setMessage("N");
			}
		} catch (Exception e) {
			log.error("\n SiteController.siteHasAssetOrOPC() Exception : " + e.getMessage());
			jsonResult.setMessage("ERROR");
		}

		return jsonResult;
	}

	@RequestMapping(value = "/assetHasModel", method = RequestMethod.POST)
	public @ResponseBody JSONResult assetHasModel(@RequestParam("asset_id") String asset_id) throws Exception {

		JSONResult jsonResult = new JSONResult();

		try {
			if (siteService.assetHasModel(asset_id)) {
				jsonResult.setMessage("Y");
			} else {
				jsonResult.setMessage("N");
			}
		} catch (Exception e) {
			log.error("\n SiteController.assetHasModel() Exception : " + e.getMessage());
			jsonResult.setMessage("ERROR");
		}

		return jsonResult;
	}

	@RequestMapping(value = "/modelHasTag", method = RequestMethod.POST)
	public @ResponseBody JSONResult modelHasTag(@RequestParam("asset_id") String asset_id) throws Exception {

		JSONResult jsonResult = new JSONResult();

		try {
			if (siteService.modelHasTag(asset_id)) {
				jsonResult.setMessage("Y");
			} else {
				jsonResult.setMessage("N");
			}
		} catch (Exception e) {
			log.error("\n SiteController.modelHasTag() Exception : " + e.getMessage());
			jsonResult.setMessage("ERROR");
		}

		return jsonResult;
	}

	@RequestMapping(value = "/opcHasTag", method = RequestMethod.POST)
	public @ResponseBody JSONResult opcHasTag(@RequestParam("opc_id") String opc_id) throws Exception {

		JSONResult jsonResult = new JSONResult();

		try {
			if (siteService.opcHasTag(opc_id)) {
				jsonResult.setMessage("Y");
			} else {
				jsonResult.setMessage("N");
			}
		} catch (Exception e) {
			log.error("\n SiteController.opcHasTag() Exception : " + e.getMessage());
			jsonResult.setMessage("ERROR");
		}

		return jsonResult;
	}
	
	
	@RequestMapping(value = "/updateModel", method = RequestMethod.POST)
	public @ResponseBody JSONResult updateModel(HttpServletResponse response) throws Exception {
		
		//
		SiteMetaModelManager smmu = new SiteMetaModelManager();
		smmu.updateModel();
		
		JSONResult result = new JSONResult();
		result.setStatus(JSONResult.SUCCESS);
		return result;
	}
}