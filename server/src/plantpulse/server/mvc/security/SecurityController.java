package plantpulse.server.mvc.security;

import java.util.List;

import javax.servlet.http.HttpServletRequest;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.json.JSONArray;
import org.json.JSONObject;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Controller;
import org.springframework.ui.Model;
import org.springframework.validation.BindingResult;
import org.springframework.web.bind.annotation.ModelAttribute;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.ResponseBody;

import plantpulse.cep.service.SecurityService;
import plantpulse.cep.service.SiteService;
import plantpulse.cep.service.TagService;
import plantpulse.domain.Security;
import plantpulse.domain.Site;
import plantpulse.server.mvc.CRUDKeys;
import plantpulse.server.mvc.JSONResult;
import plantpulse.server.mvc.Result;
import plantpulse.server.mvc.util.SessionUtils;

@Controller
public class SecurityController {

	private static final Log log = LogFactory.getLog(SecurityController.class);

	private SecurityService security_service = new SecurityService();

	@Autowired
	private SiteService site_service;

	@Autowired
	private TagService tag_service;

	//
	public Model setDefault(Model model, String insert_security_id) throws Exception {
		List<Security> security_list = security_service.getSecurityList();
		model.addAttribute("security_list", security_list);
		return model;
	}

	@RequestMapping(value = "/security/index", method = RequestMethod.GET)
	public String index(Model model, HttpServletRequest request) throws Exception {
		setDefault(model, SessionUtils.getUserFromSession(request).getUser_id());
		return "security/index";
	}

	@RequestMapping(value = "/security/array", method = RequestMethod.GET, produces = "application/json; charset=UTF-8")
	public @ResponseBody String array(Model model, HttpServletRequest request) throws Exception {
		List<Security> security_list = security_service.getSecurityList();
		JSONArray array = new JSONArray();
		for (int i = 0; i < security_list.size(); i++) {
			Security security = security_list.get(i);
			JSONObject obj = new JSONObject();
			obj.put("security_id", security.getSecurity_id());
			obj.put("security_name", security.getSecurity_name());
			array.add(obj);
		}
		return array.toString();
	}

	@RequestMapping(value = "/security/add", method = RequestMethod.GET)
	public String add(Model model, HttpServletRequest request) throws Exception {
		Security security = new Security();
		security.setMode(CRUDKeys.INSERT);
		model.addAttribute("security", security);

		//
		List<Site> site_list = site_service.selectSites();
		model.addAttribute("site_list", site_list);
		model.addAttribute("selected_site_id", (site_list != null && site_list.size() > 0) ? site_list.get(0).getSite_id() : "");

		return "security/form";
	}

	@RequestMapping(value = "/security/edit/{security_id:.+}", method = RequestMethod.GET)
	public String form(@PathVariable("security_id") String security_id, Model model, HttpServletRequest request) throws Exception {
		Security security = security_service.getSecurity(security_id);
		security.setMode(CRUDKeys.UPDATE);
		model.addAttribute("security", security);

		//
		List<Site> site_list = site_service.selectSites();
		model.addAttribute("site_list", site_list);
		model.addAttribute("selected_site_id", (site_list != null && site_list.size() > 0) ? site_list.get(0).getSite_id() : "");

		return "security/form";
	}

	@RequestMapping(value = "/security/save", method = RequestMethod.POST)
	public String save(@ModelAttribute("security") Security security, BindingResult br, Model model, HttpServletRequest request) throws Exception {
		try {
			//
			String insert_user_id = SessionUtils.getUserFromSession(request).getUser_id();
			if (security.getMode().equals(CRUDKeys.INSERT)) {
				security.setInsert_user_id(insert_user_id);
				security_service.saveSecurity(security);
			} else if (security.getMode().equals(CRUDKeys.UPDATE)) {
				security_service.updateSecurity(security);
			}
			//
			setDefault(model, insert_user_id);
			model.addAttribute("result", new Result(Result.SUCCESS, "보안정책을 저장하였습니다."));
			return "security/index";

		} catch (Exception ex) {
			model.addAttribute("result", new Result(Result.SUCCESS, "보안정책을 저장할 수 없습니다 : 메세지=[" + ex.getMessage() + "]"));
			log.warn(ex);
			return "security/form";
		}
	}

	@RequestMapping(value = "/security/delete/{security_id:.+}", method = RequestMethod.POST)
	public @ResponseBody JSONResult delete(@PathVariable String security_id, Model model, HttpServletRequest request) throws Exception {
		//
		SecurityService security_service = new SecurityService();
		security_service.deleteSecurity(security_id);
		return new JSONResult(Result.SUCCESS, "보안정책을 삭제하였습니다.");
	}
	
	

}