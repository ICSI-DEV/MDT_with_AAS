package plantpulse.server.mvc.dashboard;

import java.util.ArrayList;
import java.util.List;

import javax.servlet.http.HttpServletRequest;

import org.apache.commons.lang.StringUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.springframework.stereotype.Controller;
import org.springframework.ui.Model;
import org.springframework.validation.BindingResult;
import org.springframework.web.bind.annotation.ModelAttribute;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.ResponseBody;

import plantpulse.cep.domain.Dashboard;
import plantpulse.cep.service.DashboardService;
import plantpulse.cep.service.GraphService;
import plantpulse.cep.service.SecurityService;
import plantpulse.cep.service.UserService;
import plantpulse.domain.Security;
import plantpulse.server.mvc.util.SessionUtils;

@Controller
public class DashboardController {

	private static final Log log = LogFactory.getLog(DashboardController.class);

	private DashboardService dashboard_service = new DashboardService();
	private GraphService graph_service = new GraphService();
	private SecurityService security_service = new SecurityService();
	private UserService user_service = new UserService();

	@RequestMapping(value = "/dashboard", method = RequestMethod.GET)
	public String main(Model model, HttpServletRequest request) throws Exception {
		return index(model, request);
	}

	@RequestMapping(value = "/dashboard/index", method = RequestMethod.GET)
	public String index(Model model, HttpServletRequest request) throws Exception {
		List<Dashboard> dashboard_list = dashboard_service.getDashboardListByUserId(SessionUtils.getUserFromSession(request).getUser_id());
		List<Dashboard> share_list = new ArrayList<Dashboard>();
		//
		if (dashboard_list == null || dashboard_list.size() < 1) { // 신규 사용자이거나
																	// 등록된 대시보드가
																	// 없을 경우 "내
																	// 대시보드 생성"
			Dashboard dashboard = new Dashboard();
			dashboard.setDashboard_id("DASHBOARD_" + System.currentTimeMillis());
			dashboard.setDashboard_title("기본 대시보드");
			dashboard.setDashboard_desc("");
			dashboard.setDashboard_json("[]");
			dashboard.setInsert_user_id(SessionUtils.getUserFromSession(request).getUser_id());
			dashboard_service.insertDashboard(dashboard);
			dashboard_list = dashboard_service.getDashboardListByUserId(SessionUtils.getUserFromSession(request).getUser_id());
		}
		
		share_list = dashboard_service.getDashboardListBySecurityId(user_service.getUser(SessionUtils.getUserFromSession(request).getUser_id()).getSecurity_id());
		
		//
		Dashboard default_dashboard = null;
		if(share_list.size() == 0){
			default_dashboard = dashboard_list.get(0);
		}else{
			default_dashboard = share_list.get(0); //공유된 스카다가 있으면 먼저 표시
		}
		model.addAttribute("graph_list", graph_service.getGraphListByDashboardId(default_dashboard.getDashboard_id()));
		model.addAttribute("dashboard_list", dashboard_list);
		model.addAttribute("share_list", share_list);
		//
		//
		model.addAttribute("dashboard", dashboard_service.getDashboardById(default_dashboard.getDashboard_id()));
		return "dashboard/index";
	}

	@RequestMapping(value = "/dashboard/view/{dashboard_id}", method = RequestMethod.GET)
	public String view(@PathVariable("dashboard_id") String dashboard_id, Model model, HttpServletRequest request) throws Exception {
		List<Dashboard> dashboard_list = dashboard_service.getDashboardListByUserId(SessionUtils.getUserFromSession(request).getUser_id());
		List<Dashboard> share_list = dashboard_service.getDashboardListBySecurityId(user_service.getUser(SessionUtils.getUserFromSession(request).getUser_id()).getSecurity_id());
		model.addAttribute("dashboard_list", dashboard_list);
		model.addAttribute("share_list", share_list);
		//
		model.addAttribute("graph_list", graph_service.getGraphListByDashboardId(dashboard_id));
		//
		model.addAttribute("dashboard", dashboard_service.getDashboardById(dashboard_id));
		return "dashboard/index";
	}

	@RequestMapping(value = "/dashboard/screen/{dashboard_id}", method = RequestMethod.GET)
	public String screen(@PathVariable("dashboard_id") String dashboard_id, Model model, HttpServletRequest request) throws Exception {
		List<Dashboard> dashboard_list = dashboard_service.getDashboardListByUserId(SessionUtils.getUserFromSession(request).getUser_id());
		model.addAttribute("dashboard_list", dashboard_list);
		model.addAttribute("graph_list", graph_service.getGraphListByDashboardId(dashboard_id));
		//
		model.addAttribute("dashboard", dashboard_service.getDashboardById(dashboard_id));
		return "dashboard/screen";
	}

	@RequestMapping(value = "/dashboard/link/{dashboard_id}", method = RequestMethod.GET)
	public String link(@PathVariable("dashboard_id") String id, Model model, HttpServletRequest request) throws Exception {
		return "dashboard/link";
	}

	@RequestMapping(value = "/dashboard/add", method = RequestMethod.GET)
	public String add(Model model) throws Exception {
		Dashboard dashboard = new Dashboard();
		model.addAttribute("dashboard", dashboard);
		//
		List<Security> security_list = security_service.getSecurityList();
		model.addAttribute("security_list", security_list);
		//
		return "dashboard/form";
	}

	@RequestMapping(value = "/dashboard/edit/{dashboard_id}", method = RequestMethod.GET)
	public String edit(@ModelAttribute("dashboard") Dashboard dashboard, Model model) throws Exception {
		Dashboard target_dashboard = dashboard_service.getDashboardById(dashboard.getDashboard_id());
		model.addAttribute("dashboard", target_dashboard);
		//
		List<Security> security_list = security_service.getSecurityList();
		model.addAttribute("security_list", security_list);
		//
		return "dashboard/form";
	}

	@RequestMapping(value = "/dashboard/save", method = RequestMethod.POST)
	public @ResponseBody Dashboard save(@ModelAttribute("dashboard") Dashboard dashboard, BindingResult br, Model model, HttpServletRequest request) throws Exception {
		if (StringUtils.isEmpty(dashboard.getDashboard_id())) { //
			dashboard.setDashboard_id("DASHBOARD_" + System.currentTimeMillis());
			dashboard.setDashboard_json("[]");
			dashboard.setInsert_user_id(SessionUtils.getUserFromSession(request).getUser_id());
			model.addAttribute("dashboard", dashboard);
			//
			dashboard_service.insertDashboard(dashboard);
		} else { //
			model.addAttribute("dashboard", dashboard);
			dashboard_service.updateDashboard(dashboard);
		}
		return dashboard;
	}

	@RequestMapping(value = "/dashboard/serialize", method = RequestMethod.GET)
	public @ResponseBody String serialize(@RequestParam("id") String dashboard_id, @RequestParam("json") String dashboard_json, Model model, HttpServletRequest request) throws Exception {
		dashboard_service.updateDashboardJson(dashboard_id, dashboard_json);
		return dashboard_json;
	}

	@RequestMapping(value = "/dashboard/delete/{dashboard_id}", method = RequestMethod.POST)
	public String delete(@ModelAttribute("dashboard") Dashboard dashboard, Model model, HttpServletRequest request) throws Exception {
		dashboard_service.deleteDashboard(dashboard.getDashboard_id());
		return "dashboard/index";
	}

}
