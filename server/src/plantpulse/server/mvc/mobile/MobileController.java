package plantpulse.server.mvc.mobile;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import javax.servlet.http.HttpServletRequest;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.springframework.stereotype.Controller;
import org.springframework.ui.Model;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;

import plantpulse.cep.domain.Dashboard;
import plantpulse.cep.domain.SCADA;
import plantpulse.cep.engine.config.ConfigurationManager;
import plantpulse.cep.service.AlarmService;
import plantpulse.cep.service.DashboardService;
import plantpulse.cep.service.GraphService;
import plantpulse.cep.service.SACADAService;
import plantpulse.server.mvc.util.DateUtils;
import plantpulse.server.mvc.util.SessionUtils;

@Controller
public class MobileController {

	private static final Log log = LogFactory.getLog(MobileController.class);

	private DashboardService dashboard_service = new DashboardService();
	private SACADAService scada_service = new SACADAService();
	private GraphService graph_service = new GraphService();

	private AlarmService alarm_service = new AlarmService();

	@RequestMapping(value = "/m", method = RequestMethod.GET)
	public String index(Model model, HttpServletRequest request) throws Exception {
		return "m/login";
	}

	@RequestMapping(value = "/m/login", method = RequestMethod.GET)
	public String login(Model model, HttpServletRequest request) throws Exception {
		return "m/login";
	}

	@RequestMapping(value = "/m/dashboard", method = RequestMethod.GET)
	public String dashboard(Model model, HttpServletRequest request) throws Exception {
		List<Dashboard> dashboard_list = dashboard_service.getDashboardListByUserId(SessionUtils.getUserFromSession(request).getUser_id());
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

		//
		Dashboard default_dashboard = dashboard_list.get(0);
		model.addAttribute("graph_list", graph_service.getGraphListByDashboardId(default_dashboard.getDashboard_id()));
		model.addAttribute("dashboard_list", dashboard_list);
		//
		model.addAttribute("dashboard", dashboard_service.getDashboardById(default_dashboard.getDashboard_id()));
		return "m/dashboard";
	}

	@RequestMapping(value = "/m/dashboard/{dashboard_id}", method = RequestMethod.GET)
	public String dashboardView(@PathVariable("dashboard_id") String dashboard_id, Model model, HttpServletRequest request) throws Exception {
		List<Dashboard> dashboard_list = dashboard_service.getDashboardListByUserId(SessionUtils.getUserFromSession(request).getUser_id());
		model.addAttribute("dashboard_list", dashboard_list);
		model.addAttribute("graph_list", graph_service.getGraphListByDashboardId(dashboard_id));
		//
		model.addAttribute("dashboard", dashboard_service.getDashboardById(dashboard_id));
		return "m/dashboard";
	}
	
	@RequestMapping(value = "/m/scada", method = RequestMethod.GET)
	public String scada(Model model, HttpServletRequest request) throws Exception {
		List<SCADA> scada_list = scada_service.getSCADAListByUserId(SessionUtils.getUserFromSession(request).getUser_id());
		//
		if (scada_list == null || scada_list.size() < 1) { // 
			SCADA scada = new SCADA();
			scada.setScada_id("SCADA_" + System.currentTimeMillis());
			scada.setScada_title("기본  스카다");
			scada.setScada_desc("");
			scada.setInsert_user_id(SessionUtils.getUserFromSession(request).getUser_id());
			scada_service.insertSCADA(scada);
			scada_list = scada_service.getSCADAListByUserId(SessionUtils.getUserFromSession(request).getUser_id());
		}

		//
		SCADA default_scada = scada_list.get(0);
		model.addAttribute("scada_list", scada_list);
		model.addAttribute("scada", scada_service.getSCADAById(default_scada.getScada_id()));
		return "m/scada";
	}

	@RequestMapping(value = "/m/scada/{scada_id}", method = RequestMethod.GET)
	public String scadaView(@PathVariable("scada_id") String scada_id, Model model, HttpServletRequest request) throws Exception {
		List<SCADA> scada_list = scada_service.getSCADAListByUserId(SessionUtils.getUserFromSession(request).getUser_id());
		model.addAttribute("scada_list", scada_list);
		//
		model.addAttribute("scada", scada_service.getSCADAById(scada_id));
		return "m/scada";
	}

	@RequestMapping(value = "/m/alarm", method = RequestMethod.GET)
	public String alarm(Model model, HttpServletRequest request) throws Exception {

		String alarm_date_from = DateUtils.currDateBy00();
		String alarm_date_to = DateUtils.currDateBy24();
		model.addAttribute("alarm_date_from", alarm_date_from);
		model.addAttribute("alarm_date_to", alarm_date_to);
		model.addAttribute("insert_user_id", SessionUtils.getUserFromSession(request).getUser_id());
		//
		search(model, SessionUtils.getUserFromSession(request).getUser_id(), alarm_date_from, alarm_date_to);

		return "m/alarm";
	}

	private void search(Model model, String insert_user_id, String alarm_date_from, String alarm_date_to) throws Exception {
		Map<String, Object> params = new HashMap<String, Object>();
		params.put("alarm_date_from", alarm_date_from);
		params.put("alarm_date_to", alarm_date_to);
		params.put("insert_user_id", insert_user_id);

		String limit = ConfigurationManager.getInstance().getApplication_properties().getProperty("alarm.list.page.index.limit");
		List<Map<String, Object>> alarm_list = alarm_service.getAlarmPage(params, limit, true);
		long alarm_count = alarm_service.getAlarmCount(params);

		//
		List<Map<String, Object>> alarm_rank = alarm_service.getAlarmRank(params);

		model.addAttribute("alarm_count_info", alarm_service.getAlarmCountByPriority(alarm_date_from, alarm_date_to, insert_user_id, "INFO"));
		model.addAttribute("alarm_count_warn", alarm_service.getAlarmCountByPriority(alarm_date_from, alarm_date_to, insert_user_id, "WARN"));
		model.addAttribute("alarm_count_error", alarm_service.getAlarmCountByPriority(alarm_date_from, alarm_date_to, insert_user_id, "ERROR"));

		long alarm_total_count = alarm_service.getAlarmCountByInsertUserId(insert_user_id);

		model.addAttribute("alarm_total_count", alarm_total_count);
		model.addAttribute("alarm_count", alarm_count);
		model.addAttribute("alarm_list", alarm_list);
		model.addAttribute("alarm_rank", alarm_rank);

		List<Map<String, Object>> alarm_count_trend_list = alarm_service.getAlarmCountByHour(params);
		model.addAttribute("alarm_count_trend_list", alarm_count_trend_list);
	}
	
	
	
	@RequestMapping(value = "/m/system", method = RequestMethod.GET)
	public String system(Model model, HttpServletRequest request) throws Exception {

		String alarm_date_from = DateUtils.currDateBy00();
		String alarm_date_to = DateUtils.currDateBy24();
		model.addAttribute("alarm_date_from", alarm_date_from);
		model.addAttribute("alarm_date_to", alarm_date_to);
		model.addAttribute("insert_user_id", SessionUtils.getUserFromSession(request).getUser_id());
		//
		search(model, SessionUtils.getUserFromSession(request).getUser_id(), alarm_date_from, alarm_date_to);

		return "m/system";
	}


}
