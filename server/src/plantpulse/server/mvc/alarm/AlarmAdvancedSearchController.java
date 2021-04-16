package plantpulse.server.mvc.alarm;

import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import javax.servlet.http.HttpServletRequest;

import org.apache.commons.lang.StringUtils;
import org.json.JSONArray;
import org.json.JSONObject;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Controller;
import org.springframework.ui.Model;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.ResponseBody;

import plantpulse.cep.engine.config.ConfigurationManager;
import plantpulse.cep.service.AlarmService;
import plantpulse.cep.service.SiteService;
import plantpulse.cep.service.TagService;
import plantpulse.domain.Site;
import plantpulse.server.mvc.util.DateUtils;
import plantpulse.server.mvc.util.SessionUtils;

@Controller
public class AlarmAdvancedSearchController {

	private AlarmService alarm_service = new AlarmService();

	@Autowired
	private SiteService site_service;

	@Autowired
	private TagService tag_service;

	private void search(Model model, String insert_user_id, String alarm_date_from, String alarm_date_to, String priority, String description, String tag_ids) throws Exception {
		Map<String, Object> params = new HashMap<String, Object>();

		params.put("alarm_date_from", alarm_date_from);
		params.put("alarm_date_to", alarm_date_to);
		params.put("insert_user_id", insert_user_id);
		params.put("priority", priority);
		params.put("description", description);
		params.put("tag_ids", tag_ids);

		String limit = ConfigurationManager.getInstance().getApplication_properties().getProperty("alarm.list.page.advsearch.limit");
		List<Map<String, Object>> alarm_list = alarm_service.getAlarmPage(params, limit, false);
		long alarm_count = alarm_service.getAlarmCount(params);

		model.addAttribute("alarm_count_info", alarm_service.getAlarmCountByPriority(params, "INFO"));
		model.addAttribute("alarm_count_warn", alarm_service.getAlarmCountByPriority(params, "WARN"));
		model.addAttribute("alarm_count_error", alarm_service.getAlarmCountByPriority(params, "ERROR"));

		model.addAttribute("alarm_count", alarm_count);
		model.addAttribute("alarm_list", alarm_list);

		//
		List<Map<String, Object>> alarm_count_trend_list = alarm_service.getAlarmCountPriorityByDate(params);
		model.addAttribute("alarm_count_trend_list", JSONArray.fromObject(alarm_count_trend_list).toString());

		//
		List<Site> site_list = site_service.selectSites();
		model.addAttribute("site_list", site_list);
		model.addAttribute("selected_site_id", (site_list != null && site_list.size() > 0) ? site_list.get(0).getSite_id() : "");
	}

	@RequestMapping(value = "/alarm/advsearch/index", method = RequestMethod.GET)
	public String index(Model model, HttpServletRequest request) throws Exception {
		String alarm_date_from = DateUtils.currDateByMinus7Day();
		String alarm_date_to = DateUtils.currDateBy24();
		if (StringUtils.isEmpty(alarm_date_from) || StringUtils.isEmpty(alarm_date_to)) {
			alarm_date_from = DateUtils.currDateByMinus7Day();
			alarm_date_to = DateUtils.currDate();
		}
		String priority = request.getParameter("priority");
		String description = request.getParameter("description");
		String tag_ids = request.getParameter("tag_ids");

		model.addAttribute("alarm_date_from", alarm_date_from);
		model.addAttribute("alarm_date_to", alarm_date_to);
		model.addAttribute("priority", priority);
		model.addAttribute("description", description);
		model.addAttribute("tag_ids", null);
		model.addAttribute("insert_user_id", SessionUtils.getUserFromSession(request).getUser_id());

		//
		List<Site> site_list = site_service.selectSites();
		model.addAttribute("site_list", site_list);
		model.addAttribute("selected_site_id", (site_list != null && site_list.size() > 0) ? site_list.get(0).getSite_id() : "");
		//
		search(model, SessionUtils.getUserFromSession(request).getUser_id(), alarm_date_from, alarm_date_to, priority, description, null);
		return "alarm/advsearch/index";
	}

	@RequestMapping(value = "/alarm/advsearch/advsearch", method = RequestMethod.POST)
	public String advsearch(Model model, HttpServletRequest request) throws Exception {

		String alarm_date_from = request.getParameter("alarm_date_from");
		String alarm_date_to = request.getParameter("alarm_date_to");
		if (StringUtils.isEmpty(alarm_date_from) || StringUtils.isEmpty(alarm_date_to)) {
			alarm_date_from = DateUtils.fmtDate((DateUtils.addMonths(new Date(DateUtils.getToDate().getTime()), -1)).getTime());
			alarm_date_to = DateUtils.currDate();
		}
		String priority = request.getParameter("priority");
		String description = request.getParameter("description");
		String tag_ids = request.getParameter("tag_ids");

		model.addAttribute("alarm_date_from", alarm_date_from);
		model.addAttribute("alarm_date_to", alarm_date_to);
		model.addAttribute("priority", priority);
		model.addAttribute("description", description);
		model.addAttribute("tag_ids", tag_ids);
		model.addAttribute("insert_user_id", SessionUtils.getUserFromSession(request).getUser_id());

		//
		search(model, SessionUtils.getUserFromSession(request).getUser_id(), alarm_date_from, alarm_date_to, priority, description, tag_ids);
		return "alarm/advsearch/index";
	}

	@RequestMapping(value = "/alarm/advsearch/advsearchjson", method = RequestMethod.POST)
	public @ResponseBody String advsearchjson(Model model, HttpServletRequest request) throws Exception {

		String alarm_date_from = request.getParameter("alarm_date_from");
		String alarm_date_to = request.getParameter("alarm_date_to");
		if (StringUtils.isEmpty(alarm_date_from) || StringUtils.isEmpty(alarm_date_to)) {
			alarm_date_from = DateUtils.fmtDate((DateUtils.addMonths(new Date(DateUtils.getToDate().getTime()), -1)).getTime());
			alarm_date_to = DateUtils.currDate();
		}
		String priority = request.getParameter("priority");
		String description = request.getParameter("description");
		String tag_ids = request.getParameter("tag_ids");

		Map<String, Object> params = new HashMap<String, Object>();

		params.put("alarm_date_from", alarm_date_from);
		params.put("alarm_date_to", alarm_date_to);
		params.put("insert_user_id", SessionUtils.getUserFromSession(request).getUser_id());
		params.put("priority", priority);
		params.put("description", description);
		params.put("tag_ids", tag_ids);

		String limit = ConfigurationManager.getInstance().getApplication_properties().getProperty("alarm.list.page.advsearch.limit");

		//
		JSONObject json = new JSONObject();

		json.put("alarm_count", alarm_service.getAlarmCount(params));
		json.put("alarm_list", alarm_service.getAlarmPage(params, limit, false));

		json.put("alarm_count_info", alarm_service.getAlarmCountByPriority(params, "INFO"));
		json.put("alarm_count_warn", alarm_service.getAlarmCountByPriority(params, "WARN"));
		json.put("alarm_count_error", alarm_service.getAlarmCountByPriority(params, "ERROR"));

		json.put("alarm_count_trend_list", alarm_service.getAlarmCountPriorityByDate(params));
		//

		return json.toString();
	}

}
