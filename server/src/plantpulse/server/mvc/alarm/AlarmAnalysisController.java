package plantpulse.server.mvc.alarm;

import java.util.ArrayList;
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
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.ResponseBody;

import plantpulse.cep.engine.config.ConfigurationManager;
import plantpulse.cep.service.AlarmService;
import plantpulse.cep.service.SiteService;
import plantpulse.cep.service.TagService;
import plantpulse.domain.Site;
import plantpulse.domain.Tag;
import plantpulse.server.mvc.util.DateUtils;
import plantpulse.server.mvc.util.SessionUtils;

@Controller
public class AlarmAnalysisController {

	private AlarmService alarm_service = new AlarmService();

	@Autowired
	private SiteService site_service;

	@Autowired
	private TagService tag_service;

	private void search(Model model, String insert_user_id, String alarm_date_from, String alarm_date_to, String priority, String description) throws Exception {
		Map<String, Object> params = new HashMap<String, Object>();

		params.put("alarm_date_from", alarm_date_from);
		params.put("alarm_date_to", alarm_date_to);
		params.put("insert_user_id", insert_user_id);
		params.put("priority", priority);
		params.put("description", description);

		String limit = ConfigurationManager.getInstance().getApplication_properties().getProperty("alarm.list.page.analysis.limit");
		List<Map<String, Object>> alarm_list = alarm_service.getAlarmPage(params, limit, false);
		long alarm_count = alarm_service.getAlarmCount(params);

		model.addAttribute("alarm_count_info", alarm_service.getAlarmCountByPriority(insert_user_id, "INFO"));
		model.addAttribute("alarm_count_warn", alarm_service.getAlarmCountByPriority(insert_user_id, "WARN"));
		model.addAttribute("alarm_count_error", alarm_service.getAlarmCountByPriority(insert_user_id, "ERROR"));

		model.addAttribute("alarm_count", alarm_count);
		model.addAttribute("alarm_list", alarm_list);
	}

	@RequestMapping(value = "/alarm/analysis/index", method = RequestMethod.GET)
	public String index(Model model, HttpServletRequest request) throws Exception {
		String alarm_date_from = DateUtils.currDateByMinus7Day(); //기본 일주일
		String alarm_date_to = DateUtils.currDate();
		if (StringUtils.isEmpty(alarm_date_from) || StringUtils.isEmpty(alarm_date_to)) {
			alarm_date_from = DateUtils.currDateBy00();
			alarm_date_to = DateUtils.currDate();
		}
		;

		model.addAttribute("alarm_date_from", alarm_date_from);
		model.addAttribute("alarm_date_to", alarm_date_to);
		model.addAttribute("insert_user_id", SessionUtils.getUserFromSession(request).getUser_id());
		//
		List<Site> site_list = site_service.selectSites();
		model.addAttribute("site_list", site_list);
		model.addAttribute("selected_site_id", (site_list != null && site_list.size() > 0) ? site_list.get(0).getSite_id() : "");

		return "alarm/analysis/index";
	}

	@RequestMapping(value = "/alarm/analysis/analysis", method = RequestMethod.POST)
	public String analysis(Model model, HttpServletRequest request) throws Exception {
		String alarm_date_from = request.getParameter("alarm_date_from");
		String alarm_date_to = request.getParameter("alarm_date_to");
		if (StringUtils.isEmpty(alarm_date_from) || StringUtils.isEmpty(alarm_date_to)) {
			alarm_date_from = DateUtils.fmtDate((DateUtils.addMonths(new Date(DateUtils.getToDate().getTime()), -1)).getTime());
			alarm_date_to = DateUtils.currDate();
		}
		String priority = request.getParameter("priority");
		String description = request.getParameter("description");
		model.addAttribute("alarm_date_from", alarm_date_from);
		model.addAttribute("alarm_date_to", alarm_date_to);
		model.addAttribute("priority", priority);
		model.addAttribute("description", description);
		model.addAttribute("insert_user_id", SessionUtils.getUserFromSession(request).getUser_id());
		//
		search(model, SessionUtils.getUserFromSession(request).getUser_id(), alarm_date_from, alarm_date_to, priority, description);
		return "alarm/analysis/index";
	}

	@RequestMapping(value = "/alarm/analysis/chart/index", method = RequestMethod.GET)
	public String cindex(Model model, HttpServletRequest request) throws Exception {
		//
		return "alarm/analysis/chart/index";
	}

	@RequestMapping(value = "/alarm/analysis/chart/matrix", method = RequestMethod.GET)
	public String matrix(Model model, HttpServletRequest request) throws Exception {
		//
		return "alarm/analysis/chart/matrix";
	}

	@RequestMapping(value = "/alarm/analysis/chart/eventdrops", method = RequestMethod.GET)
	public String eventdrops(Model model, HttpServletRequest request) throws Exception {
		//
		String alarm_date_from = request.getParameter("alarm_date_from");
		String alarm_date_to = request.getParameter("alarm_date_to");
		String domain = request.getParameter("domain");
		String tag_ids = request.getParameter("tag_ids");
		String[] tag_id_array = tag_ids.split(",");

		model.addAttribute("alarm_date_from", alarm_date_from);
		model.addAttribute("alarm_date_to", alarm_date_to);
		model.addAttribute("tag_id_array", tag_id_array);
		model.addAttribute("domain", domain);

		JSONArray array = new JSONArray();
		List<Tag> tag_list = new ArrayList<Tag>();
		for (int i = 0; i < tag_id_array.length; i++) {
			Tag tag = tag_service.selectTagInfo(tag_id_array[i]);
			JSONObject json = new JSONObject();
			json.put("name", tag.getTag_name());
			JSONArray timestamp_array = alarm_service.getAlarmEventDrops(alarm_date_from, alarm_date_to, tag_id_array[i]);
			json.put("timestamp_array", timestamp_array);
			array.add(json);
		}
		model.addAttribute("tag_list", tag_list);
		model.addAttribute("eventdrops_data", array.toString());

		return "alarm/analysis/chart/eventdrops";
	}

	@RequestMapping(value = "/alarm/analysis/chart/heatmap", method = RequestMethod.GET)
	public String heatmap(Model model, HttpServletRequest request) throws Exception {
		String alarm_date_from = request.getParameter("alarm_date_from");
		String alarm_date_to = request.getParameter("alarm_date_to");
		String domain = request.getParameter("domain");
		String tag_ids = request.getParameter("tag_ids");
		String[] tag_id_array = tag_ids.split(",");
		//
		model.addAttribute("alarm_date_from", alarm_date_from);
		model.addAttribute("alarm_date_to", alarm_date_to);
		model.addAttribute("tag_id_array", tag_id_array);
		model.addAttribute("domain", domain);

		List<Tag> tag_list = new ArrayList<Tag>();
		for (int i = 0; i < tag_id_array.length; i++) {
			tag_list.add(tag_service.selectTagInfo(tag_id_array[i]));
		}
		model.addAttribute("tag_list", tag_list);
		//
		return "alarm/analysis/chart/heatmap";
	};

	@RequestMapping(value = "/alarm/analysis/chart/heatmap/data/{tag_id}/{alarm_date_from}/{alarm_date_to}", method = RequestMethod.GET)
	public @ResponseBody String heatmapData(@PathVariable String tag_id, @PathVariable String alarm_date_from, @PathVariable String alarm_date_to, Model model, HttpServletRequest request)
			throws Exception {
		JSONObject json = alarm_service.getAlarmTimestampListByTagId(alarm_date_from, alarm_date_to, tag_id);
		return json.toString();
	}

}
