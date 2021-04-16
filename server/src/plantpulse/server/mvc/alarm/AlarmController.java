package plantpulse.server.mvc.alarm;

import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import javax.servlet.http.HttpServletRequest;

import org.apache.commons.lang.StringUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.json.JSONArray;
import org.json.JSONObject;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Controller;
import org.springframework.ui.Model;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.ResponseBody;

import plantpulse.cep.domain.Alarm;
import plantpulse.cep.engine.config.ConfigurationManager;
import plantpulse.cep.service.AlarmConfigService;
import plantpulse.cep.service.AlarmService;
import plantpulse.cep.service.ExcelService;
import plantpulse.cep.service.OPCService;
import plantpulse.cep.service.SiteService;
import plantpulse.cep.service.StorageService;
import plantpulse.cep.service.TagService;
import plantpulse.cep.service.support.tag.TagLocationFactory;
import plantpulse.domain.AlarmConfig;
import plantpulse.domain.OPC;
import plantpulse.domain.Site;
import plantpulse.domain.Tag;
import plantpulse.server.mvc.JSONResult;
import plantpulse.server.mvc.Result;
import plantpulse.server.mvc.util.DateUtils;
import plantpulse.server.mvc.util.DateUtils2;
import plantpulse.server.mvc.util.SessionUtils;

@Controller
public class AlarmController {

	private static final Log log = LogFactory.getLog(AlarmController.class);

	private AlarmService alarm_service = new AlarmService();

	private AlarmConfigService alarm_config_service = new AlarmConfigService();

	@Autowired
	private TagService tag_service;

	@Autowired
	private ExcelService excel_service;

	@Autowired
	private SiteService site_service;

	@Autowired
	private OPCService opc_service;

	@Autowired
	private StorageService storage_service;

	private void search(Model model, String insert_user_id, String alarm_date_from, String alarm_date_to) throws Exception {
		Map<String, Object> params = new HashMap<String, Object>();
		params.put("alarm_date_from", alarm_date_from);
		params.put("alarm_date_to", alarm_date_to);
		params.put("insert_user_id", insert_user_id);

		long start1 = System.currentTimeMillis();
		String limit = ConfigurationManager.getInstance().getApplication_properties().getProperty("alarm.list.page.index.limit");
		List<Map<String, Object>> alarm_list = alarm_service.getAlarmPage(params, limit, false);
		for(int i=0; alarm_list != null && i < alarm_list.size(); i++) { //태그 로케이션 추가
			Map alarm = ((Map<String, Object>)alarm_list.get(i));
			String tag_id = (String)alarm.get("tag_id");
			alarm_list.get(i).put("tag_location", TagLocationFactory.getInstance().getTagLocationWithoutTagName(tag_id));
		};
		long alarm_count = alarm_service.getAlarmCount(params);

		log.debug("getAlarmPage = " + (System.currentTimeMillis() - start1) + "ms");

		//
		long start2 = System.currentTimeMillis();
		List<Map<String, Object>> alarm_rank = alarm_service.getAlarmRank(params);
		List<Map<String, Object>> alarm_rank_by_opc = alarm_service.getAlarmRankByOPC(params);
		List<Map<String, Object>> alarm_rank_by_asset = alarm_service.getAlarmRankByAsset(params);
		
		long start3 = System.currentTimeMillis();
		model.addAttribute("alarm_count_info", alarm_service.getAlarmCountByPriority(alarm_date_from, alarm_date_to, insert_user_id, "INFO"));
		model.addAttribute("alarm_count_warn", alarm_service.getAlarmCountByPriority(alarm_date_from, alarm_date_to, insert_user_id, "WARN"));
		model.addAttribute("alarm_count_error", alarm_service.getAlarmCountByPriority(alarm_date_from, alarm_date_to, insert_user_id, "ERROR"));
		log.debug("getAlarmCountByPriority = " + (System.currentTimeMillis() - start3) + "ms");

		long start4 = System.currentTimeMillis();
		long alarm_total_count = alarm_service.getAlarmCountByInsertUserId(insert_user_id);
		log.debug("getAlarmCountByInsertUserId = " + (System.currentTimeMillis() - start4) + "ms");


		long start5 = System.currentTimeMillis();
		List<Map<String, Object>> alarm_count_trend_list = alarm_service.getAlarmCountByHour(params);
		log.debug("getAlarmCountByHour = " + (System.currentTimeMillis() - start5) + "ms");
		
		model.addAttribute("alarm_total_count", alarm_total_count);
		model.addAttribute("alarm_count", alarm_count);
		model.addAttribute("alarm_list", alarm_list);
		model.addAttribute("alarm_rank", alarm_rank);
		model.addAttribute("alarm_rank_by_opc", alarm_rank_by_opc);
		model.addAttribute("alarm_rank_by_asset", alarm_rank_by_asset);
		model.addAttribute("alarm_count_trend_list", alarm_count_trend_list);
	}

	@RequestMapping(value = "/alarm/index", method = RequestMethod.GET)
	public String index(Model model, HttpServletRequest request) throws Exception {
		//
		
		String alarm_date_from = DateUtils.currDateBy00();
		String alarm_date_to = DateUtils.currDateBy24();
		model.addAttribute("alarm_date_from", alarm_date_from);
		model.addAttribute("alarm_date_to", alarm_date_to);
		model.addAttribute("insert_user_id", SessionUtils.getUserFromSession(request).getUser_id());
		//
		search(model, SessionUtils.getUserFromSession(request).getUser_id(), alarm_date_from, alarm_date_to);
		//
		
		return "alarm/index";
	}

	@RequestMapping(value = "/alarm/search/{alarm_date_from}/{alarm_date_to}", method = RequestMethod.GET)
	public String retrieve(@PathVariable String alarm_date_from, @PathVariable String alarm_date_to, Model model, HttpServletRequest request) throws Exception {
		search(model, SessionUtils.getUserFromSession(request).getUser_id(), alarm_date_from, alarm_date_to);
		return "alarm/index";
	}

	@RequestMapping(value = "/alarm/flag/{alarm_date_from}/{alarm_date_to}", method = RequestMethod.GET)
	public @ResponseBody String flag(@PathVariable String alarm_date_from, @PathVariable String alarm_date_to, Model model, HttpServletRequest request) throws Exception {
		Map<String, Object> params = new HashMap<String, Object>();
		params.put("alarm_date_from", alarm_date_from);
		params.put("alarm_date_to", alarm_date_to);
		params.put("insert_user_id", SessionUtils.getUserFromSession(request).getUser_id());

		String limit = "10000"; // TODO 스토리지의 알람 플래그는 10,000개로 제한
		List<Map<String, Object>> alarm_list = alarm_service.getAlarmFlag(params, limit);

		JSONArray array = new JSONArray();
		for (int i = 0; alarm_list != null && i < alarm_list.size(); i++) {
			Map<String, Object> data = alarm_list.get(i);
			JSONObject obj = new JSONObject();
			obj.put("x", (Long) data.get("alarm_timestamp"));
			String priority = (String) data.get("priority");
			String priority_txt = "-";
			if (priority.equals("INFO")) {
				priority_txt = "정보";
			}
			if (priority.equals("WARN")) {
				priority_txt = "경고";
			}
			if (priority.equals("ERROR")) {
				priority_txt = "심각";
			}
			obj.put("title", "<div id='" + data.get("alarm_seq") + "' onmouseover='showFlagText(" + data.get("alarm_seq") + ");'><span class='alarm-timeline-" + data.get("priority") + "' >"
					+ data.get("tag_name") + "</div>");
			obj.put("text", "<div>[" + priority_txt + "] " + data.get("description") + "</div>");
			array.add(obj);
		}
		//Collections.reverse(array);
		return array.toString();

	};
	
	
	@RequestMapping(value = "/alarm/flagByTagIds", method = RequestMethod.GET)
	public @ResponseBody String flagByTagIds( Model model, HttpServletRequest request) throws Exception {
		
		
		Map<String, Object> params = new HashMap<String, Object>();
		params.put("alarm_date_from", request.getParameter("alarm_date_from"));
		params.put("alarm_date_to",   request.getParameter("alarm_date_to"));
		params.put("tag_ids", request.getParameter("tag_ids"));
		
		params.put("insert_user_id", SessionUtils.getUserFromSession(request).getUser_id());
		

		String limit = "10000"; // TODO 스토리지의 알람 플래그는 10,000개로 제한
		List<Map<String, Object>> alarm_list = alarm_service.getAlarmFlag(params, limit);

		JSONArray array = new JSONArray();
		for (int i = 0; alarm_list != null && i < alarm_list.size(); i++) {
			Map<String, Object> data = alarm_list.get(i);
			JSONObject obj = new JSONObject();
			obj.put("x", (Long) data.get("alarm_timestamp"));
			String priority = (String) data.get("priority");
			String priority_txt = "-";
			if (priority.equals("INFO")) {
				priority_txt = "정보";
			}
			if (priority.equals("WARN")) {
				priority_txt = "경고";
			}
			if (priority.equals("ERROR")) {
				priority_txt = "심각";
			}
			obj.put("title", "<div id='" + data.get("alarm_seq") + "' onmouseover='showFlagText(" + data.get("alarm_seq") + ");'>"
					+ " <span class='alarm-timeline-" + data.get("priority") + "' >"
					+ "<i class='far fa-bell' aria-hidden='true'></i>" + "</div>");
			obj.put("text", "<div>[" + priority_txt + "] " + data.get("description") + "</div>");
			array.add(obj);
		}
		//Collections.reverse(array);
		return array.toString();

	};

	@RequestMapping(value = "/alarm/flag/{alarm_date_from}/{alarm_date_to}/{tag_id}", method = RequestMethod.GET)
	public @ResponseBody String flagByTagId(@PathVariable String alarm_date_from, @PathVariable String alarm_date_to, @PathVariable String tag_id, Model model, HttpServletRequest request)
			throws Exception {
		Map<String, Object> params = new HashMap<String, Object>();
		params.put("alarm_date_from", alarm_date_from);
		params.put("alarm_date_to", alarm_date_to);
		params.put("tag_id", tag_id);
		params.put("insert_user_id", SessionUtils.getUserFromSession(request).getUser_id());

		String limit = "10000"; // TODO 스토리지의 알람 플래그는 1,0000개로 제한
		List<Map<String, Object>> alarm_list = alarm_service.getAlarmPage(params, limit, false);

		JSONArray array = new JSONArray();
		for (int i = 0; alarm_list != null && i < alarm_list.size(); i++) {
			Map<String, Object> data = alarm_list.get(i);
			JSONObject obj = new JSONObject();
			obj.put("x", (Long) data.get("alarm_timestamp"));
			String priority = (String) data.get("priority");
			String priority_txt = "-";
			if (priority.equals("INFO")) {
				priority_txt = "정보";
			}
			if (priority.equals("WARN")) {
				priority_txt = "경고";
			}
			if (priority.equals("ERROR")) {
				priority_txt = "심각";
			}
			obj.put("title", "<div id='" + data.get("alarm_seq") + "' onmouseover='showFlagText(" + data.get("alarm_seq") + ");'><span class='alarm-timeline-" + data.get("priority")
					+ "' style='border-radius:0px;'>" + "<i class='far fa-bell' aria-hidden='true'></i>" + "</div>");
			obj.put("text", "<div>[" + priority_txt + "] " + data.get("description") + "</div>");
			array.add(obj);
		}
		return array.toString();

	}

	@RequestMapping(value = "/alarm/get/{alarm_seq}", method = RequestMethod.GET, headers = "Accept=*/*", produces = "application/json")
	public @ResponseBody JSONResult get(@PathVariable long alarm_seq, Model model, HttpServletRequest request) throws Exception {
		JSONResult json = new JSONResult();
		Alarm alarm = alarm_service.getAlarm(alarm_seq, SessionUtils.getUserFromSession(request).getUser_id());
		AlarmConfig alarm_config = alarm_config_service.getAlarmConfig(alarm.getAlarm_config_id());
		Tag tag = tag_service.selectTag(alarm_config.getTag_id());
		//
		json.getData().put("alarm", alarm); //
		json.getData().put("alarm_config", alarm_config);
		json.getData().put("tag", tag);
		//
		json.getData().put("alarm_date_string", DateUtils.fmtISO(alarm.getAlarm_date().getTime()));

		json.setStatus("SUCCESS");
		//
		return json;
	}

	@RequestMapping(value = "/alarm/view/{alarm_seq}", method = RequestMethod.GET)
	public String view(Model model, @PathVariable String alarm_seq, HttpServletRequest request) throws Exception {

		try{
		
		//
		alarm_service.updateReadAlarm(new Long(alarm_seq), SessionUtils.getUserFromSession(request).getUser_id());
		
		//
		Alarm alarm = alarm_service.getAlarm(new Long(alarm_seq), SessionUtils.getUserFromSession(request).getUser_id());
		AlarmConfig alarm_config = alarm_config_service.getAlarmConfig(alarm.getAlarm_config_id());
		if(alarm_config == null){
			//TODO 알람 저장시 태그ID도 저장을 해야함.
			throw new Exception("알람[" + alarm_seq + "] 밴드 설정이 삭제되었습니다.");
		};
		
		Tag tag = tag_service.selectTag(alarm_config.getTag_id());

		String tag_id = tag.getTag_id();

		// 기준 알람 시간
		Date alarm_date = alarm.getAlarm_date();

		String date_from = DateUtils.fmtDate(DateUtils.addHours(alarm_date, -1).getTime()); // 한시간
																							// 전후
																							// 데이터
		String date_to = DateUtils.fmtDate(DateUtils.addHours(alarm_date, 1).getTime());

		//
		model.addAttribute("alarm_date", DateUtils.fmtDate(alarm_date.getTime()));
		model.addAttribute("alarm_timestamp", alarm_date.getTime());
		model.addAttribute("date_from", date_from);
		model.addAttribute("date_to", date_to);

		model.addAttribute("alarm", alarm);

		model.addAttribute("alarm_config", alarm_config);

		// 사이트 정보
		model.addAttribute("tag", tag);

		OPC opc = opc_service.selectOpcInfo(tag.getOpc_id());
		model.addAttribute("opc", opc);

		Site site = site_service.selectSiteInfo(tag.getSite_id());
		model.addAttribute("site", site);

		// 데이터 목록
		List<Map<String, Object>> tag_data_list = storage_service.list(tag, date_from + ":00", date_to + ":59", 100, null);
		model.addAttribute("tag_data_list", tag_data_list);

		// 알람 트랜드
		Map<String, Object> params = new HashMap<String, Object>();
		params.put("alarm_date_from", date_from);
		params.put("alarm_date_to", date_to);
		params.put("tag_id", tag_id);
		params.put("insert_user_id", SessionUtils.getUserFromSession(request).getUser_id());
		//
		String limit = ConfigurationManager.getInstance().getApplication_properties().getProperty("alarm.list.page.index.limit");
		AlarmService alarm_service = new AlarmService();
		List<Map<String, Object>> alarm_list = alarm_service.getAlarmPage(params, limit, false);
		model.addAttribute("alarm_list", alarm_list);
		
		// 알람 통계
		model.addAttribute("tag_statistics", alarm_service.getTagAlarmStatistics(SessionUtils.getUserFromSession(request).getUser_id(), tag_id, null));
		//
		}catch(Exception ex){
			log.error("Alarm view error : " + ex.getMessage(), ex);
			throw ex;
		}
		return "alarm/view";
	}

	@RequestMapping(value = "/alarm/statistics", method = RequestMethod.POST)
	public @ResponseBody JSONResult statistics(@RequestBody Map<String, Object> paramMap, HttpServletRequest request) throws Exception {
		JSONResult json = new JSONResult();
		try {
			String type = (String) paramMap.get("type");
			StringBuffer conditionQuery = new StringBuffer();
			if (StringUtils.isNotEmpty(type)) {
				String first = "";
				String last = "";
				if (type.equals("month")) {
					first = DateUtils2.getFirstDateOfMonth(DateUtils2.getNow(DateUtils2.DATE_PATTERN_DASH)) + " " + DateUtils2.START_TIME;
					last = DateUtils2.getLastDateOfMonth(DateUtils2.getNow(DateUtils2.DATE_PATTERN_DASH)) + " " + DateUtils2.END_TIME;
				} else if (type.equals("week")) {
					first = DateUtils.getFirstDateOfWeek() + " " + DateUtils.START_TIME;
					last = DateUtils.getLastDateOfWeek() + " " + DateUtils.END_TIME;
				} else if (type.equals("yesterday")) {
					first = DateUtils2.getFirstDateOfPrevDay();
					last = DateUtils2.getLastDateOfPrevDay();
				} else {
					first = DateUtils2.getNow(DateUtils2.DATE_PATTERN_DASH) + " " + DateUtils2.START_TIME;
					last = DateUtils2.getNow(DateUtils2.DATE_PATTERN_DASH) + " " + DateUtils2.END_TIME;
				}
				conditionQuery.append(" AND ALARM_DATE BETWEEN CAST('" + first + "' AS TIMESTAMP)  AND CAST('" + last + "' AS TIMESTAMP)");
			}
			json.setStatus("SUCCESS");
			json.getData().put("tag_statistics", alarm_service.getTagAlarmStatistics(SessionUtils.getUserFromSession(request).getUser_id(), (String) paramMap.get("tag_id"), conditionQuery.toString()));
		} catch (Exception e) {
			json.setStatus("FAILURE");
			log.error("Tag alarm statistics data getting failed. [MESSAGE]=" + e.getMessage(), e);
		}
		return json;
	}
	
	@RequestMapping(value = "/alarm/count", method = RequestMethod.GET, headers = "Accept=*/*", produces = "application/json")
	public @ResponseBody JSONResult count(Model model, HttpServletRequest request) throws Exception {
		JSONResult json = new JSONResult();
		int cnt = alarm_service.getAlarmCountByUnread(SessionUtils.getUserFromSession(request).getUser_id());
		json.setStatus("SUCCESS");
		json.getData().put("count", cnt); // count set
		//
		return json;
	}

	@RequestMapping(value = "/alarm/readall", method = RequestMethod.GET, headers = "Accept=*/*", produces = "application/json")
	public @ResponseBody JSONResult readall(Model model, HttpServletRequest request) throws Exception {
		JSONResult json = new JSONResult();
		alarm_service.updateReadAllAlarm(SessionUtils.getUserFromSession(request).getUser_id());
		json.setStatus("SUCCESS");
		//
		return json;
	}

	@RequestMapping(value = "/alarm/delete/{alarm_seq}", method = RequestMethod.POST)
	public @ResponseBody JSONResult delete(@PathVariable Long alarm_seq, Model model, HttpServletRequest request) throws Exception {

		alarm_service.deleteAlarm(alarm_seq, SessionUtils.getUserFromSession(request).getUser_id());
		//
		return new JSONResult(Result.SUCCESS, "알람을 삭제하였습니다.");
	}

	@RequestMapping(value = "/alarm/deleteAll", method = RequestMethod.POST)
	public @ResponseBody JSONResult deleteAll(Model model, HttpServletRequest request) throws Exception {
		String insert_user_id = SessionUtils.getUserFromSession(request).getUser_id();
		alarm_service.deleteAllAlarm(insert_user_id);
		//
		return new JSONResult(Result.SUCCESS, "전체 알람을 삭제하였습니다.");
	}


	
	
	@RequestMapping(value = "/alarm/my", method = RequestMethod.GET)
	public String my(Model model, HttpServletRequest request) throws Exception {
		//
		return "alarm/my";
	}
	
	@RequestMapping(value = "/alarm/my/count", method = RequestMethod.GET)
	public @ResponseBody String myalamrmcount(Model model, HttpServletRequest request) throws Exception {
		
		String alarm_date_from = DateUtils.currDateBy00();
		String alarm_date_to = DateUtils.currDateBy24();
		String insert_user_id = SessionUtils.getUserFromSession(request).getUser_id();
		
		Map<String, Object> params = new HashMap<String, Object>();
		params.put("alarm_date_from", alarm_date_from);
		params.put("alarm_date_to", alarm_date_to);
		params.put("insert_user_id", insert_user_id);
		
		//
		JSONObject result = new JSONObject();
		result.put("alarm_count_info", alarm_service.getAlarmCountByPriority(alarm_date_from, alarm_date_to, insert_user_id, "INFO"));
		result.put("alarm_count_warn", alarm_service.getAlarmCountByPriority(alarm_date_from, alarm_date_to, insert_user_id, "WARN"));
		result.put("alarm_count_error", alarm_service.getAlarmCountByPriority(alarm_date_from, alarm_date_to, insert_user_id, "ERROR"));
		//
		return result.toString();
	}


}
