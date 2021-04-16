package plantpulse.server.mvc.tag;

import java.time.LocalDate;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import javax.servlet.http.HttpServletRequest;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.json.JSONObject;
import org.realdisplay.framework.util.StringUtils;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.HttpStatus;
import org.springframework.stereotype.Controller;
import org.springframework.ui.Model;
import org.springframework.web.bind.annotation.ModelAttribute;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.ResponseBody;
import org.springframework.web.bind.annotation.ResponseStatus;

import com.google.common.base.Preconditions;

import plantpulse.cep.engine.config.ConfigurationManager;
import plantpulse.cep.service.AlarmConfigService;
import plantpulse.cep.service.AlarmService;
import plantpulse.cep.service.ExcelService;
import plantpulse.cep.service.OPCService;
import plantpulse.cep.service.SiteService;
import plantpulse.cep.service.StorageService;
import plantpulse.cep.service.TagService;
import plantpulse.cep.service.UserService;
import plantpulse.cep.service.client.StorageClient;
import plantpulse.domain.AlarmConfig;
import plantpulse.domain.OPC;
import plantpulse.domain.Site;
import plantpulse.domain.Tag;
import plantpulse.domain.TagDataTableObject;
import plantpulse.domain.User;
import plantpulse.server.mvc.JSONResult;
import plantpulse.server.mvc.LimitConstants;
import plantpulse.server.mvc.file.DownloadKeys;
import plantpulse.server.mvc.util.Constants;
import plantpulse.server.mvc.util.DateUtils;
import plantpulse.server.mvc.util.SessionUtils;

@Controller
@RequestMapping(value = "/tag")
public class TagController {

	private static final Log log = LogFactory.getLog(TagController.class);

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
	
	@Autowired
	private StorageClient storage_client;

	private AlarmConfigService alarm_config_service = new AlarmConfigService();
	
	private UserService user_service = new UserService();
	

	@RequestMapping(value = "/index")
	public String index(Model model) {
		Tag tag = new Tag();
		model.addAttribute("tag", tag);
		model.addAttribute("total_count", tag_service.selectTagCountTotal(tag));
		return "tag/index";
	}

	@RequestMapping(value = "/search")
	public @ResponseBody List<Map<String, Object>> search(@ModelAttribute Tag tag, Model model) {
		List<Map<String, Object>> result = tag_service.searchTagConfigList(tag, LimitConstants.TAG_SEARCH_LIMIT_COUNT);
		return result;
	}

	@RequestMapping(value = "/page")
	public @ResponseBody List<Map<String, Object>> page(@ModelAttribute Tag tag, Model model) {
		List<Map<String, Object>> result = tag_service.searchTagConfigList(tag, LimitConstants.TAG_SEARCH_LIMIT_COUNT);
		return result;
	}

	@RequestMapping(value = "/export", method = RequestMethod.POST)
	public @ResponseBody JSONResult export(@ModelAttribute Tag tag, Model model, HttpServletRequest request) {
		try {

			//
			String file_dir = System.getProperty("java.io.tmpdir") + "/plantpulse";
			String file_name = "TAG_LIST_" + plantpulse.server.mvc.util.DateUtils.fmtSuffix() + ".xls";
			String file_path = file_dir + "/" + file_name;
			List<Map<String, Object>> result = tag_service.searchTagConfigList(tag, -1);
			//
			String[] headerLabelArray = new String[] { "SITE_ID", "SITE_NAME", "OPC_ID", "OPC_NAME", "TAG_ID", "TAG_NAME", "DATA_TYPE", "DATA_COUNT", "INTERVAL", "IMPORTANCE", "UNIT", "DISPLAY_FORMAT", "MIN_VALUE", "MAX_VALUE", "TRIP_HI", "HI", "HI_HI", "TRIP_LO", "LO",
					"LO_LO", "LINKED_ASSET_ID", "ALIAS_NAME", "DESCRIPTION" };
			String[] valueKeyArray = new String[]    { "SITE_ID", "SITE_NAME", "OPC_ID", "OPC_NAME", "TAG_ID", "TAG_NAME", "JAVA_TYPE", "DATA_COUNT", "INTERVAL", "IMPORTANCE", "UNIT", "DISPLAY_FORMAT", "MIN_VALUE", "MAX_VALUE", "TRIP_HI", "HI", "HI_HI", "TRIP_LO", "LO",
					"LO_LO", "LINKED_ASSET_ID", "ALIAS_NAME", "DESCRIPTION" };
			excel_service.writeExcel(headerLabelArray, valueKeyArray, result, file_path);
			//
			request.getSession().setAttribute(DownloadKeys.FILE_PATH, file_path);
			request.getSession().setAttribute(DownloadKeys.SAVE_NAME, file_name);
			request.getSession().setAttribute(DownloadKeys.CONTENT_TYPE, "unknown");

			return new JSONResult(JSONResult.SUCCESS, "엑셀을 정상적으로 출력하였습니다.");
		} catch (Exception e) {
			log.error("TagController.export() Exception : " + e.getMessage(), e);
			return new JSONResult(JSONResult.ERROR, "엑셀을 출력할 수 없습니다. : 메세지 = [" + e.getMessage() + "]");
		}
	}

	@RequestMapping(value = "/form/{tag_id}", method = RequestMethod.GET)
	public String form(Model model, @PathVariable String tag_id) throws Exception {
		Tag tag = tag_service.selectTagInfo(tag_id);
		AlarmConfig alarm_config = alarm_config_service.getAlarmConfig("ALARM_CONFIG_" + tag.getTag_id());
		if (alarm_config == null) {
			alarm_config = new AlarmConfig();
		}
		model.addAttribute("tag", tag);
		model.addAttribute("alram", alarm_config);
		//
		List<User> recieve_others_list = new ArrayList<User>();
		if (alarm_config != null) {
			if (StringUtils.isNotEmpty(alarm_config.getRecieve_others())) {
				String[] user_id_array = alarm_config.getRecieve_others().split(",");
				for (int i = 0; i < user_id_array.length; i++) {
					recieve_others_list.add(user_service.getUser(user_id_array[i]));
				}
			}
		}
		;

		model.addAttribute("recieve_others_list", recieve_others_list);

		return "tag/form";
	}

	@RequestMapping(value = "/view/{tag_id}", method = RequestMethod.GET)
	public String view(Model model, @PathVariable String tag_id, HttpServletRequest request) throws Exception {
		
		// 기준 시간
		String date_from = DateUtils.currDateBy00();
		String date_to = DateUtils.currDateBy24();
		model.addAttribute("date_from", date_from);
		model.addAttribute("date_to", date_to);

		// 사이트 정보
		Tag tag = tag_service.selectTagInfo(tag_id);
		model.addAttribute("tag", tag);
		model.addAttribute("tagCnt", tag_service.selectTagPointCount(tag));
		model.addAttribute("targetCnt", 200_000_000L);

		OPC opc = opc_service.selectOpcInfo(tag.getOpc_id());
		model.addAttribute("opc", opc);

		Site site = site_service.selectSiteInfo(tag.getSite_id());
		model.addAttribute("site", site);

		// 오늘 데이터 목록
		List<Map<String, Object>> tag_data_list = storage_service.list(tag, date_from + ":00", date_to + ":59", 100, null);
		model.addAttribute("tag_data_list", tag_data_list);

		// 오늘 알람 건수
		LocalDate today = LocalDate.now();
		JSONObject alarm_cnt = storage_client.forSelect().selectAlarmCountByDate(tag, today.getYear(), today.getMonthValue(), today.getDayOfMonth());
		model.addAttribute("today_alarm_count", alarm_cnt.getLong("alarm_count"));
		model.addAttribute("today_alarm_count_by_info", alarm_cnt.getLong("alarm_count_by_info"));
		model.addAttribute("today_alarm_count_by_warn", alarm_cnt.getLong("alarm_count_by_warn"));
		model.addAttribute("today_alarm_count_by_error", alarm_cnt.getLong("alarm_count_by_error"));
		
		// 알람 트랜드
		Map<String, Object> params = new HashMap<String, Object>();
		String insert_user_id = SessionUtils.getUserFromSession(request).getUser_id();
		params.put("alarm_date_from", date_from);
		params.put("alarm_date_to", date_to);
		params.put("tag_id", tag_id);
		params.put("insert_user_id", insert_user_id);
		
		// 알람 카운트
		String limit = ConfigurationManager.getInstance().getApplication_properties().getProperty("alarm.list.page.index.limit");
		AlarmService alarm_service = new AlarmService();
		List<Map<String, Object>> alarm_list = alarm_service.getAlarmPage(params, limit, false);
		model.addAttribute("alarm_list", alarm_list);
		model.addAttribute("alarm_count", alarm_service.getTagAlarmAggregation(insert_user_id, tag_id));
		
		// 알람 설정 리스트
		List<AlarmConfig> alarm_config_list = alarm_config_service.getAlarmConfigListByTagId(tag_id);
		model.addAttribute("alarm_config_list", alarm_config_list);
		
		return "tag/view";
	};
	
	@RequestMapping(value = "/add_vtag", method = RequestMethod.GET)
	public String add_vtag(Model model, HttpServletRequest request) throws Exception {
		Tag tag = new Tag();
		model.addAttribute("tag", tag);
		return "tag/add_vtag";
	}
	
	@RequestMapping(value = "/save_vtag", method = RequestMethod.POST)
	public @ResponseBody JSONResult save_vtag(@ModelAttribute Tag tag, HttpServletRequest request) {
		try {
			//
			tag_service.insertVituralTag(tag);
			//
			return new JSONResult(JSONResult.SUCCESS, "가상 태그를 생성하였습니다.");
		} catch (Exception e) {
			log.error("TagController.save_vtag() Exception : " + e.getMessage(), e);
			return new JSONResult(JSONResult.ERROR, "가상 태그를 생성할 수 없습니다. : 메세지 = [" + e.getMessage() + "]");
		}
	}

	@RequestMapping(value = "/getOpcCurrTagList")
	public @ResponseBody TagDataTableObject getOpcCurrTagList(@ModelAttribute Tag tag) {
		Preconditions.checkNotNull(tag, "OPC is null.");
		TagDataTableObject table = new TagDataTableObject();
		table.setAaData(tag_service.selectTagList(tag, Constants.TYPE_OPC));
		return table;
	}

	@RequestMapping(value = "/mappingTag", method = RequestMethod.POST)
	public @ResponseBody JSONResult mappingTag(@ModelAttribute Tag tag) {
		Preconditions.checkNotNull(tag, "Point is null.");

		try {
			tag_service.updateTagAsset(tag.getTag_id(), tag.getLinked_asset_id());
			return new JSONResult(JSONResult.SUCCESS, "태그를 에셋에 추가하였습니다. ");
		} catch (Exception e) {
			log.error("TagController.moveAsset() Exception : " + e.getMessage(), e);
			return new JSONResult(JSONResult.ERROR, "Add To Asset Failed.");
		}
	}

	@RequestMapping(value = "/deleteTagMappingInfo", method = RequestMethod.POST)
	public @ResponseBody JSONResult deleteTagMappingInfo(@ModelAttribute Tag tag) {
		//
		Preconditions.checkNotNull(tag, "Point is null.");
		try {
			tag_service.deleteTagMappingInfo(tag);
			return new JSONResult(JSONResult.SUCCESS, "태그 맵핑 정보를 삭제하였습니다.");
		} catch (Exception e) {
			log.error("TagController.deleteTagMappingInfo() Exception : " + e.getMessage(), e);
			return new JSONResult(JSONResult.ERROR, "태그 맵핑 정보를 삭제할 수 없습니다. : 메세지 = [" + e.getMessage() + "]");
		}
	}

	@RequestMapping(value = "/deleteAllTagMappingInfo", method = RequestMethod.POST)
	public @ResponseBody JSONResult deleteAllTagMappingInfo(@RequestParam(value = "selectedAssetIDs[]") String[] selectedAssetIDs) {
		//
		Preconditions.checkNotNull(selectedAssetIDs, "Point is null.");
		try {
			tag_service.deleteAllTagMappingInfo(selectedAssetIDs);
			return new JSONResult(JSONResult.SUCCESS, "선택한 태그 맵핑 정보를 모두 삭제하였습니다.");
		} catch (Exception e) {
			log.error("TagController.deleteTagMappingInfo() Exception : " + e.getMessage(), e);
			return new JSONResult(JSONResult.ERROR, "태그 맵핑 정보를 삭제할 수 없습니다. : 메세지 = [" + e.getMessage() + "]");
		}
	}

	// Point 물리적 삭제
	@RequestMapping(value = "/deleteTag", method = RequestMethod.POST)
	public @ResponseBody JSONResult deleteTag(@ModelAttribute Tag tag) {
		//
		Preconditions.checkNotNull(tag, "Point is null.");
		try {
			tag_service.deleteTag(tag);
			return new JSONResult(JSONResult.SUCCESS, "태그를 삭제하였습니다.");
		} catch (Exception e) {
			log.error("TagController.deleteTag() Exception : " + e.getMessage(), e);
			return new JSONResult(JSONResult.ERROR, "태그를 삭제할 수 없습니다. : 메세지 = [" + e.getMessage() + "]");
		}
	}

	// Point 물리적 삭제 (멀티 삭제)
	@RequestMapping(value = "/deleteTags", method = RequestMethod.POST)
	public @ResponseBody JSONResult deleteTags(@RequestBody List<String> list) {
		//
		try {
			tag_service.deleteTagsByIdList(list);
			return new JSONResult(JSONResult.SUCCESS, "태그를 삭제하였습니다.");
		} catch (Exception e) {
			log.error("Delete tags failed. : " + e.getMessage(), e);
			return new JSONResult(JSONResult.ERROR, "태그를 삭제할 수 없습니다. : 메세지 = [" + e.getMessage() + "]");
		}
	}

	@RequestMapping(value = "/updateTag", method = RequestMethod.POST)
	@ResponseStatus(HttpStatus.OK)
	public @ResponseBody JSONResult updateTag(@ModelAttribute Tag tag, HttpServletRequest request) {
		try {
			
			//
			tag_service.updateTag(tag);
			tag_service.deployAlarm(tag);
			tag_service.deployCalculation(tag);
			tag_service.deployAggregation(tag);
			tag_service.deployForecast(tag);
			
			//
			return new JSONResult(JSONResult.SUCCESS, "태그를 수정하였습니다.");
		} catch (Exception e) {
			log.error("TagController.updateTag() Exception : " + e.getMessage(), e);
			return new JSONResult(JSONResult.ERROR, "태그를 수정할 수 없습니다. : 메세지 = [" + e.getMessage() + "]");
		}
	}

	@RequestMapping(value = "/list")
	public @ResponseBody List<Map<String, String>> list(@ModelAttribute Tag tag, Model model) throws Exception {
		return tag_service.selectTagAllForSelect();
	}

	@RequestMapping(value = "/map")
	public @ResponseBody Map<String, String> map(@ModelAttribute Tag tag, Model model) throws Exception {
		return tag_service.selectTagMapForSelect();
	}

	@RequestMapping(value = "/get/{tag_id}", method = RequestMethod.GET)
	public @ResponseBody String get(Model model, @PathVariable String tag_id, HttpServletRequest request) throws Exception {

		// 사이트 정보
		Tag tag = tag_service.selectTagInfo(tag_id);
		//
		return JSONObject.fromObject(tag).toString();
	}

}