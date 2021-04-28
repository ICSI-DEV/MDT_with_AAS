package plantpulse.server.mvc.conrol;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;

import javax.servlet.http.HttpServletRequest;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.json.JSONArray;
import org.json.JSONObject;
import org.realdisplay.framework.util.StringUtils;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Controller;
import org.springframework.ui.Model;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.ResponseBody;

import plantpulse.cep.engine.utils.CSVUtils;
import plantpulse.cep.service.ControlService;
import plantpulse.cep.service.TagService;
import plantpulse.domain.Tag;
import plantpulse.server.mvc.JSONResult;
import plantpulse.server.mvc.file.DownloadKeys;
import plantpulse.server.mvc.util.DateUtils;


@Controller
public class ControlController {
	
	
	private static final Log log = LogFactory.getLog(ControlController.class);
	
	private static final int EXPORT_LIMIT = 100000000; //TODO 출력 천만건 제안

	@Autowired
	private TagService tag_service;
	
	@Autowired
	private ControlService control_service;
	
	
	@RequestMapping(value = "/control/index", method = RequestMethod.GET)
	public String index(Model model, HttpServletRequest request) throws Exception {
		return "control/index";
	}
	
	
	@RequestMapping(value = "/control/history", method = RequestMethod.GET)
	public String history(Model model, HttpServletRequest request) throws Exception {
		
		String search_date_from = DateUtils.currDateBy00();
		String search_date_to = DateUtils.currDateBy24();
		model.addAttribute("search_date_from", search_date_from);
		model.addAttribute("search_date_to", search_date_to);
		
		return "control/history";
	}
	//
	
	@RequestMapping(value = "/control/search", method = RequestMethod.GET)
	public @ResponseBody String search(Model model, HttpServletRequest request) throws Exception {
		
		Map<String,String> params = new HashMap<String, String>();
		params.put("search_date_from", request.getParameter("search_date_from"));
		params.put("search_date_to",   request.getParameter("search_date_to"));
		params.put("result",           request.getParameter("result"));
		params.put("wrtie_value",      request.getParameter("wrtie_value"));
		
		params.put("site_name",  request.getParameter("site_name"));
		params.put("opc_name",   request.getParameter("opc_name"));
		params.put("asset_name", request.getParameter("asset_name"));
		params.put("tag_name",   request.getParameter("tag_name"));
		
		params.put("tag_id", request.getParameter("tag_id"));
		
		//
		JSONArray  data = control_service.searchControlList(params);
		//
		JSONObject result =  new JSONObject();
		result.put("data", data);
		return result.toString();
	}
	
	@RequestMapping(value = "/control/export", method = RequestMethod.POST)
	public @ResponseBody JSONResult export(Model model, HttpServletRequest request) throws Exception {

		try {

			long start = System.currentTimeMillis();
			
			String tag_id   = request.getParameter("tag_id");
			String from     = request.getParameter("search_date_from") + ":00";
			String to       = request.getParameter("search_date_to")   + ":59";
			
			Tag tag = tag_service.selectTag(tag_id);
			
			JSONArray  array = null; // control_service.selectTagDataList(tag, from, to, EXPORT_LIMIT, condition);
			
			long end = System.currentTimeMillis() - start;
			
			log.info("Data search time = [" + (end/1000) + "]sec");
			
			//
			String file_dir = System.getProperty("java.io.tmpdir") + "/plantpulse";
			String file_name = "TAG_DATA_" + plantpulse.server.mvc.util.DateUtils.fmtSuffix() + ".csv";
			String file_path = StringUtils.cleanPath(file_dir + "/" + file_name);

			//
			String[] header_array = new String[]{
					"tag_id",
					"tag_name",
					"timestamp",
					"value",
					"unit",
					"java_type",
					"quality",
					"error_code",
					"attribute"
			};
			//
			ArrayList<Map<String, Object>> list = new ArrayList<Map<String, Object>>();
			for (int i = 0; i < array.size(); i++) {
				JSONObject json = array.getJSONObject(i);
				json.put("tag_id",   tag.getTag_id());
				json.put("tag_name", tag.getTag_name());
				json.put("timestamp", DateUtils.fmtISO(json.getJSONObject("timestamp").getLong("time")));
				json.put("unit",      tag.getUnit());
				json.put("java_type", tag.getJava_type());
				list.add(json);
			};
			CSVUtils.writeForData(header_array, list, file_path);

			// 파일생성
			request.getSession().setAttribute(DownloadKeys.FILE_PATH, file_path);
			request.getSession().setAttribute(DownloadKeys.SAVE_NAME, file_name);
			request.getSession().setAttribute(DownloadKeys.CONTENT_TYPE, "unknown");

			return new JSONResult(JSONResult.SUCCESS, "CSV을 정상적으로 출력하였습니다.");
		} catch (Exception e) {
			log.error("Data export failed : " + e.getMessage(), e);
			return new JSONResult(JSONResult.ERROR, "CSV을 출력할 수 없습니다. : 메세지 = [" + e.getMessage() + "]");
		}
	}
	
	

	@RequestMapping(value = "/control/set", method = RequestMethod.POST)
	public @ResponseBody String control(Model model, HttpServletRequest request) {
		JSONObject result = new JSONObject();
		try {
			ControlService service = new ControlService();
			String tag_id = request.getParameter("tag_id");
			String value = request.getParameter("value");
            //
			service.setValue(tag_id, value);
			//
			result.put("status", "SUCCESS");
			return result.toString();
		} catch (Exception e) {
			result.put("status",  "ERROR");
			result.put("message", e.getMessage());
		}
		return result.toString();
	}
	
	
	@RequestMapping(value = "/control/batch", method = RequestMethod.POST)
	public @ResponseBody String batch(Model model, HttpServletRequest request) {
		JSONObject result = new JSONObject();
		try {
			//
			ControlService service = new ControlService();
			String[] tag_ids = request.getParameterValues("tag_id");
			String[] values  = request.getParameterValues("value");
            //
			for(int i=0; i <tag_ids.length; i ++){
				service.setValue(tag_ids[i], values[i]);
			};
			result.put("status", "SUCCESS");
			return result.toString();
		} catch (Exception e) {
			result.put("status",  "ERROR");
			result.put("message", e.getMessage());
		}
		return result.toString();	
	}

	@RequestMapping(value = "/control/test", method = RequestMethod.GET)
	public @ResponseBody String test(Model model, HttpServletRequest request) throws Exception {
		JSONObject result = new JSONObject();
		ControlService service = new ControlService();
		service.setValue("TAG_42259", "11");
		result.put("STATUS", "SUCCESS");
		return result.toString();
	}

}
