package plantpulse.server.mvc.data;

import java.util.ArrayList;
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
import plantpulse.cep.service.DataService;
import plantpulse.cep.service.TagService;
import plantpulse.domain.Tag;
import plantpulse.server.mvc.JSONResult;
import plantpulse.server.mvc.file.DownloadKeys;
import plantpulse.server.mvc.util.DateUtils;

@Controller
public class DataController {
	
	private static final Log log = LogFactory.getLog(DataController.class);
	
	private static final int EXPORT_LIMIT = 100000000; //TODO 출력 천만건 제안
	
	@Autowired
	private TagService tag_service;
	
	@Autowired
	private DataService data_service;
	
	@RequestMapping(value = "/data/index", method = RequestMethod.GET)
	public String index(Model model, HttpServletRequest request) throws Exception {
		String search_date_from = DateUtils.currDateBy00();
		String search_date_to = DateUtils.currDateBy24();
		model.addAttribute("search_date_from", search_date_from);
		model.addAttribute("search_date_to", search_date_to);
		
		String tag_id   = request.getParameter("tag_id");
		String from     = request.getParameter("from");
		String to       = request.getParameter("to");
		if(StringUtils.isNotEmpty(tag_id)){
			Tag tag = tag_service.selectTag(tag_id);
			model.addAttribute("tag_id", tag_id);
			model.addAttribute("tag_name", tag.getTag_name());
			model.addAttribute("search_date_from", from);
			model.addAttribute("search_date_to", to);
			if(StringUtils.isEmpty(from) || StringUtils.isEmpty(to) ){
				model.addAttribute("search_date_from", search_date_from);
				model.addAttribute("search_date_to", search_date_to);
			}
		}
		return "data/index";
	}
	
	@RequestMapping(value = "/data/search", method = RequestMethod.GET)
	public @ResponseBody String search(Model model, HttpServletRequest request) throws Exception {
		String tag_id   = request.getParameter("tag_id");
		String from     = request.getParameter("search_date_from") + ":00";
		String to       = request.getParameter("search_date_to")   + ":59";
		String condition       = request.getParameter("condition");
		int  limit      = Integer.parseInt(request.getParameter("limit"));
		Tag tag = tag_service.selectTag(tag_id);
		JSONObject t = JSONObject.fromObject(tag);
		JSONArray  data = data_service.selectTagDataList(tag, from, to, limit, condition);
		JSONObject result =  new JSONObject();
		result.put("tag", t);
		result.put("data", data);
		return result.toString();
	}
	
	@RequestMapping(value = "/data/export", method = RequestMethod.POST)
	public @ResponseBody JSONResult export(Model model, HttpServletRequest request) throws Exception {

		try {

			long start = System.currentTimeMillis();
			
			String tag_id   = request.getParameter("tag_id");
			String from     = request.getParameter("search_date_from") + ":00";
			String to       = request.getParameter("search_date_to")   + ":59";
			String condition   = request.getParameter("condition");
			
			Tag tag = tag_service.selectTag(tag_id);
			
			JSONArray  array = data_service.selectTagDataList(tag, from, to, EXPORT_LIMIT, condition);
			
			long end = System.currentTimeMillis() - start;
			
			log.info("Data search time = [" + (end/1000) + "]sec");
			
			//
			String file_dir = System.getProperty("java.io.tmpdir") + "/plantpulse";
			String file_name = "TAG_DATA_" + plantpulse.server.mvc.util.DateUtils.fmtSuffix() + ".csv";
			String file_path = StringUtils.cleanPath(file_dir + "/" + file_name);

			//
			String[] header_array = new String[]{
					"site_id",
					"opc_id",
					"tag_id",
					"tag_name",
					"timestamp",
					"timestamp_iso",
					"value",
					"unit",
					"java_type",
					"type",
					"quality",
					"error_code",
					"attribute"
			};
			//
			ArrayList<Map<String, Object>> list = new ArrayList<Map<String, Object>>();
			for (int i = 0; i < array.size(); i++) {
				JSONObject json = array.getJSONObject(i);
				json.put("site_id",  tag.getSite_id());
				json.put("opc_id",   tag.getOpc_id());
				json.put("tag_id",   tag.getTag_id());
				json.put("tag_name", tag.getTag_name());
				long timestamp = json.getJSONObject("timestamp").getLong("time");
				json.put("timestamp",     timestamp);
				json.put("timestamp_iso", DateUtils.fmtISO(timestamp));
				json.put("unit",          tag.getUnit());
				json.put("type",          json.getString("type").toLowerCase());
				json.put("java_type",     tag.getJava_type());
				
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

}
