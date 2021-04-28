package plantpulse.server.mvc.log;

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

import plantpulse.cep.engine.logging.LoggingAppender;
import plantpulse.cep.engine.utils.CSVUtils;
import plantpulse.cep.service.LogService;
import plantpulse.server.mvc.JSONResult;
import plantpulse.server.mvc.file.DownloadKeys;
import plantpulse.server.mvc.util.DateUtils;

@Controller
public class LogController {
	
	private static final Log log = LogFactory.getLog(LogController.class);
	
	private static final int EXPORT_LIMIT = 1_000_000; 
	
	@Autowired
	private LogService log_service;
	
	@RequestMapping(value = "/log/index", method = RequestMethod.GET)
	public String index(Model model, HttpServletRequest request) throws Exception {
		String search_date_from = DateUtils.currDateBy00();
		String search_date_to = DateUtils.currDateBy24();
		model.addAttribute("search_date_from", search_date_from);
		model.addAttribute("search_date_to", search_date_to);
		return "log/index";
	}
	
	@RequestMapping(value = "/log/search", method = RequestMethod.GET)
	public @ResponseBody String search(Model model, HttpServletRequest request) throws Exception {
		String from     = request.getParameter("search_date_from") + ":00";
		String to       = request.getParameter("search_date_to")   + ":59";
		
		String app_name = request.getParameter("app_name");
		String level    = request.getParameter("level");
		String message  = request.getParameter("message");
		int  limit      = Integer.parseInt(request.getParameter("limit"));
		JSONArray array = log_service.selectLogList(from, to, app_name,  level, message, limit);
		return array.toString();
	};
	
	
	@RequestMapping(value = "/log/export", method = RequestMethod.POST)
	public @ResponseBody JSONResult export(Model model, HttpServletRequest request) throws Exception {

		try {

			long start = System.currentTimeMillis();
			
			String from     = request.getParameter("search_date_from") + ":00";
			String to       = request.getParameter("search_date_to")   + ":59";
			
			String app_name = request.getParameter("app_name");
			String level    = request.getParameter("level");
			String message  = request.getParameter("message");
			
			JSONArray array = log_service.selectLogList(from, to, app_name,  level, message, EXPORT_LIMIT);
			
			long end = System.currentTimeMillis() - start;
			
			log.info("Log search time = [" + (end/1000) + "]sec");
			
			//
			String file_dir  = System.getProperty("java.io.tmpdir") + "/plantpulse";
			String file_name = "LOG_" + plantpulse.server.mvc.util.DateUtils.fmtSuffix() + ".csv";
			String file_path = StringUtils.cleanPath(file_dir + "/" + file_name);

			//
			String[] header_array = new String[]{
					LoggingAppender.APP_NAME,
					LoggingAppender.TIMESTAMP + "_fmd",
					LoggingAppender.LEVEL,
					LoggingAppender.CLASS_NAME,
					LoggingAppender.METHOD_NAME,
					LoggingAppender.LINE_NUMBER,
					LoggingAppender.MESSAGE
			};
			//
			ArrayList<Map<String, Object>> list = new ArrayList<Map<String, Object>>();
			for (int i = 0; i < array.size(); i++) {
				JSONObject json = array.getJSONObject(i);
				json.put(LoggingAppender.APP_NAME,   json.getString(LoggingAppender.APP_NAME));
				json.put(LoggingAppender.TIMESTAMP + "_fmd",   json.getString(LoggingAppender.TIMESTAMP + "_fmd"));
				json.put(LoggingAppender.LEVEL,         json.getString(LoggingAppender.LEVEL));
				json.put(LoggingAppender.CLASS_NAME,    json.getString(LoggingAppender.CLASS_NAME));
				json.put(LoggingAppender.METHOD_NAME,   json.getString(LoggingAppender.METHOD_NAME));
				json.put(LoggingAppender.LINE_NUMBER,   json.getString(LoggingAppender.LINE_NUMBER));
				json.put(LoggingAppender.MESSAGE,       json.getString(LoggingAppender.MESSAGE));
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
