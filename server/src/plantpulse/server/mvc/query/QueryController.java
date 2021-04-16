package plantpulse.server.mvc.query;

import java.util.Date;
import java.util.List;

import javax.servlet.http.HttpServletRequest;
import javax.ws.rs.FormParam;

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
import org.springframework.web.bind.annotation.ResponseBody;

import plantpulse.cep.domain.Query;
import plantpulse.cep.engine.config.ConfigurationManager;
import plantpulse.cep.service.QueryService;
import plantpulse.server.mvc.JSONResult;
import plantpulse.server.mvc.util.SessionUtils;

@Controller
public class QueryController {

	private static final Log log = LogFactory.getLog(QueryController.class);

	public static final int DEFAULT_DISPLAY_COUNT = 50; // 50 개
	public static final int DEFAULT_EXPIRE_TIME = 60 * 60 * 1; // 1시간

	private QueryService query_service = new QueryService();

	@RequestMapping(value = "/query/index", method = RequestMethod.GET)
	public String index(Model model, HttpServletRequest request) throws Exception {
		Query query = new Query();
		if (StringUtils.isEmpty(query.getEpl())) {
			String last_run_epl = (String) request.getSession().getAttribute("_last_run_epl");
			if (StringUtils.isEmpty(last_run_epl)) {
				String default_query_string = ConfigurationManager.getInstance().getApplication_properties().getProperty("query.default.query.string");
				query.setEpl(default_query_string);
			} else {
				query.setEpl(last_run_epl);
			}
		}
		;

		String display_count = ConfigurationManager.getInstance().getApplication_properties().getProperty("query.default.display.count");
		String chart_fixed = ConfigurationManager.getInstance().getApplication_properties().getProperty("query.default.display.chart.fixed");
		String expire_time = ConfigurationManager.getInstance().getApplication_properties().getProperty("query.default.expire.time.sec");

		query.setDisplay_count(Integer.parseInt(display_count)); //
		query.setChart_fixed(Boolean.parseBoolean(chart_fixed));
		query.setExpire_time(Integer.parseInt(expire_time)); //
		query.setResult_format("JSON");

		model.addAttribute("query", query);
		//
		return "query/index";
	}

	@RequestMapping(value = "/query/run", method = RequestMethod.POST)
	public String run(@ModelAttribute("query") Query query, BindingResult br, Model model, HttpServletRequest request) throws Exception {

		try {

			// 마지막 실행 EPL 저장
			request.getSession().setAttribute("_last_run_epl", query.getEpl());
			//
			query.setId("QUERY_TEMP_" + System.currentTimeMillis());
			query.setUrl("/query/QUERY_TEMP_" + System.currentTimeMillis());
			query.setInsert_date(new Date());
			query_service.run(query);
			//
			model.addAttribute("success", "쿼리를 정상적으로 실행하였습니다.");
		} catch (Exception ex) {
			String msg = ex.getMessage();
			if (msg.equals("Unexpected end-of-input []"))
				msg = "쿼리 구문을 입력하여 주십시오.";
			model.addAttribute("error", "쿼리 구문이 잘못되었습니다 : 메세지=[" + msg + "]");
			log.warn(msg, ex);
		}
		return "query/index";
	}

	@RequestMapping(value = "/query/top", method = RequestMethod.POST)
	public String runWithTop(@FormParam("query") String query, Model model, HttpServletRequest request) throws Exception {

		try {

			// 마지막 실행 EPL 저장
			request.getSession().setAttribute("_last_run_epl", query);

			//
			Query q = new Query();
			q.setDisplay_count(DEFAULT_DISPLAY_COUNT); // 30개
			q.setExpire_time(DEFAULT_EXPIRE_TIME); // 60초
			q.setChart_fixed(false);
			q.setResult_format("JSON");
			q.setEpl(query);
			q.setId("QUERY_TEMP_" + System.currentTimeMillis());
			q.setUrl("/query/QUERY_TEMP_" + System.currentTimeMillis());
			q.setInsert_date(new Date());
			model.addAttribute("query", q);
			query_service.run(q);
			//
			model.addAttribute("success", "쿼리를 정상적으로 실행하였습니다.");
		} catch (Exception ex) {
			String msg = ex.getMessage();
			if (msg.equals("Unexpected end-of-input []"))
				msg = "쿼리 구문을 입력하여 주십시오.";
			model.addAttribute("error", "쿼리 구문이 잘못되었습니다 : 메세지=[" + msg + "]");
			log.warn(msg, ex);
		}
		return "query/index";
	}

	@RequestMapping(value = "/query/config", method = RequestMethod.GET)
	public String add(Model model) throws Exception {
		Query query = new Query();
		query.setDisplay_count(DEFAULT_DISPLAY_COUNT); // 50개
		query.setChart_fixed(false);
		query.setExpire_time(DEFAULT_EXPIRE_TIME); // 60초
		query.setResult_format("JSON");
		//
		model.addAttribute("query", query);
		return "query/config";
	}

	@RequestMapping(value = "/query/epl", method = RequestMethod.POST)
	public @ResponseBody Query epl(@ModelAttribute("query") Query query, BindingResult br, Model model) throws Exception {
		try {
			//
			query.setId("_TEMP_" + System.currentTimeMillis());
			query.setUrl("/query/" + query.getId());
			query.setEpl(query.getEpl());
			//
			query_service.compile(query);
			//

		} catch (Exception ex) {
			String msg = "쿼리 구문이 잘못되었습니다 : 메세지=[" + ex.getMessage() + "]";
			query.setError(msg);
			log.warn(msg, ex);
		}
		return query;
	}
	
	
	@RequestMapping(value = "/query/test", method = RequestMethod.POST)
	public @ResponseBody Query test(Model model, HttpServletRequest request) throws Exception {
		Query query = new Query();
		try {
			//
			String eql = request.getParameter("eql");
			query.setId("_TEMP_" + System.currentTimeMillis());
			query.setUrl("/query/" + query.getId());
			query.setEpl(eql);
			//
			query_service.compile(query);
			//

		} catch (Exception ex) {
			String msg = "쿼리 구문이 잘못되었습니다 : 메세지=[" + ex.getMessage() + "]";
			query.setError(msg);
			log.warn(msg, ex);
		}
		return query;
	}
	
	@RequestMapping(value = "/query/clear", method = RequestMethod.GET)
	public @ResponseBody String clear(Model model, HttpServletRequest request) throws Exception {
		query_service.clearTempQuery();
		return "SUCCESS";
	}

	@RequestMapping(value = "/query/last", method = RequestMethod.GET)
	public @ResponseBody String last(@ModelAttribute("query") Query query, Model model, HttpServletRequest request) throws Exception {
		String _last_run_epl = (String) request.getSession().getAttribute("_last_run_epl");
		if (_last_run_epl == null)
			_last_run_epl = "";
		return _last_run_epl;
	}

	@RequestMapping(value = "/query/history", method = RequestMethod.GET)
	public String history(Model model, HttpServletRequest request) throws Exception {
		String insert_user_id = SessionUtils.getUserFromSession(request).getUser_id();
		List<Query> query_history_list = query_service.getQueryHistoryList(insert_user_id);
		model.addAttribute("query_history_list", query_history_list);
		log.info("Query history size = " + query_history_list.size());
		return "query/history";
	}

	@RequestMapping(value = "/query/save", method = RequestMethod.POST)
	public @ResponseBody JSONResult save(@ModelAttribute("query") Query query, Model model, HttpServletRequest request) throws Exception {
		String insert_user_id = SessionUtils.getUserFromSession(request).getUser_id();
		query.setInsert_user_id(insert_user_id);
		//
		query_service.saveQueryHistory(query);
		log.info("Query history saved : query=" + query);
		return new JSONResult(JSONResult.SUCCESS, "쿼리를 저장하였습니다.");
	}

	@RequestMapping(value = "/query/delete/{query_history_seq}", method = RequestMethod.POST)
	public @ResponseBody JSONResult delete(@PathVariable("query_history_seq") Long query_history_seq, Model model, HttpServletRequest request) throws Exception {
		query_service.deleteQueryHistory(query_history_seq);
		log.info("Query history deleted : seq=" + query_history_seq);
		return new JSONResult(JSONResult.SUCCESS, "쿼리를 삭제하였습니다.");
	}

	@RequestMapping(value = "/query/deleteAll", method = RequestMethod.POST)
	public @ResponseBody JSONResult deleteAll(Model model, HttpServletRequest request) throws Exception {
		String insert_user_id = SessionUtils.getUserFromSession(request).getUser_id();
		;
		query_service.deleteAllQueryHistory(insert_user_id);
		return new JSONResult(JSONResult.SUCCESS, "저장된 쿼리를  모두 삭제하였습니다.");
	}

}
