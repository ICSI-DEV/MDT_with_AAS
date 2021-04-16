package plantpulse.server.mvc.graph;

import java.util.Date;
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
import org.springframework.validation.BindingResult;
import org.springframework.web.bind.annotation.ModelAttribute;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.ResponseBody;

import plantpulse.cep.domain.Graph;
import plantpulse.cep.domain.Query;
import plantpulse.cep.service.GraphService;
import plantpulse.cep.service.QueryService;
import plantpulse.cep.service.StorageService;
import plantpulse.cep.service.TagService;
import plantpulse.cep.statement.QueryStatement;
import plantpulse.domain.Tag;
import plantpulse.domain.User;
import plantpulse.server.mvc.util.Constants;
import plantpulse.server.mvc.util.DateUtils;
import plantpulse.server.mvc.util.GraphUtils;
import plantpulse.server.mvc.util.SessionUtils;

@Controller
public class GraphController {

	private static final Log log = LogFactory.getLog(GraphController.class);

	private QueryService query_service = new QueryService();

	private GraphService graph_service = new GraphService();

	@Autowired
	private StorageService storage_service;

	@Autowired
	private TagService tag_service;

	@RequestMapping(value = "/graph/index", method = RequestMethod.GET)
	public String index(@ModelAttribute("graph") Graph graph, Model model, HttpServletRequest request) throws Exception {
		User user = SessionUtils.getUserFromSession(request);
		List<Graph> graph_list = graph_service.getGraphListByUserId(user.getUser_id());
		model.addAttribute("graph_list", graph_list);
		return "graph/index";
	}

	@RequestMapping(value = "/graph/add/{dashboard_id}", method = RequestMethod.GET)
	public String form(@PathVariable("dashboard_id") String dashboard_id, Model model) throws Exception {
		Graph graph = new Graph();
		graph.setDashboard_id(dashboard_id); //
		graph.setGraph_priority(Constants.DEFAULT_GRAPH_PRIORITY);
		graph.setEpl(Constants.DEFAULT_GRAPH_EQL);
		graph.setBar_display(15);
		graph.setLine_display(10);
		graph.setLine_ymin("auto");
		graph.setLine_ymax("auto");
		graph.setArea_display(10);
		graph.setArea_ymin("auto");
		graph.setArea_ymax("auto");
		graph.setGraph_type(Constants.DEFAULT_GRAPH_TYPE);
		model.addAttribute("graph", graph);
		return "graph/form";
	}

	@RequestMapping(value = "/graph/quick/add/{dashboard_id}", method = RequestMethod.GET)
	public String formQuick(@PathVariable("dashboard_id") String dashboard_id, Model model) throws Exception {
		Graph graph = new Graph();
		graph.setDashboard_id(dashboard_id); //
		graph.setGraph_priority(Constants.DEFAULT_GRAPH_PRIORITY);
		graph.setEpl(Constants.DEFAULT_GRAPH_EQL);
		graph.setBar_display(15);
		graph.setLine_display(10);
		graph.setLine_ymin("auto");
		graph.setLine_ymax("auto");
		graph.setArea_display(10);
		graph.setArea_ymin("auto");
		graph.setArea_ymax("auto");
		graph.setGraph_type(Constants.DEFAULT_GRAPH_TYPE);
		model.addAttribute("graph", graph);
		return "graph/quick/form";
	}

	@RequestMapping(value = "/graph/quick/save", method = RequestMethod.POST)
	public @ResponseBody Graph saveQuick(@ModelAttribute("graph") Graph graph, BindingResult br, Model model, HttpServletRequest request) throws Exception {

		graph.setDashboard_id(graph.getDashboard_id());//
		graph.setGraph_id("GRAPH_" + System.currentTimeMillis());
		graph.setGraph_json(""); // 서비스단에서 오버라이드
		graph.setInsert_user_id(SessionUtils.getUserFromSession(request).getUser_id());
		//
		// graph.setEpl("SELECT * From Alarm.win:time(5 min) where tag_id = '" +
		// graph.getTag_id() + "' output every 1 sec");
		// graph.setWs_url("/alarm/tag/" + graph.getTag_id());

		graph_service.insertGraph(graph);
		//
		return graph;
	}

	@RequestMapping(value = "/graph/get/{graph_id}", method = RequestMethod.GET)
	public @ResponseBody Graph get(@PathVariable("graph_id") String graph_id, Model model, HttpServletRequest request) throws Exception {
		Graph graph = new Graph();
		if (graph_id != null) {
			graph = graph_service.getGraphById(graph_id);
			model.addAttribute("graph", graph);
		}
		return graph;
	}

	@RequestMapping(value = "/graph/json/{graph_id}", method = RequestMethod.GET)
	public @ResponseBody String json(@PathVariable("graph_id") String graph_id, Model model, HttpServletRequest request) throws Exception {
		String json = "";
		Graph graph = new Graph();
		if (graph_id != null) {
			graph = graph_service.getGraphById(graph_id);
			json = graph.getGraph_json();
		}
		return json;
	}

	@RequestMapping(value = "/graph/template/{graph_id}", method = RequestMethod.GET)
	public String template(@PathVariable("graph_id") String graph_id, Model model, HttpServletRequest request) throws Exception {
		Graph graph = new Graph();
		try {
			if (graph_id != null) {
				graph = graph_service.getGraphById(graph_id);
				if (graph == null)
					throw new Exception("[graph] is null.");
				model.addAttribute("graph", graph);
				JSONObject json = JSONObject.fromObject(graph.getGraph_json());
				//
				if (json.has("tag_id")) {
					graph.setTag_id(json.getString("tag_id"));
					if (StringUtils.isNotEmpty(graph.getTag_id())) {
						Tag tag = graph_service.selectTagByQuickGraph(graph);
						model.addAttribute("tag", tag);
					}
				}
			} else {
				throw new Exception("[graph_id] is null.");
			}
			log.debug("graph=" + graph.toString());
		} catch (Exception ex) {
			log.error("Graph template getting failed : " + ex.getMessage(), ex);
		}
		return "graph/template";
	}

	@RequestMapping(value = "/graph/preview", method = RequestMethod.POST, produces = "text/html;charset=UTF-8")
	public String preview(@ModelAttribute("graph") Graph graph, Model model, HttpServletRequest request) throws Exception {

		// log.info("graph.getGraph_title()=" + graph.getGraph_title());

		//
		Query query = new Query();
		query.setId("_PREVIEW_" + graph.getGraph_id());
		query.setUrl("/graph/" + query.getId());
		query.setEpl(graph.getEpl());
		query.setExpire_time(3600);
		query.setInsert_date(new Date());
		if (graph.getGraph_type().equals("TABLE")) {
			query_service.run(query, QueryStatement.MODE_ARRAY);
		} else {
			query_service.run(query, QueryStatement.MODE_SINGLE);
		}
		//
		graph.setGraph_id(query.getId());
		graph.setWs_url(query.getUrl());
		//
		graph.setGraph_json(GraphUtils.getGraphJSON(graph));
		//
		model.addAttribute("graph", graph);

		//
		return "graph/template";
	}

	@RequestMapping(value = "/graph/edit/{graph_id}", method = RequestMethod.GET)
	public String form(@PathVariable("graph_id") String graph_id, Model model, HttpServletRequest request) throws Exception {
		Graph graph = new Graph();
		if (graph_id != null) {
			graph = graph_service.getGraphById(graph_id);
			model.addAttribute("graph", graph);
		}
		return "graph/formEdit";
	}

	@RequestMapping(value = "/graph/info/{graph_id}", method = RequestMethod.GET)
	public String info(@PathVariable("graph_id") String graph_id, Model model, HttpServletRequest request) throws Exception {
		Graph graph = new Graph();
		if (graph_id != null) {
			graph = graph_service.getGraphById(graph_id);
			model.addAttribute("graph", graph);
		}
		return "graph/info";
	}

	@RequestMapping(value = "/graph/save", method = RequestMethod.POST)
	public @ResponseBody Graph save(@ModelAttribute("graph") Graph graph, BindingResult br, Model model, HttpServletRequest request) throws Exception {
		if (StringUtils.isEmpty(graph.getGraph_id()) || graph.getGraph_id().startsWith("_TEMP_") || graph.getGraph_id().startsWith("_PREVIEW_")) {
			graph.setDashboard_id(graph.getDashboard_id());//
			graph.setGraph_id("GRAPH_" + System.currentTimeMillis());
			graph.setGraph_json(""); // 서비스단에서 오버라이드
			graph.setInsert_user_id(SessionUtils.getUserFromSession(request).getUser_id());
			graph_service.insertGraph(graph);
		} else {
			graph_service.updateGraph(graph);
		}

		//
		return graph;
	}

	@RequestMapping(value = "/graph/delete/{graph_id}", method = RequestMethod.POST)
	public @ResponseBody String delete(@PathVariable("graph_id") String graph_id, Model model, HttpServletRequest request) throws Exception {
		graph_service.deleteGraph(graph_id);
		return graph_id;
	}

	@RequestMapping(value = "/graph/epl", method = RequestMethod.POST)
	public @ResponseBody Query epl(@ModelAttribute("graph") Graph graph, BindingResult br, Model model) throws Exception {
		Query query = new Query();
		try {

			//
			query.setId("_TEMP_" + System.currentTimeMillis());
			query.setUrl("/graph/" + query.getId());
			query.setEpl(graph.getEpl());
			query.setExpire_time(3600);
			query.setInsert_date(new Date());
			//
			query_service.run(query);
			//

		} catch (Exception ex) {
			String msg = "쿼리 구문이 잘못되었습니다 : 메세지=[" + ex.getMessage() + "]";
			query.setError(msg);
			log.warn(msg, ex);
		}
		return query;
	}

	@RequestMapping(value = "/graph/tag/data/history/{tag_id}", method = RequestMethod.GET)
	public @ResponseBody String history(@PathVariable("tag_id") String tag_id, Model model) throws Exception {
		JSONArray array = new JSONArray();
		try {
			
			int limit = 60;

			// 기준 시간
			String date_from = "2000-01-01 00:00";
			String date_to = DateUtils.currDateBy24();

			// 데이터 목록
			Tag tag = tag_service.selectTag(tag_id);
			List<Map<String, Object>> tag_data_list = storage_service.list(tag, date_from + ":00", date_to + ":59", limit, null);
			for (int i = 0; i < tag_data_list.size(); i++) {
				Map map = tag_data_list.get(i);
				JSONObject json = JSONObject.fromObject(map);
				json.put("timestamp", DateUtils.fmtISO(((Date) map.get("timestamp")).getTime()));
				array.add(json);
			}

		} catch (Exception ex) {
			String msg = "그래프 태그 히스토리 데이터 조회에 실패하였습니다  : 메세지=[" + ex.getMessage() + "]";
			log.warn(msg, ex);
		}
		return array.toString();
	}
	
	
	@RequestMapping(value = "/graph/tag/data/history/{tag_id}/{limit}", method = RequestMethod.GET)
	public @ResponseBody String history(@PathVariable("tag_id") String tag_id, @PathVariable("limit") int limit, Model model) throws Exception {
		JSONArray array = new JSONArray();
		try {
			

			// 기준 시간
			String date_from = "2000-01-01 00:00";
			String date_to = DateUtils.currDateBy24();

			// 데이터 목록
			Tag tag = tag_service.selectTag(tag_id);
			List<Map<String, Object>> tag_data_list = storage_service.list(tag, date_from + ":00", date_to + ":59", limit, null);
			for (int i = 0; i < tag_data_list.size(); i++) {
				Map map = tag_data_list.get(i);
				JSONObject json = JSONObject.fromObject(map);
				json.put("timestamp", DateUtils.fmtISO(((Date) map.get("timestamp")).getTime()));
				array.add(json);
			}

		} catch (Exception ex) {
			String msg = "그래프 태그 히스토리 데이터 조회에 실패하였습니다  : 메세지=[" + ex.getMessage() + "]";
			log.warn(msg, ex);
		}
		return array.toString();
	}

}