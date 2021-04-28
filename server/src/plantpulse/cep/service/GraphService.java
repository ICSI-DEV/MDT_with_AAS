package plantpulse.cep.service;

import java.util.List;

import org.realdisplay.framework.util.StringUtils;

import plantpulse.cep.dao.GraphDAO;
import plantpulse.cep.domain.Graph;
import plantpulse.cep.domain.Query;
import plantpulse.cep.statement.QueryStatement;
import plantpulse.domain.Tag;
import plantpulse.server.mvc.util.GraphUtils;

/**
 * GraphService
 * 
 * @author lenovo
 * 
 */
public class GraphService {

	private GraphDAO dao;
	private QueryService query_service;

	public GraphService() {
		this.dao = new GraphDAO();
		this.query_service = new QueryService();
	}

	public List<Graph> getGraphListByUserId(String insert_user_id) throws Exception {
		return dao.getGraphListByUserId(insert_user_id);
	}

	public List<Graph> getGraphListByDashboardId(String dashboard_id) throws Exception {
		return dao.getGraphListByDashboardId(dashboard_id);
	}

	public Graph getGraphById(String graph_id) throws Exception {
		return dao.getGraphById(graph_id);
	}

	public void insertGraph(Graph graph) throws Exception {

		// EQL이 있으면 값을 등록
		if (StringUtils.isNotEmpty(graph.getEpl())) {
			String deploy_ws_url = "/graph/" + graph.getGraph_id();
			Query query = new Query();
			// TODO EPL 스텝 파라메터를 오버라이드, 과거 템프 쿼리는 제거
			query.setId(graph.getGraph_id());
			query.setUrl(deploy_ws_url);
			query.setEpl(graph.getEpl());
			//
			if (graph.getGraph_type().equals("TABLE")) {
				query_service.run(query, QueryStatement.MODE_ARRAY);
			} else {
				query_service.run(query, QueryStatement.MODE_SINGLE);
			}
			//
			graph.setWs_url(deploy_ws_url);
		}

		graph.setGraph_json(GraphUtils.getGraphJSON(graph)); // 최종 저장할 그래프 JSON
																// 생성
		dao.insertGraph(graph);
	}

	public void updateGraph(Graph graph) throws Exception {
		dao.updateGraph(graph);
	}

	public void deleteGraph(String graph_id) throws Exception {
		Query query = new Query();
		query.setId(graph_id);
		query_service.remove(query);
		//
		dao.deleteGraph(graph_id);
	}

	public Tag selectTagByQuickGraph(Graph graph) throws Exception {
		return dao.selectTagByQuickGraph(graph.getTag_id());
	}

}
