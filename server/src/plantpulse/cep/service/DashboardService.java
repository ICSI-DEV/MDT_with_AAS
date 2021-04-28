package plantpulse.cep.service;

import java.util.List;

import plantpulse.cep.dao.DashboardDAO;
import plantpulse.cep.dao.GraphDAO;
import plantpulse.cep.domain.Dashboard;
import plantpulse.cep.domain.Graph;
import plantpulse.cep.domain.Query;

public class DashboardService {

	private DashboardDAO dao;
	private GraphDAO graph_dao;
	private QueryService query_service;

	public DashboardService() {
		this.dao = new DashboardDAO();
		this.graph_dao = new GraphDAO();
		this.query_service = new QueryService();
	}

	public List<Dashboard> getDashboardListByUserId(String insert_user_id) throws Exception {
		return dao.getDashboardListByUserId(insert_user_id);
	}

	public List<Dashboard> getDashboardListBySecurityId(String security_id) throws Exception {
		return dao.getDashboardListBySecurityId(security_id);
	}

	public Dashboard getDashboardById(String dashboard_id) throws Exception {
		return dao.getDashboardById(dashboard_id);
	}

	public void insertDashboard(Dashboard dashboard) throws Exception {
		dao.insertDashboard(dashboard);
	}

	public void updateDashboard(Dashboard dashboard) throws Exception {
		dao.updateDashboard(dashboard);
	}

	public void updateDashboardJson(String dashboard_id, String dashboard_json) throws Exception {
		dao.updateDashboardJson(dashboard_id, dashboard_json);
	}

	public void deleteDashboard(String dashboard_id) throws Exception {
		// 대시보드내의 그래프 EQL 삭제
		List<Graph> graph_list = graph_dao.getGraphListByDashboardId(dashboard_id);
		for (int i = 0; graph_list != null && i < graph_list.size(); i++) {
			Query query = new Query();
			query.setId(graph_list.get(i).getGraph_id());
			query_service.remove(query);
		}
		//
		dao.deleteDashboard(dashboard_id);
	}

}
