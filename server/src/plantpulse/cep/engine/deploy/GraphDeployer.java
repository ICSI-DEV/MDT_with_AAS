package plantpulse.cep.engine.deploy;

import java.util.List;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.realdisplay.framework.util.StringUtils;

import plantpulse.cep.dao.GraphDAO;
import plantpulse.cep.domain.Graph;
import plantpulse.cep.domain.Query;
import plantpulse.cep.engine.logging.EngineLogger;
import plantpulse.cep.service.QueryService;
import plantpulse.cep.statement.QueryStatement;

public class GraphDeployer implements Deployer  {

	private static final Log log = LogFactory.getLog(GraphDeployer.class);

	public void deploy() {
		try {
			GraphDAO dao = new GraphDAO();
			List<Graph> list = dao.getGraphListAll();
			for (int i = 0; list != null && i < list.size(); i++) {
				Graph graph = list.get(i);
				//
				QueryService query_service = new QueryService();
				Query query = new Query();
				query.setId(graph.getGraph_id());
				query.setUrl(graph.getWs_url());
				query.setEpl(graph.getEpl());
				//
				if (StringUtils.isNotEmpty(graph.getEpl())) {
					if (graph.getGraph_type().equals("TABLE")) {
						query_service.run(query, QueryStatement.MODE_ARRAY);
					} else {
						query_service.run(query, QueryStatement.MODE_SINGLE);
					}
				}
			}
		} catch (Exception ex) {
			EngineLogger.error("그래프를 배치하는 도중 오류가 발생하였습니다 : 에러=" + ex.getMessage());
			log.warn("Graph deploy error : " + ex.getMessage(), ex);
		}
	}

}
