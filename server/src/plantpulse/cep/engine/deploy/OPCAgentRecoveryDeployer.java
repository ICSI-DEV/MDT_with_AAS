package plantpulse.cep.engine.deploy;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.commons.lang.StringUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.json.JSONObject;

import plantpulse.cep.dao.OPCDAO;
import plantpulse.cep.dao.SiteDAO;
import plantpulse.cep.dao.TagDAO;
import plantpulse.cep.engine.logging.EngineLogger;
import plantpulse.cep.service.RestService;
import plantpulse.domain.OPC;
import plantpulse.domain.Site;
import plantpulse.domain.Tag;
import plantpulse.server.mvc.util.Constants;

/**
 * OPCAgentRecoveryDeployer
 * @author leesa
 *
 */
public class OPCAgentRecoveryDeployer implements Deployer {

	private static final Log log = LogFactory.getLog(OPCAgentRecoveryDeployer.class);

	public void deploy() {

		// 1. OPC 목록 로딩
		RestService restService = new RestService();
		OPCDAO oPCDAO = new OPCDAO();
		SiteDAO siteDAO = new SiteDAO();
		TagDAO tagDAO = new TagDAO();

		try {
			List<OPC> opcList = oPCDAO.selectOpcList();
			if (opcList == null) {
				log.warn("Not found any OPC Information.");
			}

			// 2. OPC별 Agent Start
			for (int x = 0; opcList != null && x < opcList.size(); x++) {
				OPC opc = opcList.get(x);
				if (StringUtils.isNotEmpty(opc.getOpc_agent_ip())) {
					Tag tag = new Tag();
					tag.setOpc_id(opc.getOpc_id());
					String path = "http://" + opc.getOpc_agent_ip() + ":" + String.valueOf(opc.getOpc_agent_port());
					Site site = siteDAO.selectSiteInfo(opc.getSite_id());
					List<Tag> tagList = tagDAO.selectTagList(tag, Constants.TYPE_OPC);


					List<JSONObject> list = new ArrayList<>();

					JSONObject tagJson;
					for(Tag t : tagList) {
						tagJson = new JSONObject();
						tagJson.put("tag_id", t.getTag_id());
						tagJson.put("opc_id", t.getOpc_id());
						tagJson.put("site_id", t.getSite_id());
						tagJson.put("tag_name", t.getTag_name());
						tagJson.put("java_type", t.getJava_type());
						list.add(tagJson);
					}

					JSONObject jsonObject = new JSONObject();
					jsonObject.put("opc", opc);
					jsonObject.put("site", site);
					jsonObject.put("tags", tagList);

					Map<String, Object> map = new HashMap<String, Object>();
					map.put("jsonString", jsonObject.toString());
					map.put("path", path);

					restService.recoveryOPC(map);
				}
			}
		} catch (Exception ex) {
			EngineLogger.error("OPC 에이전트에 연결하는 도중 오류가 발생하였습니다 : 에러=[" + ex.getMessage() + "]");
			log.warn("OPC Agent Starting error : " + ex.getMessage(), ex);
		}
	}
}
