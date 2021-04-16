package plantpulse.cep.engine.deploy;

import java.util.List;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.json.JSONArray;
import org.json.JSONObject;

import com.espertech.esper.client.EPStatement;
import com.espertech.esper.client.EventBean;
import com.espertech.esper.client.UpdateListener;

import plantpulse.cep.dao.SiteDAO;
import plantpulse.cep.engine.CEPEngineManager;
import plantpulse.cep.engine.logging.EngineLogger;
import plantpulse.cep.listener.ResultServiceManager;
import plantpulse.cep.listener.push.PushClient;
import plantpulse.domain.Site;

/**
 * SiteTagCountDeployer
 * 
 * @author lsb
 *
 */
public class SiteTagCountDeployer implements Deployer {

	private static final Log log = LogFactory.getLog(SiteTagCountDeployer.class);
	
	//
	public static final String PUSH_TODAY_URL = "/today";

	public void deploy() {
		try {

			SiteDAO site_dao = new SiteDAO();
			List<Site> site_list = site_dao.selectSites();

			// 사이트 1초 전체 카운트
			for (int i = 0; i < site_list.size(); i++) {
				final Site site = site_list.get(i);
				//
				StringBuffer eql = new StringBuffer();
				eql.append(" SELECT current_timestamp() as timestamp, COUNT(*) AS tag_count FROM Point( site_id = '" + site.getSite_id() + "' ).win:time_batch(1 sec) OUTPUT LAST EVERY 1 SEC  ");
				EPStatement ep = CEPEngineManager.getInstance().getProvider().getEPAdministrator().createEPL(eql.toString(), "TODAY_TAG_COUNT_1SEC");
				ep.addListener(new UpdateListener() {
					@Override
					public void update(EventBean[] newEvents, EventBean[] oldEvents) {
						//
						try {
							final JSONArray data_array = new JSONArray();
							for (int i = 0; newEvents != null && i < newEvents.length; i++) {
								EventBean event = newEvents[i];
								if (event != null) {
									//
									String json_text = CEPEngineManager.getInstance().getProvider().getEPRuntime().getEventRenderer().renderJSON("json", event);
									JSONObject json = JSONObject.fromObject(JSONObject.fromObject(json_text).getJSONObject("json").toString());
									//
									PushClient client = ResultServiceManager.getPushService().getPushClient(PUSH_TODAY_URL + "/tag/count/1sec/" + site.getSite_id());
									client.sendJSON(json);
								}
							}

						} catch (Exception e) {
							log.error("Today aggregation listener error : " + e.getMessage(), e);
						}
						//
					}
				});

			}

		} catch (Exception ex) {
			EngineLogger.error("사이트 집계 처리기를 배치하는 도중 오류가 발생하였습니다 : 에러=" + ex.getMessage());
			log.warn("Today aggregation deploy error : " + ex.getMessage(), ex);
		}
	}

}
