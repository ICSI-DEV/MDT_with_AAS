package plantpulse.cep.engine.monitoring.statement;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.commons.lang.StringUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.json.JSONArray;

import com.espertech.esper.client.EventBean;

import plantpulse.cep.Metadata;
import plantpulse.cep.engine.CEPEngineManager;
import plantpulse.cep.engine.logging.EngineLogger;
import plantpulse.cep.engine.monitoring.MonitorConstants;
import plantpulse.cep.listener.ResultServiceManager;
import plantpulse.cep.listener.push.PushClient;

/**
 * EngineMetricStatement
 * 
 * @author lsb
 *
 */
public class StatementMetricStatement extends Statement {

	private static final Log log = LogFactory.getLog(StatementMetricStatement.class);
	
	private static final int LIMIT_CPU_TIME_WARN_SEC  = 1000 *  60 * 1;  //1분
	private static final int LIMIT_WALL_TIME_WARN_SEC = 1000 *  60 * 10; //10분

	public StatementMetricStatement(String serviceId, String name) {
		super(serviceId, name);
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see org.realdisplay.cep.engine.monitoring.EPLProvider#getEPL()
	 */
	@Override
	public String getEPL() {
		
		StringBuffer epl = new StringBuffer();
		epl.append(" SELECT statementName,  ");
		epl.append("    current_timestamp() as timestamp,  ");
		epl.append("    max(Math.round(cpuTime  / 1000000.0)) as cpuTimeMs,   ");
		epl.append("    max(Math.round(wallTime / 1000000.0)) as wallTimeMs  ");
		epl.append("  FROM com.espertech.esper.client.metric.StatementMetric.win:time(60 sec)  ");
		epl.append(" WHERE engineURI='" + Metadata.CEP_SERVICE_ID + "'   ");
		epl.append(" GROUP BY statementName  ");
		epl.append("OUTPUT LAST EVERY 10 sec  ");
		epl.append(" ORDER BY cpuTime desc, wallTime desc ");
		epl.append(" LIMIT 10 ");
	 
		return epl.toString();
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see
	 * com.espertech.esper.client.UpdateListener#update(com.espertech.esper.
	 * client.EventBean[], com.espertech.esper.client.EventBean[])
	 */
	@Override
	public void update(EventBean[] newEvents, EventBean[] oldEvents) {
		try {
			if (newEvents == null) {
				return;
			}
			List<Map<String, Object>> list = new ArrayList<Map<String, Object>>();
			for (int i = 0; i < newEvents.length; i++) {
				EventBean event = newEvents[i];
				if (event != null) {
					if (event.get("statementName") != null && StringUtils.isNotEmpty((String) event.get("statementName"))) {
						Map<String, Object> data_map = new HashMap<String, Object>();
						//data_map.put("engine_uri", event.get("engineURI") + "");
						data_map.put("timestamp", event.get("timestamp") + "");
						data_map.put("statement_name", event.get("statementName") + "");
						data_map.put("cpu_time", event.get("cpuTimeMs") + "");
						data_map.put("wall_time", event.get("wallTimeMs") + "");
						//data_map.put("num_input", event.get("numInput") + "");
						//data_map.put("num_output_i_stream", event.get("numOutputIStream") + "");
						//data_map.put("num_output_r_stream", event.get("numOutputRStream") + "");

						try {
							String eql = CEPEngineManager.getInstance().getProvider().getEPAdministrator().getStatement((String) event.get("statementName")).getText();
							data_map.put("eql", eql);
						} catch (Exception ex) {
							// TODO EQL NULL 체크 필요
						}
						
						long cpu_time = ((Long)event.get("cpuTimeMs"));
						if(cpu_time > LIMIT_CPU_TIME_WARN_SEC){
							EngineLogger.warn("EQL 스테이트먼트 [" + event.get("statementName")  +"]의 CPU 시간이 너무 오래 걸립니다. CPU 시간=[" + cpu_time + "]ms");
						};
						
						
						long wall_time = ((Long)event.get("wallTimeMs"));
						if(wall_time > LIMIT_WALL_TIME_WARN_SEC){
							EngineLogger.warn("EQL 스테이트먼트 [" + event.get("statementName")  +"]의 대기 시간이 너무 오래 걸립니다. 대기 시간=[" + wall_time + "]ms");
						};
						

						//
						list.add(data_map);
					}
				}
			}

			JSONArray json = new JSONArray();
			json = JSONArray.fromObject(list);

			PushClient client1 = ResultServiceManager.getPushService().getPushClient(MonitorConstants.PUSH_URL_STATEMENT_TABLE + "/" + serviceId);
			client1.sendJSONArray(json);

			// System.out.println(json.toString());

		} catch (Exception ex) {
			log.error("StatementMetricStatement error : " + ex.getMessage(), ex);
		}
	}

}
