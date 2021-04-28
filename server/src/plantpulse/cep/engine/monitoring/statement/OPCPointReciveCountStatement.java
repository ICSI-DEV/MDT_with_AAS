package plantpulse.cep.engine.monitoring.statement;

import java.util.HashMap;
import java.util.Map;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import com.espertech.esper.client.EventBean;

public class OPCPointReciveCountStatement  extends Statement {

	private static final Log log = LogFactory.getLog(OPCPointReciveCountStatement.class);

	public OPCPointReciveCountStatement(String serviceId, String name) {
		super(serviceId, name);
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see org.realdisplay.cep.engine.monitoring.EPLProvider#getEPL()
	 */
	@Override
	public String getEPL() {
		return " SELECT opc_id, COUNT(*) AS count FROM Point.win:text_timed_batch(timestamp, 10 min) GROUP BY opc_id";
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
			};
			if (newEvents != null && newEvents.length > 0) {
				EventBean event = newEvents[0];
				if (event != null) {
					Map<String, Object> data_map = new HashMap<String, Object>();
					String opc_id = event.get("opc_id").toString();
					int count = (Double.valueOf(event.get("count").toString()).intValue());
				};
				
				/*
				 * TODO 데이터베이스에 저장된 연결 목록과 비교를 통해 데이터 수집 헬스 체크
				LoadingCache<String, Integer> exp_map = CacheBuilder.newBuilder()
					    .concurrencyLevel(4)
					    .weakKeys()
					    .maximumSize(10000)
					    .expireAfterWrite(10, TimeUnit.MINUTES)
					    .build(
					        new CacheLoader<String, Integer>() {
					          public Integer load(String key) throws Exception {
					            return  this.load(key);
					          }
					        });
				
				exp_map.put("", 1);
				*/
				
			}
		} catch (Exception ex) {
			log.error("OPCPointReciveCountStatement error : " + ex.getMessage(), ex);
		}
	}

}

