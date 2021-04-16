package plantpulse.cep.service.support.tag.calculation;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.json.JSONObject;
import org.springframework.util.StringUtils;

import com.espertech.esper.client.EventBean;
import com.espertech.esper.client.UpdateListener;

import plantpulse.cep.engine.CEPEngineManager;
import plantpulse.cep.engine.logging.EngineLogger;
import plantpulse.cep.engine.pipeline.DataFlowPipe;
import plantpulse.cep.engine.utils.JavaTypeUtils;
import plantpulse.domain.Tag;
import plantpulse.event.opc.Point;

/**
 * CalculationTagEventListener
 * @author lsb
 *
 */
public class CalculationTagEventListener  implements UpdateListener {

	private static final Log log = LogFactory.getLog(CalculationTagEventListener.class);
	
	private DataFlowPipe dataflow = CEPEngineManager.getInstance().getData_flow_pipe();

	private Tag tag;

	public CalculationTagEventListener(Tag tag) {
		this.tag = tag;
	}

	@Override
	public void update(EventBean[] newEvents, EventBean[] arg1) {

		try {

			if (newEvents != null && newEvents.length > 0) {
				EventBean event = newEvents[0];

				//
				if (event != null) {
					
					  String json_text = CEPEngineManager.getInstance().getProvider().getEPRuntime().getEventRenderer().renderJSON("json", event);
					  JSONObject json = JSONObject.fromObject(JSONObject.fromObject(json_text).getJSONObject("json").toString());
					  //
					  Point point = new Point();
					  point.setSite_id(tag.getSite_id());
					  point.setOpc_id(tag.getOpc_id());
					  point.setTag_id(tag.getTag_id());
					  point.setTag_name(tag.getTag_name());
					  point.setGroup_name("");
					  point.setType(JavaTypeUtils.convertJavaTypeToDataType(tag.getJava_type())); //
					  
					  //String 데이터 타입 체크
					  String value = "";
					  try {
						  value = json.getString("value");
					  }catch(Exception ex) {
						  log.warn("Point value data type is not String : object=[" + json.get("value") + "]");
						  return ;
					  };
					  
					  //포인트 구성 중 NULL이 있으면 제거
					  if(json.get("timestamp") == null 
							  || StringUtils.isEmpty(value)
							  || json.get("quality") == null 
							  || json.get("error_code") == null){
						  log.warn("Calculated point validate failed : timestamp, value, quality, error_code is null.");
						  return ;
					  };
					  
					  //
					  point.setTimestamp(json.getLong("timestamp"));
					  point.setValue(value);
					  point.setQuality(json.getInt("quality"));
					  point.setError_code(json.getInt("error_code"));
					 
					  //
					  dataflow.flow(point);
				}
			}

		} catch (Exception ex) {
			EngineLogger.error("계산태그[" + tag.getTag_id() + "] 업데이트 리스너에서 오류가 발생하였습니다.");
			log.error("CalculationTagEventListener update error :" + ex.getMessage(), ex);
		}
	}
	
	

}
