package plantpulse.cep.engine.pipeline.dataflow.adjuster;

import java.util.HashMap;
import java.util.Map;
import java.util.Objects;

import org.apache.commons.lang.StringUtils;

import plantpulse.cep.engine.utils.JavaTypeUtils;
import plantpulse.cep.engine.utils.TagUtils;
import plantpulse.cep.service.support.tag.TagCacheFactory;
import plantpulse.domain.Tag;
import plantpulse.event.opc.Point;

/**
 * 
 * DefaultDataFlowAdjuster
 * @author lsb
 *
 */
public class DefaultDataFlowAdjuster implements DataFlowAdjuster {
	
	public static final long ENGINE_PROCESS_TIME_MS = 0; //레이턴시 보정을 위한 엔진 프로세스 타임

	
	private static final String __EMPTY_STRING_VALUE_REPLACE = "N/A";
	
	@Override
	public boolean validate(Point point) throws Exception {
		
		//
		Tag tag = TagCacheFactory.getInstance().getTag(point.getTag_id());
		
		point.setSite_id(tag.getSite_id());
		point.setOpc_id(tag.getOpc_id());
		point.setTag_name(tag.getTag_name());
		point.setGroup_name(tag.getGroup_name());
		
		//
		if (StringUtils.isEmpty(point.getTag_id()) || StringUtils.isEmpty(point.getTag_name())
					|| StringUtils.isEmpty(point.getType()) || Objects.isNull(point.getTimestamp())
					|| StringUtils.isEmpty(point.getValue()) || "null".equals(point.getValue())
					|| Objects.isNull(point.getQuality()) || Objects.isNull(point.getError_code())) {
			
			  if(TagUtils.isStringDataType(tag)) { //스트링 타입일 경우, 널 허용
				  point.setValue(__EMPTY_STRING_VALUE_REPLACE);
				  return true;
			  }else {
				   return false; 
			  }
		} else {
			return true;
		}
	}
	
	
	@Override
	public Point beforeStream(Point point) throws Exception {
		Map<String,String> attribute = point.getAttribute();
		if(point.getAttribute() == null){
			attribute = new HashMap<String,String>();
		};
		
		
		//---------------------------------------------------------------------------------------------
		//필수 속성값 (지우지 마세요)
		//---------------------------------------------------------------------------------------------
		attribute.put("latency", String.valueOf(System.currentTimeMillis() - point.getTimestamp() - ENGINE_PROCESS_TIME_MS));
		//attribute.put("in_timestamp", System.currentTimeMillis() + "");
		//
		point.setAttribute(attribute);
		return point;
	};

	@Override
	public Point beforeStore(Point point) throws Exception {
		return point;
	}

	



}
