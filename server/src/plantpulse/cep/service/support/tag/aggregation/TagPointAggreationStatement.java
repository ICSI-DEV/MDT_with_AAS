package plantpulse.cep.service.support.tag.aggregation;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.json.JSONObject;

import com.espertech.esper.client.EventBean;
import com.espertech.esper.client.UpdateListener;

import plantpulse.cep.engine.CEPEngineManager;
import plantpulse.cep.service.client.StorageClient;
import plantpulse.domain.Tag;


/**
 * TagPointAggreationStatement
 * @author lsb
 *
 */
public class TagPointAggreationStatement implements UpdateListener {

	private static final Log log = LogFactory.getLog(TagPointAggreationStatement.class);
	
	private Tag tag;
	private String term;

	public TagPointAggreationStatement(Tag tag, String term) {
		this.tag = tag;
		this.term = term;
	}

	public String getEPL() {
		//
		
		//
		StringBuffer epl = new StringBuffer();
		epl.append(" CONTEXT " + ("EVERY_" + term) + " "); 
		epl.append(" SELECT tag_id, ");
		epl.append("        current_timestamp() as timestamp, ");
		epl.append("        count(*) as count, ");
		epl.append("        first(to_double(value)) as first, ");
		epl.append("        last(to_double(value)) as last, ");
		epl.append("        min(to_double(value)) as min, ");
		epl.append("        max(to_double(value)) as max, ");
		epl.append("        avg(to_double(value)) as avg, ");
		epl.append("        sum(to_double(value)) as sum, ");
		epl.append("        stddev(to_double(value)) as stddev, ");
		epl.append("        median(to_double(value)) as median  ");
		epl.append("   FROM Point(tag_id='" + tag.getTag_id() + "') ");
		epl.append(" OUTPUT last when terminated ");
		return epl.toString();
	};
	
	
	public String getName(){
		return "PP_TAG_AGGREGATION_" + tag.getTag_id() + "_" + term + "_STMT";
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
			
			long current_timestamp = System.currentTimeMillis();

			if (newEvents == null) {
				return;
			}

			if (newEvents != null && newEvents.length > 0) {
				EventBean event = newEvents[0];
				if (event != null) {
					
					String json_text = CEPEngineManager.getInstance().getProvider().getEPRuntime().getEventRenderer().renderJSON("json", event);
					JSONObject json = JSONObject.fromObject(JSONObject.fromObject(json_text).getJSONObject("json").toString());
					log.debug("TagAggreation underlying : json=" + json.toString());
					
					//
					StorageClient client = new StorageClient();
					client.forInsert().insertPointAggregation(tag, term, current_timestamp, json);
				}
			}
		} catch (Exception ex) {
			log.error("TagAggreationStatement error : " + ex.getMessage(), ex);
		}
	}

}
