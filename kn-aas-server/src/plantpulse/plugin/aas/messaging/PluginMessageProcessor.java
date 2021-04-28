package plantpulse.plugin.aas.messaging;

import java.util.Map;
import java.util.Properties;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import plantpulse.domain.Asset;
import plantpulse.event.asset.AssetData;
import plantpulse.event.opc.Point;
import plantpulse.json.JSONObject;
import plantpulse.plugin.aas.server.AASServer;


public class PluginMessageProcessor {


	private static final Log log = LogFactory.getLog(PluginMessageProcessor.class);
	
	private AASServer server;


	public PluginMessageProcessor(AASServer server) {
		this.server = server;
	}


	/**
	 * process
	 *
	 * @param headers _event_type and _event_class
	 * @param body
	 */
	public void process(Map<String, String> headers, String topic, String body) {
		try {
			if(topic.equals("pp-tag-point")) {
				JSONObject json = JSONObject.fromObject(body);
				log.debug("Kafka JSON = " + json.toString());
				server.updatePoint((Point) JSONObject.toBean(json, Point.class));
			}else {
				log.warn("Unsupport : topic=[" + topic + "]");
			}
		} catch (Exception e) {
			log.error("Message processor error : " + e.getMessage(), e);
		}
	}


	public AASServer getServer() {
		return server;
	}

	

}
