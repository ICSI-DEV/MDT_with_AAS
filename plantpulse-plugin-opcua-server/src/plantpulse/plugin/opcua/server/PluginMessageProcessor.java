package plantpulse.plugin.opcua.server;

import java.util.Map;
import java.util.Properties;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import plantpulse.json.JSONObject;


public class PluginMessageProcessor {


	private static final Log log = LogFactory.getLog(PluginMessageProcessor.class);
	
	private PlantPulseNamespace namespace;


	public PluginMessageProcessor(PlantPulseNamespace namespace) {
		this.namespace = namespace;
	}


	/**
	 * process
	 *
	 * @param headers _event_type and _event_class
	 * @param body
	 */
	public void process(Map<String, String> headers, String body) {
		try {
			namespace.setValue(JSONObject.fromObject(body));
		} catch (Exception e) {
			log.error("Message processor error : " + e.getMessage(), e);
		}
	}


	public PlantPulseNamespace getNamespace() {
		return namespace;
	}

	

}
