package plantpulse.cep.engine.dds.processor;

import org.apache.commons.lang.StringUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.json.JSONObject;

import plantpulse.cep.engine.monitoring.metric.MetricNames;
import plantpulse.cep.engine.monitoring.metric.MonitoringMetrics;
import plantpulse.cep.service.client.MessageClient;
import plantpulse.domain.Tag;

/**
 * DDSMessageProcessor
 * 
 * @author leesa
 *
 */
public class DDSMessageProcessor {

	private static final Log log = LogFactory.getLog(DDSMessageProcessor.class);

	private MessageClient client;

	public DDSMessageProcessor() {
		client = new MessageClient();
	}

	/**
	 * Tag 에 연관된 데이터를 KAFKA로 전송한다.
	 * 
	 * @param topic
	 * @param data
	 */
	public void sendTagTypeData(String type, Tag tag, JSONObject data) {

		try {
			if (type.equals(DDSTypes.TAG_POINT)) {
				if (StringUtils.isNotEmpty(tag.getPublish_kafka()) && tag.getPublish_kafka().equals("Y")) {
					client.toKAFKA().send("tag-" + type, tag.getTag_id(), data.toString());
					MonitoringMetrics.getMeter(MetricNames.DDS_PROCESSED).mark();
				}
				;
				if (StringUtils.isNotEmpty(tag.getPublish_mqtt()) && tag.getPublish_mqtt().equals("Y")) {
					client.toMQTT().send("tag/" + type + "/" + tag.getTag_id(), data.toString());
					MonitoringMetrics.getMeter(MetricNames.DDS_PROCESSED).mark();
				}
				;
				MonitoringMetrics.getMeter("DDS_PROCESSED_TYPE_TAG_" + type.toUpperCase()).mark();
			} else { // 태그 알람이면, 무조건 전송
				client.toKAFKA().send("tag-" + type, tag.getTag_id(), data.toString());
				client.toMQTT().send("tag/" + type + "/" + tag.getTag_id(), data.toString());
				
				MonitoringMetrics.getMeter("DDS_PROCESSED_TYPE_TAG_" + type.toUpperCase()).mark();
			}
		} catch (Exception e) {
			log.error("Tag type data send error : " + e.getMessage(), e);
		}
	}

	/**
	 * Asset 에 연관된 데이터를 KAFKA와 MQTT 로 전송한다.
	 * 
	 * @param topic
	 * @param data
	 */
	public void sendAssetTypeData(String type, String asset_id, JSONObject data) {
		try {
			client.toKAFKA().send("asset-" + type + "", asset_id, data.toString());
			client.toMQTT().send("asset/" + type + "/" + asset_id, data.toString());
			MonitoringMetrics.getMeter(MetricNames.DDS_PROCESSED).mark(2);
			MonitoringMetrics.getMeter("DDS_PROCESSED_TYPE_ASSET_" + type.toUpperCase()).mark(2);
		} catch (Exception e) {
			log.error("Asset type data send error : " + e.getMessage(), e);
		}
	}

}
