package plantpulse.cep.engine.messaging.impoter;

import java.util.Objects;
import java.util.concurrent.atomic.AtomicLong;

import org.apache.commons.lang.StringUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.json.JSONObject;
import org.redisson.api.RQueue;

import plantpulse.buffer.db.DBDataPoint;
import plantpulse.cep.engine.config.ConfigurationManager;
import plantpulse.cep.engine.messaging.unmarshaller.DefaultTagMessageUnmarshaller;
import plantpulse.cep.engine.messaging.unmarshaller.MessageUnmarshaller;
import plantpulse.cep.engine.stream.queue.db.DBDataPointQueue;
import plantpulse.cep.engine.stream.type.queue.PointQueueFactory;
import plantpulse.event.opc.Point;

/**
 * PointImportProcessor
 * @author leesa
 *
 */
public class PointImportProcessor {
	
	private static final Log log = LogFactory.getLog(PointImportProcessor.class);
	
	private AtomicLong imported_count = new AtomicLong(0);
	
	public void process(int index, String query)  throws Exception {
		
		JSONObject event = JSONObject.fromObject(query);

		String api_key = ConfigurationManager.getInstance().getApplication_properties().getProperty("api.key");
		if(!api_key.equals(event.getString("_api_key"))){
			throw new Exception("[_api_key] this is an unauthenticated message with an invalid value."); 
		};

		//
		MessageUnmarshaller unmarshaller = new DefaultTagMessageUnmarshaller();
		Point point = unmarshaller.unmarshal(event);
		//
		//
		if (!validate(point)) {
			log.warn("Point validate failed : " + point.toString());
			;
			return;
		}
		;
		//
		DBDataPointQueue<RQueue<DBDataPoint>> db_data_queue 
		                               = PointQueueFactory.getDBDataPointQueue("_DATA_IMPORT_CLIENT_[" + index  + "]");
		
		//레디스 버퍼링 &큐 로직 추가, 캐쉬에 1차, 2차 배치 저장 프로세서
		DBDataPoint db_data = new DBDataPoint(point);
		db_data_queue.getQueue().addAsync(db_data);
		
		imported_count.incrementAndGet();
	}
	
	
	public long getTotalImportedCount() {
		return imported_count.get();
	}
	
	
	/*
	 * (non-Javadoc)
	 * 
	 * @see
	 * plantpulse.cep.engine.pipeline.dataflow.DataFlowPipe#validate(plantpulse.
	 * event.opc.Point)
	 */
     private boolean validate(Point point) throws Exception {
		if (StringUtils.isEmpty(point.getTag_id()) || StringUtils.isEmpty(point.getTag_name())
				|| StringUtils.isEmpty(point.getType()) || Objects.isNull(point.getTimestamp())
				|| StringUtils.isEmpty(point.getValue()) || "null".equals(point.getValue())
				|| Objects.isNull(point.getQuality()) || Objects.isNull(point.getError_code())) {
			return false;
		} else {
			return true;
		}
	}

}
