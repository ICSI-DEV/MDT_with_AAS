package plantpulse.cep.engine.messaging.listener;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.json.JSONObject;

import plantpulse.cep.engine.messaging.backup.TimeoutMessageBackup;
import plantpulse.cep.engine.pipeline.DataFlowPipe;
import plantpulse.cep.engine.stream.processor.PointStreamStastics;
import plantpulse.event.opc.Point;

/**
 * MessageProcessEnable
 * 
 * @author leesa
 *
 */
abstract public class MessageProcessEnable {
	
	
	private static final Log log = LogFactory.getLog(MessageProcessEnable.class);
	
	
	protected void enable(Point point, TimeoutMessageBackup backup, DataFlowPipe dataflow) throws Exception {
 		//
			long latency = (System.currentTimeMillis() - point.getTimestamp());
			boolean IS_WARNING  = (latency >= PointStreamStastics.MESSAGING_LATENCY_WARNING_MS.get());
			boolean IS_TIMEOVER = (latency >= PointStreamStastics.MESSAGING_LATENCY_TIMEOUT_MS.get());
			if(IS_WARNING) {
				log.debug("Delay in receiving a message : tag_id=[" + point.getTag_id() + "], latency=[" + latency + "/" + PointStreamStastics.MESSAGING_LATENCY_WARNING_MS.get() + "]");
			}
			if(IS_TIMEOVER ) {
				backup.storeTimeoutMessage(JSONObject.fromObject(point).toString());
			}else { 
				dataflow.flow(point);
			}
	}

}
