package plantpulse.cep.engine.messaging.listener.http;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.json.JSONObject;

import plantpulse.cep.engine.CEPEngineManager;
import plantpulse.cep.engine.async.AsynchronousExecutorStatus;
import plantpulse.cep.engine.messaging.backup.TimeoutMessageBackup;
import plantpulse.cep.engine.messaging.listener.MessageListenerStatus;
import plantpulse.cep.engine.messaging.listener.MessageProcessEnable;
import plantpulse.cep.engine.messaging.unmarshaller.MessageUnmarshaller;
import plantpulse.cep.engine.pipeline.DataFlowPipe;
import plantpulse.cep.engine.stream.processor.PointStreamStastics;
import plantpulse.event.opc.Point;

public class HTTPTypeBaseListener extends MessageProcessEnable {

	private static final Log log = LogFactory.getLog(HTTPTypeBaseListener.class);

	private MessageUnmarshaller unmarshaller;
	//
	private DataFlowPipe dataflow = CEPEngineManager.getInstance().getData_flow_pipe();
	
	private TimeoutMessageBackup backup = new TimeoutMessageBackup();

	public HTTPTypeBaseListener( MessageUnmarshaller unmarshaller) {
		super();
		this.unmarshaller = unmarshaller;
	}

	public void onEvent(String query) throws Exception {
		try {
			
			//
			if(CEPEngineManager.getInstance().isShutdown()) {
				return;
			}
			
			MessageListenerStatus.TOTAL_IN_COUNT.incrementAndGet();
			MessageListenerStatus.TOTAL_IN_BYTES.addAndGet(query.getBytes().length);
    		MessageListenerStatus.TOTAL_IN_COUNT_BY_HTTP.incrementAndGet();

			JSONObject event = JSONObject.fromObject(query);
			Point point = unmarshaller.unmarshal(event);
			//
			super.enable(point ,backup, dataflow);
		} catch (Exception e) {
			log.error("HTTPTypeBaseListener message receive error : " + e.getMessage(), e);
		}
	}

}
