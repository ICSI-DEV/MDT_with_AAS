package plantpulse.cep.engine.stream;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import plantpulse.cep.engine.properties.PropertiesLoader;
import plantpulse.cep.engine.stream.processor.PointStreamStastics;
import plantpulse.cep.engine.stream.type.direct.PointDirectProcessor;
import plantpulse.cep.engine.stream.type.distruptor.PointDistruptorProcessor;
import plantpulse.cep.engine.stream.type.queue.PointQueueProcessor;

/**
 * StreamProcessorFactory
 * 
 * @author leesa
 *
 */
public class StreamProcessorFactory {

	private static final Log log = LogFactory.getLog(StreamProcessorFactory.class);

	private static class StreamProcessorFactoryHolder {
		static StreamProcessorFactory instance = new StreamProcessorFactory();
	}

	public static StreamProcessorFactory getInstance() {
		return StreamProcessorFactoryHolder.instance;
	};

	public static final String DIRECT = "DIRECT";

	public static final String DISTRUPTOR = "DISTRUPTOR";

	public static final String QUEUE = "QUEUE";

	private StreamProcessor stream_prcoessor = null;

	/**
	 * 초기화
	 */
	public void init() {
		String type = PropertiesLoader.getEngine_properties().getProperty("engine.stream.processor", DISTRUPTOR);
		switch (type) {
		case DIRECT:
			stream_prcoessor = new PointDirectProcessor();
			break;
		case DISTRUPTOR:
			stream_prcoessor = new PointDistruptorProcessor();
			break;
		case QUEUE:
			stream_prcoessor = new PointQueueProcessor();
			break;
		default:
			stream_prcoessor = new PointDirectProcessor();
			break;
		};
		
		log.info("Stream processor inited : type=["+type+"], class_name=[" + stream_prcoessor.getClass().getName() + "]");
		
		// 스트리밍 타임아웃 값 설정 ----------------------------------------------------------------------------------------------
		long PROP_MESSAGING_WARNING_MS = Long.parseLong(PropertiesLoader.getEngine_properties().getProperty("engine.streaming.messaging.warning.ms",   StreamConstants.DEFAULT_MESSAGING_WARNING_MS + ""));
		long PROP_MESSAGING_TIMEOVER_MS = Long.parseLong(PropertiesLoader.getEngine_properties().getProperty("engine.streaming.messaging.timeover.ms", StreamConstants.DEFAULT_MESSAGING_TIMEOVER_MS + ""));
		long PROP_CEP_TIMEOVER_MS = Long.parseLong(PropertiesLoader.getEngine_properties().getProperty("engine.streaming.cep.timeover.ms",  StreamConstants.DEFAULT_CEP_TIMEOVER_MS + ""));

		//
		PointStreamStastics.MESSAGING_LATENCY_WARNING_MS.set(PROP_MESSAGING_WARNING_MS);
		PointStreamStastics.MESSAGING_LATENCY_TIMEOUT_MS.set(PROP_MESSAGING_TIMEOVER_MS);
		PointStreamStastics.CEP_TIMEOVER_MS.set(PROP_CEP_TIMEOVER_MS);;
		// 스트리밍 타임아웃 값 설정 ----------------------------------------------------------------------------------------------

		log.info("Point streaming timeout setting : messaging=[" +PointStreamStastics. MESSAGING_LATENCY_WARNING_MS + "|" + PointStreamStastics.MESSAGING_LATENCY_TIMEOUT_MS + "]ms, CEP=[" + PointStreamStastics.CEP_TIMEOVER_MS + "]ms");
	}

	/**
	 * 
	 * @return
	 */
	public StreamProcessor getStreamProcessor() {
		return stream_prcoessor;
	}
	

}
