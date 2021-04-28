package plantpulse.cep.engine.stream.type.direct;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import plantpulse.cep.engine.stream.StreamProcessor;
import plantpulse.cep.engine.stream.processor.PointStreamProcessor;
import plantpulse.event.opc.Point;

/**
 * PointDirectProcessor
 * @author leesa
 *
 */
public class PointDirectProcessor  implements StreamProcessor {

	private static final Log log = LogFactory.getLog(PointDirectProcessor.class);

	//
	private PointStreamProcessor processor = new PointStreamProcessor();
	
	@Override
	public void start() throws Exception {
		log.info("Point direct processor started.");
	}

	@Override
	public void publish(Point point) {
		try {
			processor.process(point);
		} catch (Exception e) {
			log.error("Point direct process error : " + e.getMessage(), e);
		}
	}

	@Override
	public void stop() {
		processor = null;
	}

}
