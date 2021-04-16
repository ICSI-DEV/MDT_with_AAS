package plantpulse.cep.engine.stream.processor;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.json.JSONObject;

import com.codahale.metrics.Meter;
import com.codahale.metrics.Timer;

import plantpulse.cep.engine.CEPEngineManager;
import plantpulse.cep.engine.dds.DataDistributionService;
import plantpulse.cep.engine.monitoring.metric.MetricNames;
import plantpulse.cep.engine.monitoring.metric.MonitoringMetrics;
import plantpulse.cep.engine.snapshot.PointMapFactory;
import plantpulse.event.opc.Point;

/**
 * PointStreamProcessor
 * 
 * @author lsb
 *
 */
public class PointStreamProcessor {

	private static final Log log = LogFactory.getLog(PointStreamProcessor.class);

	public void process(final Point point) throws Exception {
		final Timer.Context context = MonitoringMetrics.getTimer(MetricNames.STREAM_PROCESSED_TIME).time();
		try {

			//
			long start = System.currentTimeMillis();
			PointStreamStastics.RUNNING_THREAD.incrementAndGet();

			// 스트리밍 타임오버 값보다 지난 이벤트이면 CEP 및 PUSH 에서 제거 (저장 처리)
			if ((System.currentTimeMillis() - point.getTimestamp()) <= PointStreamStastics.CEP_TIMEOVER_MS.get()) {
				// CEP 소비 및 MQ 리 퍼블리시
				final CEPEngineManager manager = CEPEngineManager.getInstance();
				if (manager.getProvider() != null && !manager.getProvider().isDestroyed()) {
					manager.getProvider().getEPRuntime().sendEvent(point);
				}
			} else {
				//
				MonitoringMetrics.getMeter(MetricNames.STREAM_TIMEOVER).mark();
				//
				PointStreamStastics.TIMEOVER_COUNT.incrementAndGet();
				if (!PointStreamStastics.TIMEOVER_POINT_LIST.contains(point.getTag_id())) {
					PointStreamStastics.TIMEOVER_POINT_LIST.add(point.getTag_id());
				}
				;
			}

			// PointMap 등록
			PointMapFactory.getInstance().getPointMap().put(point.getTag_id(), point);

			long end = System.currentTimeMillis() - start;
			//
			PointStreamStastics.PROCESSED_COUNT.incrementAndGet();
			PointStreamStastics.PROCESSED_TIME.addAndGet(end);
			PointStreamStastics.RUNNING_THREAD.decrementAndGet();

		} catch (Exception ex) {
			log.error("Point stream processing error : " + ex.getMessage());
			throw ex;
		} finally {
			//
			context.stop();
			MonitoringMetrics.getMeter(MetricNames.STREAM_PROCESSED).mark();
		}
	}

}
