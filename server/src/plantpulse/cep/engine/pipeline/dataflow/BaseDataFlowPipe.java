package plantpulse.cep.engine.pipeline.dataflow;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.json.JSONObject;

import plantpulse.buffer.db.DBDataPoint;
import plantpulse.cep.engine.dds.DataDistributionService;
import plantpulse.cep.engine.monitoring.metric.MetricNames;
import plantpulse.cep.engine.monitoring.metric.MonitoringMetrics;
import plantpulse.cep.engine.pipeline.DataFlowPipe;
import plantpulse.cep.engine.pipeline.dataflow.adjuster.DataFlowAdjuster;
import plantpulse.cep.engine.storage.StorageProcessor;
import plantpulse.cep.engine.stream.StreamProcessor;
import plantpulse.event.opc.Point;

public class BaseDataFlowPipe implements DataFlowPipe {

	private static Log log = LogFactory.getLog(ParallelDataFlowPipe.class);

	private DataFlowAdjuster data_flow_adjuster;
	//
	private StreamProcessor stream_prcoessor;
	private StorageProcessor storage_processor;
	//
	private DataDistributionService dds;

	//
	public BaseDataFlowPipe(DataFlowAdjuster data_flow_adjuster, StreamProcessor stream_processor,
			StorageProcessor storage_processor, DataDistributionService dds) {
		super();

		this.data_flow_adjuster = data_flow_adjuster;
		this.stream_prcoessor = stream_processor;
		this.storage_processor = storage_processor;
		this.dds = dds;

		//
		log.info("Message data flow initiallized.");
	}

	@Override
	public void init() {
		MonitoringMetrics.getCounter(MetricNames.DATAFLOW_WORKER_THREAD).inc();
	}

	@Override
	public void flow(Point point) throws Exception {
		// 1. 검증
		if (!data_flow_adjuster.validate(point)) {
			MonitoringMetrics.getMeter(MetricNames.DATAFLOW_VALIDATE_FAILED).mark();
			log.warn("Point validate failed : " + point.toString());
		} else {

			// 2. 스트리밍
			point = data_flow_adjuster.beforeStream(point);
			stream_prcoessor.publish(point);

			// 3. 스토리지
			point = data_flow_adjuster.beforeStore(point);
			storage_processor.getBuffer().add(new DBDataPoint(point));

			// 4. 데이터 배포
			dds.sendTagPoint(point.getTag_id(), JSONObject.fromObject(point));
		}
		;

	}

	@Override
	public void close() {
		//
		MonitoringMetrics.getCounter(MetricNames.DATAFLOW_WORKER_THREAD).dec();
	}

}
