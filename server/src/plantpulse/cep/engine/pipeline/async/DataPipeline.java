package plantpulse.cep.engine.pipeline.async;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicLong;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import plantpulse.cep.engine.dds.DataDistributionService;
import plantpulse.cep.engine.monitoring.metric.MetricNames;
import plantpulse.cep.engine.monitoring.metric.MonitoringMetrics;
import plantpulse.cep.engine.pipeline.dataflow.adjuster.DataFlowAdjuster;
import plantpulse.cep.engine.pipeline.dataflow.strategy.RoundRobinStrategy;
import plantpulse.cep.engine.storage.StorageProcessor;
import plantpulse.cep.engine.stream.StreamProcessor;
import plantpulse.event.opc.Point;

/**
 * DataPipeline
 * 
 * @author leesa
 *
 */
public abstract class DataPipeline {

	private static Log log = LogFactory.getLog(DataPipeline.class);
	
	private static final int WORKER_COUNT = 4;
	
	private ExecutorService executor;
	
	private RoundRobinStrategy round_robin;

	private List<DataPipelineWorker> worker_list;
	
	private AtomicLong flow_in_count;
	
	
	public DataPipeline() {
	   //
	}
	
	/**
	 * 데이터 파이프라인 초기화 (라운드 로빈 방식)
	 */
	public void init() {
		//
		executor = Executors.newFixedThreadPool(WORKER_COUNT);
		round_robin = new RoundRobinStrategy(WORKER_COUNT);
		worker_list = new ArrayList<DataPipelineWorker>();
		flow_in_count = new AtomicLong(0);
		
		for(int i=0; i < WORKER_COUNT; i++) {
			DataPipelineWorker worker = new DataPipelineWorker(i, getAdjuster(), getStreamProcessor(), getStorageProcessor(), getDDS());
			executor.execute(worker);
			worker_list.add(worker);
			//
			MonitoringMetrics.getCounter(MetricNames.DATAFLOW_WORKER_THREAD).inc();
		}
		
		log.info("DataPipeline initialized with round robin flow process : worker_thread_count=[" +  WORKER_COUNT + "]");
	}

	/**
	 * 데이터 파이프라인 흐름 
	 * @param point
	 * @throws Exception
	 */
	public void flow(Point point) throws Exception {
		worker_list.get(round_robin.index()).publish(point);
		flow_in_count.incrementAndGet();
	};

	public abstract DataFlowAdjuster getAdjuster();

	public abstract StreamProcessor getStreamProcessor();

	public abstract StorageProcessor getStorageProcessor();
	
	public abstract DataDistributionService getDDS();
	
	/**
	 * 닫기
	 */
	public void close() {
		for(int i=0; i < worker_list.size(); i++) {
			worker_list.get(i).stop();
		};
		executor.shutdown();
	}


}
