package plantpulse.cep.engine.pipeline.async;

import java.util.ArrayList;
import java.util.List;
import java.util.Timer;
import java.util.TimerTask;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.ReentrantLock;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.json.JSONObject;

import plantpulse.buffer.db.DBDataPoint;
import plantpulse.cep.engine.dds.DataDistributionService;
import plantpulse.cep.engine.monitoring.metric.MetricNames;
import plantpulse.cep.engine.monitoring.metric.MonitoringMetrics;
import plantpulse.cep.engine.pipeline.dataflow.adjuster.DataFlowAdjuster;
import plantpulse.cep.engine.storage.StorageProcessor;
import plantpulse.cep.engine.stream.StreamProcessor;
import plantpulse.event.opc.Point;

/**
 * DataPipelineWorker
 * @author leesa
 *
 */
public class DataPipelineWorker implements Runnable {
	
	private static Log log = LogFactory.getLog(DataPipelineWorker.class);

	private static final int MAX_LIMIT = 1_000_000;
	
	private static final boolean USE_DRATIN = false;
	
	private ArrayBlockingQueue<Point> queue = new ArrayBlockingQueue<Point>(MAX_LIMIT);
	
	private AtomicBoolean started = new AtomicBoolean(false);
	
	private AtomicLong processed_count = new AtomicLong(0);
	
	private ReentrantLock lock = new ReentrantLock();
	
	
	private int num;
	
	private DataFlowAdjuster data_flow_adjuster;
	private StreamProcessor  stream_processor;
	private StorageProcessor storage_processor;
	private DataDistributionService dds;
	
	private Timer monitor;
	
	public DataPipelineWorker(int num, DataFlowAdjuster data_flow_adjuster, StreamProcessor stream_processor, StorageProcessor storage_processor, DataDistributionService dds) {
		this.num = num;
		this.data_flow_adjuster = data_flow_adjuster;
		this.stream_processor = stream_processor;
		this.storage_processor = storage_processor;
		this.dds = dds;
	}

	@Override
	public void run() {
		
		//
		Thread.currentThread().setName("PP_MICRO_PIPE_LINE_WORKER-" + num);
		
		//
		started.set(true);
		
		//
		monitor = new Timer("PP_MICRO_PIPE_LINE_WORKER_MONITOR-" + num);
		monitor.scheduleAtFixedRate(new TimerTask() {
			@Override
			public void run() {
				if(queue.remainingCapacity() < (MAX_LIMIT/2)) { //큐가 반이상 사용중일때, 경고
					log.warn("DataPipelineWorker[" + num + "] 1/2 warnning : queue_size = [" + queue.size() + "/" + MAX_LIMIT + "], processed_count=[" + processed_count.get() + "]");
				};
				if(lock.isLocked() && queue.remainingCapacity() >= 1_000) {
					lock.unlock();
					log.info("DataPipelineWorker[" + num + "] queue is free, publisher now unlocked.");
				}
			}
		}, 1*100, 1*1000);
		
		log.info("DataPipelineWorker[" + num + "] started.");
		
		//
		while(started.get()) {
			
			try {
			    //
				List<Point> micro_point_list = new ArrayList<>();
				//
				if(USE_DRATIN) {
					 queue.drainTo(micro_point_list, 10);
				}else {
					 micro_point_list.add(queue.take());	
				};
				
				//
				if(micro_point_list != null && micro_point_list.size() > 0) {
					for(int i=0; i < micro_point_list.size(); i++){
						Point point = micro_point_list.get(i);
						// 1. 검증
						if (!data_flow_adjuster.validate(point)) {
							MonitoringMetrics.getMeter(MetricNames.DATAFLOW_VALIDATE_FAILED).mark();
							log.warn("Point validate failed : " + point.toString());
						}else {
							
							// 2. 스트리밍
							point = data_flow_adjuster.beforeStream(point);
							stream_processor.publish(point);
			
							// 3. 스토리지
							point = data_flow_adjuster.beforeStore(point);
							storage_processor.getBuffer().add(new DBDataPoint(point));
							
							// 4. 데이터 배포
							dds.sendTagPoint(point.getTag_id(), JSONObject.fromObject(point));
						};
						
						//
						processed_count.incrementAndGet();
					};
					micro_point_list.clear();
				}else {
					Thread.sleep(1 * 1000);
				};
				
			} catch (Exception ex) {
				log.error("Pipeline data flow worker error : " + ex.getMessage(), ex);
			} finally {
				//
			}
		};
		
	}
	
	
	public void publish(Point point) {
		queue.add(point);
		if(queue.remainingCapacity() == 0) {
			lock.lock();
			log.warn("DataPipelineWorker[" + num + "] queue is FULL, publisher now locked.");
		};
	}
	
	public void stop() {
		monitor.cancel();
		started.set(false);
		//
		MonitoringMetrics.getCounter(MetricNames.DATAFLOW_WORKER_THREAD).dec();
	}

}
