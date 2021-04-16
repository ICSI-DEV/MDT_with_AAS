package plantpulse.cep.engine.monitoring.metric;

import java.util.concurrent.TimeUnit;

import com.codahale.metrics.Counter;
import com.codahale.metrics.Histogram;
import com.codahale.metrics.JmxReporter;
import com.codahale.metrics.Meter;
import com.codahale.metrics.MetricRegistry;
import com.codahale.metrics.SlidingTimeWindowReservoir;
import com.codahale.metrics.Timer;


/**
 * PipelineMetricRegistry
 * 
 * @author leesa
 *
 */
public class PipelineMetricRegistry {

	private MetricRegistry registry = null;
	private JmxReporter reporter = null;
	
	/**
	 * init
	 */
	public void init() {
	
		registry = new MetricRegistry();
		
		//--------------------------------------------------------------------
		//0. 네트워크
		//--------------------------------------------------------------------
		registry.register(MetricNames.NETWORK_LATENCY,  new Histogram(new SlidingTimeWindowReservoir(1, TimeUnit.MINUTES)));
		
		//--------------------------------------------------------------------
		//1. 메세징
		//--------------------------------------------------------------------
		registry.register(MetricNames.MEESSAGE_LATENCY,  new Histogram(new SlidingTimeWindowReservoir(1, TimeUnit.MINUTES)));
		registry.register(MetricNames.MESSAGE_IN,        new Meter());
		registry.register(MetricNames.MESSAGE_IN_KAFKA,  new Meter());
		registry.register(MetricNames.MESSAGE_IN_MQTT,   new Meter());
		registry.register(MetricNames.MESSAGE_IN_STOMP,  new Meter());
		registry.register(MetricNames.MESSAGE_OUT,       new Meter());
		registry.register(MetricNames.MESSAGE_OUT_KAFKA, new Meter());
		registry.register(MetricNames.MESSAGE_OUT_MQTT,  new Meter());
		registry.register(MetricNames.MESSAGE_OUT_STOMP, new Meter());
		registry.register(MetricNames.MESSAGE_TIMEOUT,   new Meter());
		registry.register(MetricNames.MESSAGE_TIMEOUT_RE_PROCESSED,      new Meter());
		registry.register(MetricNames.MESSAGE_TIMEOUT_RE_PROCESSED_TIME, new Timer());
		
		//--------------------------------------------------------------------
		//2. 데이터 플로우
		//--------------------------------------------------------------------
		registry.register(MetricNames.DATAFLOW_WORKER_THREAD,    new Counter());
		registry.register(MetricNames.DATAFLOW_VALIDATE_FAILED,  new Meter());
		
		//--------------------------------------------------------------------
		//3. 스트림(CEP)
		//--------------------------------------------------------------------
		registry.register(MetricNames.STREAM_PROCESSED,       new Meter());
		registry.register(MetricNames.STREAM_PROCESSED_TIME,  new Timer());
		registry.register(MetricNames.STREAM_TIMEOVER,        new Meter());
		
		//--------------------------------------------------------------------
		//4. 스토리지 버퍼
		//--------------------------------------------------------------------
		registry.register(MetricNames.STORAGE_DB_BUFFER,                new Counter());
		registry.register(MetricNames.STORAGE_DB_BUFFER_QUEUE_PEDDING,  new Counter());
		
		//--------------------------------------------------------------------
		//5. 데이터 배포 서비스
		//--------------------------------------------------------------------
		registry.register(MetricNames.DDS_PROCESSED,       new Meter());
		registry.register(MetricNames.DDS_PROCESSED_TYPE_TAG_POINT,   new Meter());
		registry.register(MetricNames.DDS_PROCESSED_TYPE_TAG_ALARM,   new Meter());
		registry.register(MetricNames.DDS_PROCESSED_TYPE_ASSET_DATA,   new Meter());
		registry.register(MetricNames.DDS_PROCESSED_TYPE_ASSET_ALARM,  new Meter());
		registry.register(MetricNames.DDS_PROCESSED_TYPE_ASSET_EVENT,  new Meter());
		registry.register(MetricNames.DDS_PROCESSED_TYPE_ASSET_CONTEXT,  new Meter());
		registry.register(MetricNames.DDS_PROCESSED_TYPE_ASSET_AGGREGATION,  new Meter());
		
		//콘솔 리포터
		reporter = JmxReporter.forRegistry(registry).inDomain("plantpulse.cep").build();
		reporter.start();

	}
	
	/**
	 * getReporter
	 */
	public JmxReporter getReporter() {
		return reporter;
	}
	
	/**
	 * getRegistry
	 * @return
	 */
	public MetricRegistry getRegistry() {
	  return registry;	
	}
	
	/**
	 * close
	 */
	public void close() {
		//
		reporter.stop();
	}
	
}
