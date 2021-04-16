package plantpulse.cep.engine.monitoring.jmx;

import java.text.SimpleDateFormat;

import plantpulse.Metadata;
import plantpulse.cep.engine.CEPEngineManager;
import plantpulse.cep.engine.async.AsynchronousExecutorStatus;
import plantpulse.cep.engine.messaging.listener.MessageListenerStatus;
import plantpulse.cep.engine.monitoring.event.OSPerformance;
import plantpulse.cep.engine.network.Latency;
import plantpulse.cep.engine.os.OSPerformanceUtils;
import plantpulse.cep.engine.storage.StorageStatusUtils;
import plantpulse.cep.engine.stream.processor.PointStreamStastics;

/**
 * PlantPulse
 * 	
 * 
 * @author leesa
 *
 */
public class PlantPulse implements PlantPulseMBean {
	
	
	@Override
	public String getPRODUCT_NAME() {
		return Metadata.PRODUCT_NAME;
	}
	
	@Override
	public String getPRODUCT_VERSION() {
		return Metadata.PRODUCT_VERISON;
	}
	
	@Override
	public long getSYSTEM_CPU_USED_PERCENT() {
		OSPerformance performance = OSPerformanceUtils.getOSPerformance();
		return Math.round(performance.getUsed_cpu_percent());
	}
	
	@Override
	public long getSYSTEM_MEMORY_USED_PERCENT() {
		OSPerformance performance = OSPerformanceUtils.getOSPerformance();
		return Math.round(performance.getUsed_memory_percent());
	}

	@Override
	public long getJAVA_HEAP_MEMORY_FREE_SIZE() {
		return Runtime.getRuntime().freeMemory();
	}
	@Override
	public long getJAVA_HEAP_MEMORY_TOTAL_SIZE() {
		return Runtime.getRuntime().totalMemory();
	}
	@Override
	public long getJAVA_HEAP_MEMORY_USED_SIZE() {
		return Runtime.getRuntime().totalMemory() - Runtime.getRuntime().freeMemory();
	}

	
	@Override
	public long getSTREAM_TIMEOVER_MS() {
		return PointStreamStastics.CEP_TIMEOVER_MS.get();
	}

	@Override
	public long getSTREAM_TIMEOVER_COUNT() {
		return PointStreamStastics.TIMEOVER_COUNT.get();
	}

	@Override
	public String getSTREAM_TIMEOVER_POINT_LIST() {
		return PointStreamStastics.TIMEOVER_POINT_LIST.toString();
	}

	@Override
	public long getSTREAM_PROCESSED_COUNT() {
		return PointStreamStastics.PROCESSED_COUNT.get();
	}

	@Override
	public long getSTREAM_PROCESSED_TIME() {
		return PointStreamStastics.PROCESSED_TIME.get();
	}
	
	@Override
	public double getSTREAM_PROCESSED_COUNT_BY_SEC() {
		return PointStreamStastics.PROCESSED_COUNT_BY_SEC.get();
	}
	
	@Override
	public long getSTREAM_PENDING_QUEUE_COUNT() {
		return PointStreamStastics.PENDING_QUEUE_COUNT.get();
	}

	@Override
	public long getSTREAM_REMAINING_CAPACITY() {
		return PointStreamStastics.REMAINING_CAPACITY.get();
	}

	@Override
	public long getSTREAM_RUNNING_THREAD() {
		return PointStreamStastics.RUNNING_THREAD.get();
	}

	@Override
	public long getSTORAGE_ACTIVE_TIMER() {
		return StorageStatusUtils.now().getActive_timer();
	}

	@Override
	public long getSTORAGE_ACTIVE_BATCH() {
		return  StorageStatusUtils.now().getActive_batch();
	}

	@Override
	public long getSTORAGE_PROCESS_CURRENT_DATA() {
		return  StorageStatusUtils.now().getProcess_current_data();
	}

	@Override
	public long getSTORAGE_PROCESS_TOTAL_DATA() {
		return StorageStatusUtils.now().getProcess_total_data();
	}

	@Override
	public long getSTORAGE_TOTAL_SAVED_TAG_DATA_COUNT() {
		return StorageStatusUtils.now().getTotal_saved_tag_data_count() + StorageStatusUtils.now().getProcess_total_data();
	}

	@Override
	public double getSTORAGE_WRITE_PER_SEC_THREAD() {
		return StorageStatusUtils.now().getWrite_per_sec_thread();
	}

	@Override
	public long getSTORAGE_PENDING_DATA_SIZE_IN_BUFFER() {
		return StorageStatusUtils.now().getPending_data_size_in_buffer();
	}

	@Override
	public long getASYNC_EXEC_POOL_SIZE() {
		return AsynchronousExecutorStatus.POOL_SIZE;
	}

	@Override
	public long getASYNC_EXEC_CORE_POOL_SIZE() {
		return AsynchronousExecutorStatus.CORE_POOL_SIZE;
	}

	@Override
	public long getASYNC_EXEC_MAXIMUM_POOL_SIZE() {
		return AsynchronousExecutorStatus.MAXIMUM_POOL_SIZE;
	}

	@Override
	public long getASYNC_EXEC_ACTIVE_COUNT() {
		return AsynchronousExecutorStatus.ACTIVE_COUNT;
	}

	@Override
	public long getASYNC_EXEC_COMPLETED_TASK_COUNT() {
		return AsynchronousExecutorStatus.COMPLETED_TASK_COUNT;
	}

	@Override
	public long getASYNC_EXEC_TASK_COUNT() {
		return AsynchronousExecutorStatus.TASK_COUNT;
	}

	@Override
	public long getASYNC_EXEC_PENDING_TASK_SIZE() {
		return AsynchronousExecutorStatus.PENDING_TASK_SIZE;
	}
	
	@Override
	public String getASYNC_EXEC_IS_BUSY() {
		return AsynchronousExecutorStatus.IS_BUSY ? "BUSY" : "NORMAL";
	}

	@Override
	public long getMESSAGING_TOTAL_IN_COUNT() {
		return MessageListenerStatus.TOTAL_IN_COUNT.get();
	}
	
	public long getMESSAGING_TOTAL_IN_BYTES() {
		return MessageListenerStatus.TOTAL_IN_BYTES.get();
	}
	
	@Override
	public long getMESSAGING_TOTAL_IN_COUNT_BY_KAFKA() {
		return MessageListenerStatus.TOTAL_IN_COUNT_BY_KAFKA.get();
	}

	@Override
	public long getMESSAGING_TOTAL_IN_COUNT_BY_MQTT() {
		return MessageListenerStatus.TOTAL_IN_COUNT_BY_MQTT.get();
	}

	@Override
	public long getMESSAGING_TOTAL_IN_COUNT_BY_STOMP() {
		return MessageListenerStatus.TOTAL_IN_COUNT_BY_STOMP.get();
	}

	@Override
	public long getMESSAGING_TOTAL_IN_COUNT_BY_HTTP() {
		return MessageListenerStatus.TOTAL_IN_COUNT_BY_HTTP.get();
	}

	@Override
	public long getMESSAGING_BACKUPED_TOTAL_COUNT() {
		return MessageListenerStatus.TIMEOUT_BACKUPED_TOTAL_COUNT.get();
	}

	@Override
	public long getMESSAGING_BACKUPED_REPROCESS_COUNT() {
		return MessageListenerStatus.TIMEOUT_BACKUPED_REPROCESS_COUNT.get();
	}
	@Override
	public long getMESSAGING_BACKUPED_REMAINING_COUNT() {
		return MessageListenerStatus.TIMEOUT_BACKUPED_REMAINING_COUNT.get();
	}
	
	@Override
	public double getMESSAGING_TOTAL_IN_RATE_1_SECOND() {
		return MessageListenerStatus.TOTAL_IN_RATE_1_SEC.get();
	}

	@Override
	public double getMESSAGING_TOTAL_IN_RATE_1_MINUTE() {
		return MessageListenerStatus.TOTAL_IN_RATE_1_MIN.get();
	}



	@Override
	public double getSTREAM_NETWORK_LATENCY_MIN() {
		return Latency.MIN.get();
	}

	@Override
	public double getSTREAM_NETWORK_LATENCY_MAX() {
		return Latency.MAX.get();
	}
	
	@Override
	public double getSTREAM_NETWORK_LATENCY_AVG(){
		return Latency.AVG.get();
	}

	@Override
	public long getSERVER_START_TIMESTAMP() {
		return CEPEngineManager.getInstance().getStartedDate().getTime();
	}

	@Override
	public long getSERVER_START_DURATION() {
		return System.currentTimeMillis() - CEPEngineManager.getInstance().getStartedDate().getTime();
	}

	@Override
	public String getSERVER_START_DATE_TEXT() {
        SimpleDateFormat f = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
		return f.format(CEPEngineManager.getInstance().getStartedDate());
	}

	
}
