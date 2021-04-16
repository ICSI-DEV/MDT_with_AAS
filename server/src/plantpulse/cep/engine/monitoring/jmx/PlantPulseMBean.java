package plantpulse.cep.engine.monitoring.jmx;

/**
 * PlantPulseMBean
 * 
 * @author leesa
 *
 */
public interface PlantPulseMBean {
	
	
	//제품 정보
	public String getPRODUCT_NAME();
	public String getPRODUCT_VERSION(); 
	
	public long getSERVER_START_TIMESTAMP(); 
	public long getSERVER_START_DURATION(); 
	public String getSERVER_START_DATE_TEXT(); 
	
	//
	public long getSYSTEM_CPU_USED_PERCENT();
	public long getSYSTEM_MEMORY_USED_PERCENT();
	
    //자바
	public long getJAVA_HEAP_MEMORY_FREE_SIZE();
	public long getJAVA_HEAP_MEMORY_TOTAL_SIZE() ;
	public long getJAVA_HEAP_MEMORY_USED_SIZE();
	
	//네트워크 정보
	public double getSTREAM_NETWORK_LATENCY_MIN(); 
	public double getSTREAM_NETWORK_LATENCY_MAX(); 
	public double getSTREAM_NETWORK_LATENCY_AVG();
	
	//메세징
	public long   getMESSAGING_TOTAL_IN_COUNT();
	public long   getMESSAGING_TOTAL_IN_BYTES() ;
	public long   getMESSAGING_TOTAL_IN_COUNT_BY_KAFKA();
	public long   getMESSAGING_TOTAL_IN_COUNT_BY_MQTT();
	public long   getMESSAGING_TOTAL_IN_COUNT_BY_STOMP();
	public long   getMESSAGING_TOTAL_IN_COUNT_BY_HTTP();
	public double getMESSAGING_TOTAL_IN_RATE_1_SECOND();
	public double getMESSAGING_TOTAL_IN_RATE_1_MINUTE();
	
	//메세징-타임아웃-백업
	public long   getMESSAGING_BACKUPED_TOTAL_COUNT();
	public long   getMESSAGING_BACKUPED_REPROCESS_COUNT();
	public long   getMESSAGING_BACKUPED_REMAINING_COUNT();


	//스트리밍 정보 ()
	public long   getSTREAM_TIMEOVER_COUNT(); 
	public long   getSTREAM_TIMEOVER_MS();
	public String getSTREAM_TIMEOVER_POINT_LIST(); 
	public long   getSTREAM_PROCESSED_COUNT(); 
	public long   getSTREAM_PROCESSED_TIME(); 
	public double getSTREAM_PROCESSED_COUNT_BY_SEC(); 
	public long   getSTREAM_PENDING_QUEUE_COUNT(); 
	public long   getSTREAM_RUNNING_THREAD();  
	public long   getSTREAM_REMAINING_CAPACITY();
	
	//스토리지 배치 ()
	public long   getSTORAGE_PENDING_DATA_SIZE_IN_BUFFER();
	public long   getSTORAGE_ACTIVE_TIMER();
	public long   getSTORAGE_ACTIVE_BATCH();
	public long   getSTORAGE_PROCESS_CURRENT_DATA();
	public long   getSTORAGE_PROCESS_TOTAL_DATA();
	public long   getSTORAGE_TOTAL_SAVED_TAG_DATA_COUNT();
	public double getSTORAGE_WRITE_PER_SEC_THREAD();
	
	//비동기 스레드 풀
	public long   getASYNC_EXEC_POOL_SIZE();
	public long   getASYNC_EXEC_CORE_POOL_SIZE();
	public long   getASYNC_EXEC_MAXIMUM_POOL_SIZE();
	public long   getASYNC_EXEC_ACTIVE_COUNT();
	public long   getASYNC_EXEC_COMPLETED_TASK_COUNT();
	public long   getASYNC_EXEC_TASK_COUNT();
	public long   getASYNC_EXEC_PENDING_TASK_SIZE();
	public String getASYNC_EXEC_IS_BUSY();
	

};

