package plantpulse.cep.engine.async;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

/**
 * <B>AsynchronousExecutor</B>
 * 
 * 
 * @author leesangboo
 * 
 */
public class AsynchronousExecutor {
	
	private static final Log log = LogFactory.getLog(AsynchronousExecutor.class);
	
	
	private static final long INFO_MS    = 10 * 1000;
	private static final long WARNING_MS = 60 * 1000;


	
	/**
	 * 비동기로 워커를 실행한다.
	 * 
	 * @param worker
	 * @param delay_ms
	 */
	public void execute(final Worker worker) {
		
		//
		AsynchronousExecutorPool.getInstance().getPool().execute(new Runnable(){
			@Override
			public void run() {
				try {
					AsynchronousExecutorPool.getInstance().addWorker(worker);
					//
					long start = System.currentTimeMillis();
					worker.execute();
					long run_time_ms = System.currentTimeMillis() - start;
					
					if(run_time_ms >= WARNING_MS) {
						log.warn("Asynchronous worker takes long to run : name=[" + worker.getName() + "], run_time_ms=[" + run_time_ms + "], warnning_time_ms=[" + WARNING_MS + "]");
					}else if(run_time_ms <  WARNING_MS && run_time_ms >= INFO_MS) {
						log.info("Long run asynchronous workers logging : name=[" + worker.getName() + "], run_time_ms=[" + run_time_ms + "]" );
					};
				//
				}catch(Exception ex) {
					log.error("Worker exectue failed : " + ex.getMessage(), ex);
				}finally {
					//keep alive time 으로 제어
					//Thread.currentThread().interrupt();
					//	
					AsynchronousExecutorPool.getInstance().removeWorker(worker);
				}
			}
		});
    };
    
}
