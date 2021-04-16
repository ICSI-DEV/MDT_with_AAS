package plantpulse.cep.engine.storage.insert;

import java.util.concurrent.RejectedExecutionHandler;
import java.util.concurrent.ThreadPoolExecutor;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

public class BatchWorkerRejectedHandler implements RejectedExecutionHandler {

	private static Log log = LogFactory.getLog(BatchWorkerRejectedHandler.class);

	@Override
	public void rejectedExecution(Runnable r, ThreadPoolExecutor executor) {
		log.error(
				"Batch worker is rejected. Active threads are piling up time is too long to continue the batch processing thread pool. " + "Increase value of 'storage_batch_executor_pool_max_size'");
	}

};