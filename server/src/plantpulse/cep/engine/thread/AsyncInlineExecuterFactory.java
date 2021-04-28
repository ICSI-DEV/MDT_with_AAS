package plantpulse.cep.engine.thread;


import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

/**
 * AsyncInlineExecuterFactory
 * @author leesa
 *
 */
public class AsyncInlineExecuterFactory {
	
	/**
	 * getExecuter()
	 * core - 2
	 * @return
	 */
	public static ExecutorService getExecutor()  {
		CommonThreadFactory factory = new CommonThreadFactory("PP-INLINE-EXEC");
		return Executors.newFixedThreadPool(Runtime.getRuntime().availableProcessors() - 2, factory);
	}

}
