package plantpulse.cep.engine.stream.type.queue;

import java.util.List;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.redisson.api.RQueue;

import plantpulse.cep.engine.stream.processor.PointStreamProcessor;
import plantpulse.cep.engine.stream.queue.cep.CEPDataPointQueue;
import plantpulse.cep.engine.stream.type.distruptor.PointDistruptorable;

/**
 * PointQueueTaskThread
 * @author lsb
 * 
 */
public class PointQueueTaskThread implements Runnable {

	private static Log log = LogFactory.getLog(PointQueueTaskThread.class);
	
	private PointStreamProcessor processor = new PointStreamProcessor();
	
	private CEPDataPointQueue<RQueue<PointDistruptorable>> queue = null;
	
	private static final int POLL_COUNT = 2; //천개씩 프로세싱
	
	@Override
	public void run() {
		
		queue = PointQueueFactory.getCEPDataPointQueue("STREAM_CEP_QUEUE_TASK");
		
		while (true) {
			try {
				List<PointDistruptorable> tag_queue_list = queue.getQueue().poll(POLL_COUNT);
				if(tag_queue_list != null && tag_queue_list.size() >  0){
					for(int i=0; i < tag_queue_list.size(); i++) {
						processor.process(tag_queue_list.get(i).getPoint());	
					}
				}else{
					 //LockSupport.parkNanos(TimeUnit.MILLISECONDS.toNanos(100));
					Thread.sleep(100);
				}
			} catch (Exception e) {
				log.error("Point queue task thread failed : " + e.getMessage());
			}finally {
				
			}
		}

	}

}