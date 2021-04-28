package plantpulse.cep.engine.messaging.listener.kafka;

import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.ReentrantLock;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import plantpulse.cep.engine.CEPEngineManager;
import plantpulse.cep.engine.messaging.listener.MessageListenerStatus;


/**
 * KafkaConsumerRunnerForPointTask
 * @author leesa
 *
 */
public class KafkaConsumerRunnerForPointTask implements Runnable {

    private final List<ConsumerRecord<String, String>> records;

    private volatile boolean stopped = false;

    private volatile boolean started = false;

    private volatile boolean finished = false;

    private final CompletableFuture<Long> completion = new CompletableFuture<>();

    private final ReentrantLock startStopLock = new ReentrantLock();

    private final AtomicLong currentOffset = new AtomicLong();
    
    //
    private KafkaTypeBaseListener listener ;

    private Logger log = LoggerFactory.getLogger(KafkaConsumerRunnerForPointTask.class);

    public KafkaConsumerRunnerForPointTask(List<ConsumerRecord<String, String>> records, KafkaTypeBaseListener listener ) {
        this.records = records;
        this.listener = listener;
    }


    public void run() {
    	
    	//
    	if(CEPEngineManager.getInstance().isShutdown()) {
			return;
		};
    	
    	//
        startStopLock.lock();
        if (stopped){
            return;
        }
        started = true;
        startStopLock.unlock();

        for (ConsumerRecord<String, String> record : records) {
            if (stopped) break;
            
            // process record here and make sure you catch all exceptions;
				try {
					//
					MessageListenerStatus.TOTAL_IN_COUNT.incrementAndGet();
					MessageListenerStatus.TOTAL_IN_BYTES.addAndGet(record.value().getBytes().length);
					MessageListenerStatus.TOTAL_IN_COUNT_BY_KAFKA.incrementAndGet();
					//
					listener.onEvent(record.value());
				} catch (Exception e) {
					log.error("KafkaMessageListener point type message listen error : " + e.getMessage(), e);
				};
				
			//
            currentOffset.set(record.offset() + 1);
        }
        finished = true;
        completion.complete(currentOffset.get());
    }

    public long getCurrentOffset() {
        return currentOffset.get();
    }

    public void stop() {
        startStopLock.lock();
        this.stopped = true;
        if (!started) {
            finished = true;
            completion.complete(currentOffset.get());
        }
        startStopLock.unlock();
    }

    public long waitForCompletion() {
        try {
            return completion.get();
        } catch (InterruptedException | ExecutionException e) {
            return -1;
        }
    }

    public boolean isFinished() {
        return finished;
    }

}