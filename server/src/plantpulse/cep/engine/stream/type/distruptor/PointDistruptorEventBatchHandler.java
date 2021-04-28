package plantpulse.cep.engine.stream.type.distruptor;

import com.lmax.disruptor.Sequence;
import com.lmax.disruptor.SequenceReportingEventHandler;

import plantpulse.cep.engine.stream.processor.PointStreamProcessor;

public class PointDistruptorEventBatchHandler implements SequenceReportingEventHandler<PointDistruptorable>
{
	
	private PointStreamProcessor processor = new PointStreamProcessor();
	
    private Sequence sequenceCallback;
    private int batchRemaining = 20;

    @Override
    public void setSequenceCallback(Sequence sequenceCallback)
    {
        this.sequenceCallback = sequenceCallback;
    }

    @Override
    public void onEvent(PointDistruptorable event, long sequence, boolean endOfBatch)
    {
        processEvent(event);

        boolean logicalChunkOfWorkComplete = isLogicalChunkOfWorkComplete();
        if (logicalChunkOfWorkComplete)
        {
            sequenceCallback.set(sequence);
        }

        batchRemaining = logicalChunkOfWorkComplete || endOfBatch ? 20 : batchRemaining;
    }

    private boolean isLogicalChunkOfWorkComplete()
    {
        // Ret true or false based on whatever criteria is required for the smaller
        // chunk.  If this is doing I/O, it may be after flushing/syncing to disk
        // or at the end of DB batch+commit.
        // Or it could simply be working off a smaller batch size.

        return --batchRemaining == -1;
    }

    private void processEvent(PointDistruptorable event) 
    {
    	try {
			processor.process(event.getPoint());
		} catch (Exception e) {
			e.printStackTrace();
		}
    }
}