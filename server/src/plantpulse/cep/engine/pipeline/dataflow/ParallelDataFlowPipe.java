package plantpulse.cep.engine.pipeline.dataflow;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import plantpulse.cep.engine.dds.DataDistributionService;
import plantpulse.cep.engine.pipeline.DataFlowPipe;
import plantpulse.cep.engine.pipeline.async.DataPipeline;
import plantpulse.cep.engine.pipeline.dataflow.adjuster.DataFlowAdjuster;
import plantpulse.cep.engine.storage.StorageProcessor;
import plantpulse.cep.engine.stream.StreamProcessor;

/**
 * ParallelDataFlowPipe
 * 
 * @author lsb
 *
 */
public class ParallelDataFlowPipe extends DataPipeline implements DataFlowPipe {

	private static Log log = LogFactory.getLog(ParallelDataFlowPipe.class);

	private DataFlowAdjuster data_flow_adjuster;
	//
	private StreamProcessor stream_prcoessor; 
	private StorageProcessor storage_processor;
	//
	private DataDistributionService dds;
	
    //
	public ParallelDataFlowPipe(DataFlowAdjuster data_flow_adjuster, StreamProcessor stream_processor, StorageProcessor storage_processor, DataDistributionService dds)  {
		super();
		
		this.data_flow_adjuster = data_flow_adjuster;
		this.stream_prcoessor   = stream_processor;
		this.storage_processor = storage_processor;
		this.dds = dds;
		
		//
		log.info("Message data flow initiallized.");
	}

	@Override
	public DataFlowAdjuster getAdjuster() {
		return data_flow_adjuster;
	}

	@Override
	public StreamProcessor getStreamProcessor() {
		return stream_prcoessor;
	}

	@Override
	public StorageProcessor getStorageProcessor() {
		return storage_processor;
	}

	@Override
	public DataDistributionService getDDS() {
		return dds;
	};
	
}
