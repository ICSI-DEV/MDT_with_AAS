package plantpulse.cep.engine.storage.insert.scylladb;

import plantpulse.cep.engine.storage.insert.cassandra.CassandraBatchWriteWorker;
import plantpulse.cep.engine.storage.timeseries.TimeSeriesDatabase;

/**
 * ScyllaDBBatchWriteWorker
 * @author leesa
 *
 */
public class ScyllaDBBatchWriteWorker extends CassandraBatchWriteWorker {

	public ScyllaDBBatchWriteWorker(int timer_number, int thread_number, boolean patition_grouping,
			boolean use_async_statement, TimeSeriesDatabase timeseries_db) {
		super(timer_number, thread_number, patition_grouping, use_async_statement, timeseries_db);
	    //
	}

}
