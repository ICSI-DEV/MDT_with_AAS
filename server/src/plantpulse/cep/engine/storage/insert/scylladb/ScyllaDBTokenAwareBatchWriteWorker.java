package plantpulse.cep.engine.storage.insert.scylladb;

import plantpulse.cep.engine.storage.insert.cassandra.CassandraTokenAwareBatchWriteWorker;
import plantpulse.cep.engine.storage.timeseries.TimeSeriesDatabase;

public class ScyllaDBTokenAwareBatchWriteWorker extends CassandraTokenAwareBatchWriteWorker {

	public ScyllaDBTokenAwareBatchWriteWorker(int timer_number, int thread_number, boolean patition_grouping,
			int replication_factor, TimeSeriesDatabase timeseries_sync) {
		super(timer_number, thread_number, patition_grouping, replication_factor, timeseries_sync);
	}

}
