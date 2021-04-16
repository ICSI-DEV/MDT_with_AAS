package plantpulse.cep.engine.storage.timeseries;

import plantpulse.cep.engine.storage.timeseries.kairosdb.KairosDBTimeSeriesDatabase;

/**
 * TimeSeriesDatabase
 * @author leesa
 *
 */
public class TimeSeriesDatabaseFactory {
	
	public static final String TS_DB_NAME = "TS_DB_NAME";
	
	public static final String KAIROS_DB  = "KAIROS_DB";
	
	public TimeSeriesDatabase getTimeSeriesDatabase() {
		return new KairosDBTimeSeriesDatabase();
	}

}
