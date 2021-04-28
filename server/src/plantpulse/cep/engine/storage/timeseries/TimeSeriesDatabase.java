package plantpulse.cep.engine.storage.timeseries;


import java.util.List;

import org.json.JSONObject;

import plantpulse.buffer.db.DBDataPoint;
import plantpulse.cep.engine.monitoring.jmx.PlantPulse;
import plantpulse.event.opc.Alarm;

/**
 * TimeSeriesDatabase
 * @author leesa
 *
 */
public interface TimeSeriesDatabase {

	public void connect()  ;
	
	public void syncPoint(List<DBDataPoint> db_data_list) ;
	
	public void syncAlarm(Alarm alarm, JSONObject extend) ;
	
	public void syncJMX(PlantPulse jmx) ;
	
	public void close() ;

}
