package plantpulse.cep.engine.storage.buffer;

import java.util.List;

import plantpulse.buffer.db.DBDataPoint;

public interface DBDataPointBuffer {

	public void init();

	public List<DBDataPoint> getDBDataList() throws Exception ;

	public void add(DBDataPoint dd) throws Exception;

	public void addList(List<DBDataPoint> list) throws Exception;

	public long size() ;
	
	public void stop() ;
	
}