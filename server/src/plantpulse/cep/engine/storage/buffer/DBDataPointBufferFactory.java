package plantpulse.cep.engine.storage.buffer;

import plantpulse.cep.engine.storage.buffer.remote.DBDataPointBufferRemote;

/**
 * DBDataPointBufferFactory
 * @author leesa
 *
 */
public class DBDataPointBufferFactory {

	public static DBDataPointBuffer getDBDataBuffer() {
		//return DBDataPointBufferLocal.getInstance();
		//레디스로 DB 버퍼를 변경
		return DBDataPointBufferRemote.getInstance();
	}

}
