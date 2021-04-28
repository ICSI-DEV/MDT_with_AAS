package plantpulse.cep.engine.scheduling.job;

import java.util.Map;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.json.JSONObject;

import plantpulse.cep.engine.async.AsynchronousExecutor;
import plantpulse.cep.engine.async.Worker;
import plantpulse.cep.engine.snapshot.PointMapSnapshot;
import plantpulse.cep.service.client.StorageClient;
import plantpulse.event.opc.Point;

/**
 * TagPointSnapshotJob
 * 
 * @author lsb
 * @deprecated
 */
public class TagPointMapBynaryJob extends SchedulingJob {

	private static final Log log = LogFactory.getLog(TagPointMapBynaryJob.class);

	
	public TagPointMapBynaryJob() {
		//
	}

	@Override
	public void run() {
		//
		try {
			
			final long current_timestamp = (System.currentTimeMillis()/1000) * 1000;
			final Map<String, Point> point_map = new PointMapSnapshot().snapshot(); //메모리
			final StorageClient client = new StorageClient();
			
			AsynchronousExecutor exec = new AsynchronousExecutor();
			exec.execute(new Worker() {
				public String getName() { return "TagPointMapBynaryJob"; };
				@Override
				public void execute() {
					try {
						//
						JSONObject json = JSONObject.fromObject(point_map);
						//client.getInsertDAO().insertPointMapBinary(current_timestamp, json);
						//
					} catch (Exception e) {
						log.error("Point map binary data insert error : " + e.getMessage(), e);
					}
				}
			});
		} catch (Exception e) {
			log.error("Point map binary data insert failed : " + e.getMessage(), e);
		}
		;
	}
	

}
