package plantpulse.cep.engine.scheduling.job;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.json.JSONObject;
import org.redisson.api.RFuture;
import org.redisson.api.RTimeSeries;

import plantpulse.cep.engine.async.AsynchronousExecutor;
import plantpulse.cep.engine.async.Worker;
import plantpulse.cep.engine.inmemory.redis.RedisInMemoryClient;
import plantpulse.cep.engine.utils.TimestampUtils;
import plantpulse.cep.service.client.StorageClient;
import plantpulse.event.opc.Point;

@Deprecated
public class TagPointMatrixJob  extends SchedulingJob {

	private static final Log log = LogFactory.getLog(TagPointMatrixJob.class);
	
	public static final long RANGE_MINUS_MS = 5000;

	public TagPointMatrixJob() {
		//
	}

	@Override
	public void run() {
		//
		try {

			log.debug("Point matrix job starting ...");

			final long current_timestamp = System.currentTimeMillis();
			
			final long timestamp = (current_timestamp / 1000) * 1000;
			final long from = timestamp - RANGE_MINUS_MS - 1000;
			final long to   = timestamp - RANGE_MINUS_MS;
			final int  date = TimestampUtils.timestampToYYYYMMDD(from);
	
			final StorageClient client = new StorageClient();
			
			AsynchronousExecutor exec = new AsynchronousExecutor();
			exec.execute(new Worker() {
				public String getName() { return "TagPointMatrixJob"; };
				@Override
				public void execute() {
					try {
						//
						long start = System.currentTimeMillis();
						log.debug("Point matrix job start :  timestamp=[" + timestamp + "]");
						//
						RTimeSeries<Point> ts = RedisInMemoryClient.getInstance().getPointTimeSeries();
						
						RFuture<Collection<Point>> future = ts.rangeAsync(from, to);
						future.whenComplete((points, exception) -> {
						    Map<String, List<String>> data_map = new HashMap<>();
						    if(points == null) log.debug("Not found point in timeseries cache.");
						    if(points != null) {
							    Iterator<Point> it = points.iterator();
							    while(it.hasNext()) {
							    	Point point = it.next();
							    	if(!data_map.containsKey(point.getTag_id())) {
							    		data_map.put(point.getTag_id(), new ArrayList<String>());
							    	}
							    	data_map.get(point.getTag_id()).add(JSONObject.fromObject(point).toString());
							    };
								try {
									client.forInsert().insertPointMatrix(date, timestamp, from, to, data_map);
								} catch (Exception e) {
									log.error("Point matrix insert error : " + e.getMessage(), e);
								}
						    };
						});
						
						long end = System.currentTimeMillis() - start;
						log.debug("Point matrix job completed :  exec_time=[" + end + "]ms");
						//
						
					} catch (Exception e) {
						log.error("Point matrix job error : " + e.getMessage(), e);
					}

				}
				
			});

		} catch (Exception e) {
			log.error("Point matrix data insert job failed : " + e.getMessage(), e);
		}
		;
	}
	
}