package plantpulse.cep.engine.scheduling.job;


import java.util.Map;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.json.JSONObject;

import plantpulse.cep.engine.CEPEngineManager;
import plantpulse.cep.engine.async.AsynchronousExecutor;
import plantpulse.cep.engine.async.Worker;
import plantpulse.cep.engine.config.ConfigurationManager;
import plantpulse.cep.engine.inmemory.redis.RedisInMemoryClient;
import plantpulse.cep.engine.scheduling.JobConstants;
import plantpulse.cep.engine.snapshot.PointMapSnapshot;
import plantpulse.cep.engine.timer.TimerExecuterPool;
import plantpulse.cep.listener.ResultServiceManager;
import plantpulse.event.opc.Point;
import plantpulse.event.opc.PointMap;


/**
 * TagPointMapJob
 * 
 * @author lsb
 *
 */
public class TagPointMapJob {

	private static final Log log = LogFactory.getLog(TagPointMapJob.class);

	private ScheduledFuture<?> timer;
	
	public static final String POINT_MAP_WS_PATH = "/data/pointmap";

	
	public void start(){
		
		long INTERVAL = Long.parseLong(ConfigurationManager.getInstance().getApplication_properties().getProperty("pointmap.timer.period.ms"));
		
		timer = TimerExecuterPool.getInstance().getPool().scheduleAtFixedRate(new Runnable() {

			@Override
			public void run() {
				//
				try {

					log.debug("Point map job starting...");

					final long current_timestamp = ((long) (System.currentTimeMillis() / 1000)) * 1000 ; //MS단위 절삭
					final Map<String, Point> point_map = new PointMapSnapshot().snapshot(); //메모리
					
					//final StorageClient client = new StorageClient();
					
					TimeUnit.MILLISECONDS.sleep(JobConstants.JOB_DELAY_1_SEC);
					
					//1. 포인트 맵 저장
					AsynchronousExecutor exec = new AsynchronousExecutor();
					exec.execute(new Worker() {
						public String getName() { return "TagPointMapJob"; };
						@Override
						public void execute() {
							try {
								   //
									if(point_map != null){
									  long start = System.currentTimeMillis();
									  //FIXME 포인트 맵 저장 성능 저하 요소 - 반드시 삭제
									  //client.getInsertDAO().insertPointMap(current_timestamp, point_map);
									  long end = System.currentTimeMillis() - start;
									  log.debug("Point map inserted : column_size=[" + point_map.size() + "], exec_time=[" + end + "]ms");
									}
							} catch (Exception e) {
								log.error("Point map storage insert error : " + e.getMessage(), e);
							}
						}
					});
					
					//
					PointMap event_map = new PointMap();
					event_map.setTimestamp(current_timestamp);
					event_map.setCount(point_map.size());
					event_map.setTag(point_map);
					
					
					//이벤트 전송
					if (CEPEngineManager.getInstance().getProvider() != null) {
						CEPEngineManager.getInstance().getProvider().getEPRuntime().sendEvent(event_map);
					};
					
					//푸시 전송
					try{
						JSONObject ws_data = JSONObject.fromObject(event_map);
						ResultServiceManager.getPushService().getPushClient(POINT_MAP_WS_PATH).sendJSON(ws_data);
					}catch(Exception ex){
						log.error("Point map websocket push failed : " + ex.getMessage(), ex);
					}
					
                    //2. 레디스로 저장
					try{
						RedisInMemoryClient.getInstance().getPointMap().putAllAsync(point_map);
						//
					}catch(Exception ex){
						log.error("Point map json in-memory save error", ex);
					}

				} catch (Exception e) {
					log.error("Point map job exectue failed : " + e.getMessage(), e);
				}
				;
			}
			
		}, 0, INTERVAL, TimeUnit.MILLISECONDS);
	}

	public void stop(){
		timer.cancel(true);
	}
	

}
