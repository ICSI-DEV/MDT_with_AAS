package plantpulse.cep.engine.scheduling.job;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.json.JSONObject;

import plantpulse.cep.dao.SiteDAO;
import plantpulse.cep.dao.TagDAO;
import plantpulse.cep.engine.async.AsynchronousExecutor;
import plantpulse.cep.engine.async.Worker;
import plantpulse.cep.engine.scheduling.JobDateUtils;
import plantpulse.cep.engine.snapshot.PointMapSnapshot;
import plantpulse.cep.engine.thread.AsyncInlineExecuterFactory;
import plantpulse.cep.service.client.StorageClient;
import plantpulse.domain.Site;
import plantpulse.domain.Tag;
import plantpulse.event.opc.Point;

/**
 * TagPointSnapshotJob
 * 
 * @author lsb
 *
 */
public class TagPointSnapshotJob  extends SchedulingJob {

	private static final Log log = LogFactory.getLog(TagPointSnapshotJob.class);

	private String term;

	public TagPointSnapshotJob() {
		
	}

	@Override
	public void run() {
		//
		try {
			
			term = context.getJobDetail().getJobDataMap().getString("term");

			log.debug("Point snapshot job starting... : term=[" + term + "]");

			final long current_timestamp = ((long) (System.currentTimeMillis() / 1000)) * 1000 ; 
			
			final String from = JobDateUtils.from(current_timestamp, term);
			final String to = JobDateUtils.to(current_timestamp);
			final Map<String, Point> point_map = new PointMapSnapshot().snapshot(); //메모리
			final SiteDAO site_dao = new SiteDAO();
			final TagDAO tag_dao   = new TagDAO();
			final StorageClient client = new StorageClient();
			
			AsynchronousExecutor exec = new AsynchronousExecutor();
			exec.execute(new Worker() {
				public String getName() { return "TagPointSnapshotJob"; };
				@Override
				public void execute() {
					try {
						//
						long start = System.currentTimeMillis();
						//
						
						List<Tag> tag_list = tag_dao.selectTagAll();
						
						JSONObject data_map = new JSONObject();
						JSONObject timestamp_map = new JSONObject();
						JSONObject alarm_map = new JSONObject();
			
					   //1. 값 및 타임스탬프 맵 생성
						for(int i = 0; i < tag_list.size(); i++) {
								final Tag tag = tag_list.get(i);
								String value = point_map != null && point_map.containsKey(tag.getTag_id()) ? point_map.get(tag.getTag_id()).getValue() : "";
								Long timestamp = point_map != null && point_map.containsKey(tag.getTag_id()) ? new Long(point_map.get(tag.getTag_id()).getTimestamp()) : new Long(0);
								data_map.put(tag.getTag_id(), value);
								timestamp_map.put(tag.getTag_id(), timestamp);
						}
						
						//
						ExecutorService executor = AsyncInlineExecuterFactory.getExecutor();
                        List<Callable<TaskResult>> tasks = new ArrayList<Callable<TaskResult>>();
						for(int i = 0; i < tag_list.size(); i++) {
							final Tag tag = tag_list.get(i);
							AlarmErrorCountTask task = new AlarmErrorCountTask(client, current_timestamp, from, to, tag);
							tasks.add(task);
						}
						
						List<Future<TaskResult>> futures = executor.invokeAll(tasks);
						
						for (Future<TaskResult> future : futures) {
							TaskResult result = future.get(); //
							alarm_map.put(result.tag.getTag_id(), result.alarm);
						};
						
						executor.shutdown();
						
						long middle = System.currentTimeMillis() - start;
						log.debug("Point snapshot tasks processing time = [" + middle + "]ms");
						
						//스냅샷 카산드라 저장 (사이트별)
						List<Site> site_list = site_dao.selectSites();
						for(int si=0; si < site_list.size(); si++){
							Site site = site_list.get(si);
							List<Tag> site_tag_list = tag_dao.selectTagsBySiteId(site.getSite_id());
							
							JSONObject site_data_map = new JSONObject();
							JSONObject site_timestamp_map = new JSONObject();
							JSONObject site_alarm_map = new JSONObject();
							
							for(int sti=0; site_tag_list != null && sti < site_tag_list.size(); sti++) {
								Tag site_tag = site_tag_list.get(sti);
								site_data_map.put(site_tag.getTag_id(), data_map.get(site_tag.getTag_id()));
								site_timestamp_map.put(site_tag.getTag_id(), timestamp_map.get(site_tag.getTag_id()));
								site_alarm_map.put(site_tag.getTag_id(), alarm_map.get(site_tag.getTag_id()));
							};
						    //
							client.forInsert().insertPointSnapshot(site.getSite_id(), term, current_timestamp, site_data_map, site_timestamp_map, site_alarm_map);
						}
						
						long end = System.currentTimeMillis() - start;
						log.debug("Point snapshot job completed : term=[" + term + "], exec_time=[" + end + "]ms");
						//
					} catch (Exception e) {
						log.error("Point snapshot error : term=[" + term + "]", e);
					}

				}
				
			});
			

		} catch (Exception e) {
			log.error("Point snapshot data insert failed : " + e.getMessage(), e);
		}
		;
	}
	
	/**
	 * TaskResult
	 * @author leesa
	 *
	 */
	public class TaskResult {
		private Tag tag;
		public long alarm = 0;
	}
	
	/**
	 * AlarmErrorCountTask
	 * @author leesa
	 *
	 */
	public class AlarmErrorCountTask implements Callable<TaskResult>{
		private StorageClient client;
		private long current_timestamp;
		private String from;
		private String to;
		private Tag tag;
	
		public AlarmErrorCountTask(StorageClient client, long current_timestamp,  String from, String to, Tag tag) {
			this.client = client;
			this.current_timestamp = current_timestamp;
			this.from = from;
			this.to = to;
			this.tag = tag;
		}

		@Override
		public TaskResult call() throws Exception {
			//
			JSONObject alarm = client.forSelect().selectAlarmErrorCountForSnapshot(tag, current_timestamp, from, to);
			//
			TaskResult sdt = new TaskResult();
			sdt.tag = tag;
			sdt.alarm = (alarm != null && alarm.containsKey("error_count")) ? alarm.getLong("error_count") :  (0);
			return sdt;
		}
	
		
	}

}
