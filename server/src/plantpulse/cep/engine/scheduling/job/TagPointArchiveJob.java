package plantpulse.cep.engine.scheduling.job;

import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import plantpulse.cep.dao.TagDAO;
import plantpulse.cep.engine.async.AsynchronousExecutor;
import plantpulse.cep.engine.async.Worker;
import plantpulse.cep.engine.scheduling.JobConstants;
import plantpulse.cep.engine.scheduling.JobDateUtils;
import plantpulse.cep.service.client.StorageClient;
import plantpulse.domain.Tag;
import plantpulse.json.JSONObject;

/**
 * 포인트 아카이브 잡
 * @author leesa
 * @deprecated
 */
public class TagPointArchiveJob   extends SchedulingJob {

	private static final Log log = LogFactory.getLog(TagPointArchiveJob.class);
	
	private static final int THEAD_COUNT     = 6;
	
	private static final int SLOW_NOTI_MILIS = 1 * 1000; //

	
	public TagPointArchiveJob() {
		
	}

	@Override
	public void run() {
		//
		try {
			
			//
			log.info("Point archive job starting...  delay=[" + JobConstants.JOB_DELAY_1_MIN + "]");
			
			TimeUnit.MILLISECONDS.sleep(JobConstants.JOB_DELAY_1_MIN);
			
			AsynchronousExecutor exec = new AsynchronousExecutor();
			exec.execute(new Worker() {
				public String getName() { return "TagPointArchiveJob"; };
				@Override
				public void execute() {
					
					ExecutorService exec_service = Executors.newFixedThreadPool(THEAD_COUNT); 
					
					try {
						
						//
						LocalDateTime datetime = LocalDateTime.now().minusHours(1);
						
						DateTimeFormatter date_formatter = DateTimeFormatter.ofPattern("yyyyMMdd");
						DateTimeFormatter hour_formatter = DateTimeFormatter.ofPattern("HH");
						
						final int date = Integer.parseInt(datetime.format(date_formatter));
						final int hour = Integer.parseInt(datetime.format(hour_formatter));
						
						//
						
						//
						long start = (System.currentTimeMillis()/1000) * 1000;
						
						
						final StorageClient client = new StorageClient();
						
						TagDAO tag_dao = new TagDAO();
						List<Tag> tag_list = tag_dao.selectTagAll();
						int tag_size = tag_list.size();
						
						CountDownLatch latch = new CountDownLatch(tag_list.size());
						AtomicLong total_point_count = new AtomicLong(0);
						
						log.info("Point archive job started : tag_size=[" + tag_size + "], date=[" + date + "], hour=[" + hour + "]");
						
						for (int i = 0; i < tag_list.size(); i++) {
							Tag tag = tag_list.get(i);
							PointArchiveTask task = new PointArchiveTask(client, latch, total_point_count, tag, date, hour);
							exec_service.execute(task);
						};
					
						latch.await();
						
						long end = System.currentTimeMillis() - start;
						log.info("Point archive job completed : tag_size=[" + tag_size + "], total_point_count=[" + total_point_count.get() + "], date=[" + date + "], hour=[" + hour + "], exec_time=[" + end + "]ms");
						//
					} catch (Exception e) {
						log.error("Point archive job error : " + e.getMessage(), e);
					}finally {
						exec_service.shutdown();
					}

				}
				
			});
			

		} catch (Exception e) {
			log.error("Point archive data insert failed : " + e.getMessage(), e);
		}
		;
	}
	
	class PointArchiveTask implements Runnable {
		
		private StorageClient client;

		private  CountDownLatch  latch;
		private  AtomicLong  total_point_count;
		
		private Tag tag;
		private int date;
		private int hour;
		
		public PointArchiveTask(StorageClient client,CountDownLatch  latch, AtomicLong  total_point_count, Tag tag, int date, int hour) {
			this.client = client;
			this.latch = latch;
			this.total_point_count = total_point_count;
			this.tag = tag;
			this.date = date;
			this.hour = hour;
		}
		
		@Override
		public void run(){
			try {
				long in_start = System.currentTimeMillis();
				//
				//
				String data_s = JobDateUtils.intToDashDate(date);
		         
				//
				int count = 0;
				Map<String, List<String>> map = new HashMap<String, List<String>>();
				for(int minute=0; minute < 60; minute++) {
					String from = data_s + " " + String.format("%02d", hour) + ":" + String.format("%02d", minute) + ":00.000";
					String to   = data_s + " " + String.format("%02d", hour) + ":" + String.format("%02d", minute) + ":59.999";
					//
					List<Map<String, Object>> result = client.forSelect().selectPointList(tag, from, to);
					List<String> list = new ArrayList<String>();
					for(int j=0; j < result.size();j++) {
						list.add(JSONObject.fromObject(result.get(j)).toString());
					}
					map.put("M_" + (String.format("%02d", minute)) , list);
					count += list.size();
				};
				//
				total_point_count.set(total_point_count.get() + count);
				//
				client.forInsert().insertPointArchive(tag, date, hour, count, map);
				//
				JSONObject json = JSONObject.fromObject(map);
				final byte[] utf8_bytes = json.toString().getBytes("UTF-8");
				long bytes = (utf8_bytes.length); //
				//
				long in_end = System.currentTimeMillis() - in_start;
				if(in_end > SLOW_NOTI_MILIS) { //저장처리가 3초보다 느릴 경우, 로그 표시
					log.info("Point archive saved with slow notify : tag=[" + tag.getTag_id() + "], date=[" + date + "], hour=[" + hour + "], count=[" + count + "], bytes=[" + bytes + "], exec_time=[" +in_end + "]ms");
				};
			
			}catch(Exception ex) {
				log.error("Point archive save job task error : " + ex.getMessage(), ex);
			}finally {
				if (latch == null) return;
				latch.countDown();
			}
		}

	}

}
