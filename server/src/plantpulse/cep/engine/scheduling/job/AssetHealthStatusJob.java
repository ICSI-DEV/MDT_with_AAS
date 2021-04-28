package plantpulse.cep.engine.scheduling.job;

import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.json.JSONObject;

import plantpulse.cep.engine.asset.AssetTableLayoutFactory;
import plantpulse.cep.engine.async.AsynchronousExecutor;
import plantpulse.cep.engine.async.Worker;
import plantpulse.cep.engine.scheduling.JobConstants;
import plantpulse.cep.engine.scheduling.JobDateUtils;
import plantpulse.cep.engine.snapshot.PointMapSnapshot;
import plantpulse.cep.service.client.StorageClient;
import plantpulse.domain.Asset;
import plantpulse.event.opc.Point;

/**
 * TagDataRowJob
 * @author lsb
 *
 */
public class AssetHealthStatusJob  extends SchedulingJob {

	private static final Log log = LogFactory.getLog(AssetHealthStatusJob.class);

	private String term;
	
	public AssetHealthStatusJob() {
		//
	}

	@Override
	public void run() {
		//
		try {

			log.debug("Asset health status job starting...");
			
			term = context.getJobDetail().getJobDataMap().getString("term");

			final long current_timestamp  = (System.currentTimeMillis()/1000) * 1000;
			final String from = JobDateUtils.from(current_timestamp, term);
			final String to  = JobDateUtils.to(current_timestamp);
			final Map<String, Point> point_map = new PointMapSnapshot().snapshot(); //메모리
			
			final StorageClient client = new StorageClient();
			
			TimeUnit.MILLISECONDS.sleep(JobConstants.JOB_DELAY_10_SEC);
			
			AsynchronousExecutor exec = new AsynchronousExecutor();
			exec.execute(new Worker() {
				public String getName() { return "AssetHealthStatusJob"; };
				@Override
				public void execute() {
					try {
						   //
							if(point_map != null){
								
							    long start = System.currentTimeMillis();
								//AssetDAO dao = new AssetDAO();
								//List<Asset> list = dao.selectModels(); //
							    List<Asset> list = AssetTableLayoutFactory.getInstance().getAssetList();
								//
								for (int i = 0; list != null && i < list.size(); i++) {
									Asset model = list.get(i);

									int info_count = 0;
									int warn_count = 0;
									int error_count = 0;
									
									//
									JSONObject count_json = client.forSelect().selectAlarmCountByAssetId(model, current_timestamp, from, to);
									info_count  += count_json.getInt("info_count");
									warn_count  += count_json.getInt("warn_count");
									error_count += count_json.getInt("error_count");
									
									//알람 레벨별 건수로, 다시 헬스 상태 재 집계
							        String status = "NORMAL";
							        if(info_count > 0){ 
							        	status = "INFO";
							        };
							        if(warn_count > 0){
							        	status = "WARN";
							        };
							        if(error_count > 0){
							        	status = "ERROR";
							        };
									
									client.forInsert().insertAssetHealthStatus(
											current_timestamp, 
											model, 
											JobDateUtils.toTimestamp(from), 
											JobDateUtils.toTimestamp(to), 
											status, 
											info_count, 
											warn_count, 
											error_count
											);
									
									log.debug("Asset health status inserted : asset_id=[" + model.getAsset_id() + "], asset_name=[" + model.getAsset_name() + "], status=[" + status + "]");
							
									
								}
							  long end = System.currentTimeMillis() - start;
							  log.debug("Asset health status all inserted : asset_size=[" + list.size() + "], exec_time=[" + end + "]ms");
							}
					} catch (Exception e) {
						log.error("Asset health status insert error : " + e.getMessage(), e);
					}
				}
				
			});

		} catch (Exception e) {
			log.error("Asset health status job failed : " + e.getMessage(), e);
		}
		;
	}

}
