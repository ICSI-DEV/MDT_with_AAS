package plantpulse.cep.engine.scheduling.job;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.TimeUnit;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.json.JSONObject;

import plantpulse.cep.engine.asset.AssetTableLayoutFactory;
import plantpulse.cep.engine.async.AsynchronousExecutor;
import plantpulse.cep.engine.async.Worker;
import plantpulse.cep.engine.diagnostic.DiagnosticHandler;
import plantpulse.cep.engine.diagnostic.DiagnosticLevel;
import plantpulse.cep.engine.scheduling.JobConstants;
import plantpulse.cep.engine.scheduling.JobDateUtils;
import plantpulse.cep.engine.snapshot.PointMapSnapshot;
import plantpulse.cep.engine.thread.AsyncInlineExecuterFactory;
import plantpulse.cep.service.client.StorageClient;
import plantpulse.domain.Asset;
import plantpulse.domain.OPC;
import plantpulse.domain.Tag;
import plantpulse.event.opc.Point;

/**
 * 에셋 데이터 연결 상태 잡
 * @author leesa
 *
 */
public class AssetConnectionStatusJob  extends SchedulingJob {

	private static final Log log = LogFactory.getLog(AssetConnectionStatusJob.class);

	private String term;
	
	public static final long CONNECTION_WARN_TIMESTAMP  = (1000*60*10); //10분
	public static final long CONNECTION_ERROR_TIMESTAMP = (1000*60*60); //60분(1시간)
	
	public AssetConnectionStatusJob() {
		//
	}

	@Override
	public void run() {
		//
		try {

			log.debug("Asset connection status job starting...");
			
			term = context.getJobDetail().getJobDataMap().getString("term");

			final long current_timestamp  =  (System.currentTimeMillis()/1000) * 1000;
			final String from = JobDateUtils.from(current_timestamp, term);
			final String to  = JobDateUtils.to(current_timestamp);
			final Map<String, Point> point_map = new PointMapSnapshot().snapshot(); //메모리
			
			final StorageClient client = new StorageClient();
			
		
			TimeUnit.MILLISECONDS.sleep(JobConstants.JOB_DELAY_10_SEC);
			
			AsynchronousExecutor exec = new AsynchronousExecutor();
			exec.execute(new Worker() {
				public String getName() { return "AssetConnectionStatusJob"; };
				@Override
				public void execute() {
					try {
						   //
							if(point_map != null){
								
							    long start = System.currentTimeMillis();
								//
							    List<Asset> list = AssetTableLayoutFactory.getInstance().getAssetList();
								//
							    ExecutorService executor = AsyncInlineExecuterFactory.getExecutor();
								for (int i = 0; list != null && i < list.size(); i++) {
									final Asset model = list.get(i);
									executor.execute(new ConnectionCheckAsyncTask(model, current_timestamp, from, to, client));
								}//for
								executor.shutdown();
							  long end = System.currentTimeMillis() - start;
							  log.debug("Asset connection status all inserted : asset_size=[" + list.size() + "], exec_time=[" + end + "]ms");
							}
					} catch (Exception e) {
						log.error("Asset connection status insert error : " + e.getMessage(), e);
					}
				}
				
			});

		} catch (Exception e) {
			log.error("Asset connection status job failed : " + e.getMessage(), e);
		}
		;
	}
	
	/**
	 * ConnectionCheckAsyncTask
	 * @author leesa
	 *
	 */
	class ConnectionCheckAsyncTask implements Runnable {

		private Asset model;
		private long current_timestamp;
		private String from;
		private String to;
		private StorageClient client;

		public ConnectionCheckAsyncTask(Asset model,long current_timestamp, String from, String to, StorageClient client) {
			this.model = model;
			this.current_timestamp = current_timestamp;
			this.from = from;
			this.to = to;
			this.client = client;
		}

		@Override
		public void run() {
			
			try {
			int warn_count = 0;
			int error_count = 0;
			
			
			List<Tag> tags =  AssetTableLayoutFactory.getInstance().getTagList(model.getAsset_id());;

			if (tags != null && tags.size() > 0) {
				//
				List<String> checked_opc_id = new ArrayList<>();
				for (int x = 0; x < tags.size(); x++) {
						Tag tag = tags.get(x);
						if(!checked_opc_id.contains(tag.getOpc_id())) {
							OPC opc = new OPC();
							opc.setOpc_id(tag.getOpc_id());
							JSONObject opc_updated = client.forSelect().selectOPCPointLastUpdate(opc);
							if(opc_updated.has("last_update_timestamp")) {
								long last_update_timestamp = opc_updated.getLong("last_update_timestamp");
								if((System.currentTimeMillis() - last_update_timestamp) >= CONNECTION_WARN_TIMESTAMP) { //OPC 포인트가 마지막 10분동안 1건도 안들어왔으면, WARN
									warn_count++;
								}else if((System.currentTimeMillis() - last_update_timestamp) >= CONNECTION_ERROR_TIMESTAMP) { //OPC 포인트가 마지막 1시간동안 1건도 안들어왔으면, WARN
									error_count++;
								}
							};
							checked_opc_id.add(tag.getOpc_id());
						}
		        };
				
		        String status = "NORMAL";
		        if(warn_count > 0){
		        	status = "WARN";
		        }
		        if(error_count > 0){
		        	status = "ERROR";
		        };
		        
		        if(warn_count > 0 || error_count > 0) {
		        	log.warn("Asset connection status warnning : asset_id=[" + model.getAsset_id() + "], status=["  + status + "]");
		        	
		        };
		        
		        //에러카운트가 0보다 클경우 연결 종료 진단 알람
		        if( error_count > 0) {
		        	DiagnosticHandler.handleFormLocal(System.currentTimeMillis(), DiagnosticLevel.WARN, 
		        			"에셋 " + model.getAsset_name() + " [" + model.getDescription() + "]에 데이터 연결 문제가 발생하였습니다. 현재 최근 1시간동안 데이터가 수신되고 있지 않습니다."
		        			);
		        }
				
				client.forInsert().insertAssetConnectionStatus(
						current_timestamp, 
						model, 
						JobDateUtils.toTimestamp(from), 
						JobDateUtils.toTimestamp(to), 
						status
						);
				
				log.debug("Asset connection status inserted : asset_id=[" + model.getAsset_id() + "], asset_name=[" + model.getAsset_name() + "], status=[" + status + "]");
		
			}
			//
			}catch(Exception ex) {
				log.error("Asset connection status check failed : " + ex.getMessage(), ex);
			}
			
		}

	}

}