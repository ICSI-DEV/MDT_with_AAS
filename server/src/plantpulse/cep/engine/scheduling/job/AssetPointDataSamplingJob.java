package plantpulse.cep.engine.scheduling.job;

import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutorService;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.json.JSONObject;

import plantpulse.cep.engine.asset.AssetTableLayoutFactory;
import plantpulse.cep.engine.async.AsynchronousExecutor;
import plantpulse.cep.engine.async.Worker;
import plantpulse.cep.engine.snapshot.PointMapSnapshot;
import plantpulse.cep.engine.thread.AsyncInlineExecuterFactory;
import plantpulse.cep.engine.utils.AliasUtils;
import plantpulse.cep.service.client.StorageClient;
import plantpulse.domain.Asset;
import plantpulse.domain.Tag;
import plantpulse.event.opc.Point;

/**
 * TagDataRowJob
 * 
 * 
 * @author lsb
 *
 */
public class AssetPointDataSamplingJob extends SchedulingJob {

	private static final Log log = LogFactory.getLog(AssetPointDataSamplingJob.class);
	
	private String term;
	
	public AssetPointDataSamplingJob() {
		
	}

	
	@Override
	public void run() {
		//
		try {
			//

			log.debug("Point asset data sampling job starting...");
			
			
			term = context.getJobDetail().getJobDataMap().getString("term");

			final long current_timestamp = (System.currentTimeMillis()/1000) * 1000;
			final Map<String, Point> point_map = new PointMapSnapshot().snapshot(); //메모리
			final StorageClient client = new StorageClient();

			//
			if (point_map != null) {

				List<Asset> list = AssetTableLayoutFactory.getInstance().getAssetList();
				//
				AsynchronousExecutor exec = new AsynchronousExecutor();
				exec.execute(new Worker() {
						public String getName() { return "AssetPointDataSamplingJob"; };
						@Override
						public void execute() {
							long start = System.currentTimeMillis();
							ExecutorService executor = AsyncInlineExecuterFactory.getExecutor();
							for (int i = 0; list != null && i < list.size(); i++) {
								final Asset model = list.get(i);
								executor.execute(new AsyncProcessor(client, model, point_map, current_timestamp)); 
						    };
						    executor.shutdown();
						    long end = System.currentTimeMillis()  -start;
						    log.debug("Asset point data sampling all inserted : asset_size=[" + list.size() + "], process_time["  + end + "]ms");
						}
					});
		}
		} catch (Exception e) {
			log.error("Asset point data sampling job failed : " + e.getMessage(), e);
		}
	};
	
	
	class AsyncProcessor implements Runnable {

		private StorageClient client;
		private Asset model;
		private Map<String, Point> point_map;
		private long current_timestamp;
		
		public AsyncProcessor( StorageClient client,  Asset model,  Map<String, Point> point_map,  long current_timestamp) {
			this.client = client;
			this.model = model;
			this.point_map = point_map;
			this.current_timestamp = current_timestamp;
		};
		
		@Override
		public void run() {
			try {

				//TagDAO tag_dao = new TagDAO();
				//List<Tag> tags = td.selectTagsByLinkAssetId(model.getAsset_id());
				List<Tag> tags = AssetTableLayoutFactory.getInstance().getTagList(model.getAsset_id());

				//
				if (tags != null && tags.size() > 0) {

					//
					Map<String, Tag> tag_map = new HashMap<String, Tag>();
					for (int x = 0; x < tags.size(); x++) {
						Tag tag = tags.get(x);
						String filed_name = AliasUtils.getAliasName(tag);
						tag_map.put(filed_name, tag);
					}
					;

					// 선택
					JSONObject data_map = new JSONObject();
					JSONObject timestamp_map = new JSONObject();
					Iterator<String> titer = tag_map.keySet().iterator();
					while (titer.hasNext()) {
						Tag tag = tag_map.get(titer.next());
						String filed_name = AliasUtils.getAliasName(tag);
						Point point = point_map.get(tag.getTag_id());
						if (point != null) {
							data_map.put(filed_name, point.getValue());
							timestamp_map.put(filed_name, point.getTimestamp());
						} else {
							data_map.put(filed_name, "");
							timestamp_map.put(filed_name, new Long(0));
						}
					};

					// CEP 전송
					/*
					plantpulse.event.asset.AssetData asset_data_event = new plantpulse.event.asset.AssetData();
					asset_data_event.setTimestamp(current_timestamp);
					asset_data_event.setSite_id(model.getSite_id());
					asset_data_event.setAsset_id(model.getAsset_id());
					asset_data_event.setAsset_name(model.getAsset_name());
					asset_data_event.setAsset_type(model.getTable_type()); // 테이블 타입 -> 에셋 타입 변경의 유의
					asset_data_event.setData_map(new HashMap<String, String>(data_map));
					asset_data_event.setTimestamp_map(new HashMap<String, Long>(timestamp_map));
					//
					if (CEPEngineManager.getInstance().getProvider() != null) {
						CEPEngineManager.getInstance().getProvider().getEPRuntime().sendEvent(asset_data_event);
					}
					;
					*/

					// CASSANDRA 저장
					client.forInsert().insertAssetDataSampling(term, current_timestamp, model, tag_map, point_map);
					

					// 데이터 전송
					/*
					DataDistributionService dds = CEPEngineManager.getInstance().getData_distribution_service();
					dds.sendAssetData(model.getAsset_id(), JSONObject.fromObject(asset_data_event));
					*/

					//
					log.debug("Asset data sampling aync saved : asset_id=[" + model.getAsset_id() + "], asset_name=[" + model.getAsset_name() + "]");

				}
				; // tag size > 0

			} catch (Exception e) {
				log.error("Asset data samplling aync save error : " + e.getMessage(), e);
			}
		
		}
		
	}
	
}
