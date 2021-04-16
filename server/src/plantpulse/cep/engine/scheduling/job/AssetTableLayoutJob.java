package plantpulse.cep.engine.scheduling.job;

import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutorService;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import plantpulse.cep.dao.AssetDAO;
import plantpulse.cep.dao.TagDAO;
import plantpulse.cep.engine.asset.AssetTableLayoutFactory;
import plantpulse.cep.engine.async.AsynchronousExecutor;
import plantpulse.cep.engine.async.Worker;
import plantpulse.cep.engine.snapshot.PointMapSnapshot;
import plantpulse.cep.engine.thread.AsyncInlineExecuterFactory;
import plantpulse.domain.Asset;
import plantpulse.domain.Tag;
import plantpulse.event.opc.Point;

/**
 * AssetTableLayoutJob
 * 
 * <pre>
 * 에셋의 테이블 레이아웃을 로드하여 캐싱한다.
 * </pre>
 * 
 * @author lsb
 *
 */
public class AssetTableLayoutJob extends SchedulingJob {

	private static final Log log = LogFactory.getLog(AssetTableLayoutJob.class);

	public AssetTableLayoutJob() {
		//
	}

	public void load() {
		log.info("Asset table layout cache loading for system startup ...");
		run();
	}

	@Override
	public void run() {
		//
		try {
			//

			Thread.sleep(1000 * 60); // 60초 딜레이

			long start = System.currentTimeMillis();
			log.debug("Asset table layout cache job starting...");
			final Map<String, Point> point_map = new PointMapSnapshot().snapshot(); // 메모리

			//
			if (point_map != null) {

				AssetDAO dao = new AssetDAO();
				List<Asset> model_list = dao.selectModels(); //
				if (model_list != null && model_list.size() > 0) {
					log.debug("Asset model size = [" + model_list.size() + "]");
				}
				AssetTableLayoutFactory.getInstance().setAssetList(model_list);
				AsynchronousExecutor exec = new AsynchronousExecutor();
				exec.execute(new Worker() {
					public String getName() {
						return "AssetTableLayoutJob";
					};

					@Override
					public void execute() {
						//
						ExecutorService executor = AsyncInlineExecuterFactory.getExecutor();
						for (int i = 0; model_list != null && i < model_list.size(); i++) {
							final Asset model = model_list.get(i);
							executor.execute(new LaoyoutAsyncTask(model));
						}
						executor.shutdown();
						//
					}
				});

				long end = System.currentTimeMillis() - start;
				log.info("Asset table layout cached : asset_size=[" + model_list.size() + "], process_time=[" + end + "]ms");
			}

			; //

		} catch (Exception e) {
			log.error("Asset table layout cache job failed : " + e.getMessage(), e);
		}
	};
	
	/**
	 * LaoyoutAsyncTask
	 * @author leesa
	 *
	 */
  class LaoyoutAsyncTask implements Runnable {

		private Asset model;
		public LaoyoutAsyncTask( Asset model) {
			this.model = model;
		}

		@Override
		public void run() {
			try {
				TagDAO td = new TagDAO();
				List<Tag> tags = td.selectTagsByLinkAssetId(model.getAsset_id());
				//
				if (tags != null && tags.size() > 0) {
					AssetTableLayoutFactory.getInstance().putTagList(model.getAsset_id(), tags);
					log.debug("Asset table layout cached : asset_id=[" + model.getAsset_id() + "], tag_list=[" + tags.size() + "]");
				}
				;
			} catch (Exception e) {
				log.error("Asset table layout cache error : " + e.getMessage(), e);
			}
		}
	}

}