package plantpulse.cep.engine.deploy;

import java.util.List;
import java.util.concurrent.ExecutorService;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import plantpulse.cep.dao.TagDAO;
import plantpulse.cep.engine.async.AsynchronousExecutor;
import plantpulse.cep.engine.async.Worker;
import plantpulse.cep.engine.logging.EngineLogger;
import plantpulse.cep.engine.storage.StorageConstants;
import plantpulse.cep.engine.thread.AsyncInlineExecuterFactory;
import plantpulse.cep.service.support.tag.aggregation.TagAggregationDeployer;
import plantpulse.domain.Tag;

/**
 * 
 * PointAggregationDeployer
 * 
 * 1분 10분 1시간 12시간 24시간
 * 
 * @author lsb
 *
 */
public class PointAggregationDeployer implements Deployer {

	private static final Log log = LogFactory.getLog(PointAggregationDeployer.class);

	public void deploy() {

		//
		if (StorageConstants.AGGREGATION_ENABLED) {
			//
			log.info("Point aggregation deploy started...");

			AsynchronousExecutor exec = new AsynchronousExecutor();
			exec.execute(new Worker() {
				public String getName() {
					return "PointAggregationDeployer";
				};

				public void execute() {
					try {
						long start = System.currentTimeMillis();
						
						TagDAO tag_dao = new TagDAO();
						List<Tag> tag_list = tag_dao.selectTagAll();

						ExecutorService executor = AsyncInlineExecuterFactory.getExecutor();
						for (int i = 0; tag_list != null && i < tag_list.size(); i++) {
							final Tag tag = tag_list.get(i);
							executor.execute(new AggAsyncTask(tag));
						}
						executor.shutdown();
						
						long end = System.currentTimeMillis() - start;
						log.info("Tag aggregation deploy completed : process_time=[" + end + "]ms");
						
					} catch (Exception ex) {
						EngineLogger.error("태그 포인트 집계 처리기를 배치하는 도중 오류가 발생하였습니다 : 에러=" + ex.getMessage());
						log.error("Point aggregation deploy error : " + ex.getMessage(), ex);
					}
				}
			});

			log.info("Point aggregation job deployed.");
		} else {
			log.info("Point aggregation job disabled.");
		}

	}

	
	class AggAsyncTask implements Runnable {

		private Tag tag;

		public AggAsyncTask(Tag tag) {
			this.tag = tag;
		}

		@Override
		public void run() {
			//
			try {
				TagAggregationDeployer deployer = new TagAggregationDeployer();
				deployer.deploy(tag);
			} catch (Exception ex) {
				EngineLogger.error(
						"태그 집계 설정을 배치하는 도중 오류가 발생하였습니다 : 태그ID=[" + tag.getTag_id() + "], 에러=[" + ex.getMessage() + "]");
			}
		}

	}

}
