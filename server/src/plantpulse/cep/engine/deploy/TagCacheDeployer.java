package plantpulse.cep.engine.deploy;

import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import plantpulse.cep.dao.TagDAO;
import plantpulse.cep.engine.async.AsynchronousExecutor;
import plantpulse.cep.engine.async.Worker;
import plantpulse.cep.engine.logging.EngineLogger;
import plantpulse.cep.service.support.tag.TagAndLocationCacheTask;
import plantpulse.domain.Tag;

/**
 * TagLocationDeployer
 * 
 * @author leesa
 *
 */
public class TagCacheDeployer implements Deployer  {

	private static final Log log = LogFactory.getLog(TagCacheDeployer.class);


	public void deploy() {
		
		AsynchronousExecutor exec = new AsynchronousExecutor();
		exec.execute(new Worker() {
			public String getName() { return "TagCacheDeployer"; };
			public void execute() {
					//
					try {
						
						long start = System.currentTimeMillis();
						TagDAO tag_dao = new TagDAO();
						List<Tag> list = tag_dao.selectTagAll();
						if(list != null) {
							log.info("Async caching deployed tag ... : count=[" + list.size() + "]");
						
							ExecutorService executor = Executors.newCachedThreadPool();
							for (int i = 0; list != null && i < list.size(); i++) {
								executor.execute(new TagAndLocationCacheTask(list.get(i)));
							};
							executor.shutdown();
							long end = System.currentTimeMillis() - start; 
							log.info("Tag cache completed : count=[" + list.size() + "], process_time=[" + end + "]ms");
							EngineLogger.info("전체 태그를 메모리에 캐싱 완료하였습니다.");
						}
					} catch (Exception ex) {
						EngineLogger.error("전체 태그를 캐싱하는 도중 오류가 발생하였습니다 : 에러=" + ex.getMessage());
						log.warn("Tag cache deploy error : " + ex.getMessage(), ex);
					}
			}
		});
			
	}

}
