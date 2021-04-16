package plantpulse.cep.engine.eventbus;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import plantpulse.cep.dao.BackupDAO;
import plantpulse.cep.engine.CEPEngineManager;
import plantpulse.cep.engine.logging.EngineLogger;
import plantpulse.cep.engine.timer.TimerExecuterPool;
import plantpulse.server.cache.UICache;

/**
 * DomainChangeSyncTimer
 * 
 * <pre>
 * 도매인 변경 동기화 타이머
 * </pre>
 * 
 * @author lsb
 *
 */
public class DomainChangeSyncTimer {

	private static final Log log = LogFactory.getLog(DomainChangeSyncTimer.class);

	private static final int TIMER_PERIOD = (1000 * 60) * 1; // 1분마다 실행

	private ScheduledFuture<?> task;

	public void start() {
		task = TimerExecuterPool.getInstance().getPool().scheduleAtFixedRate(new Runnable() {

			@Override
			public void run() {
				//
				try {
					// 변경된 도매인이 있을 경우, 사이트 메타 모델을 동기화 한다. (백업도 할까요?)
					long changed_count = DomainChangeQueue.getInstance().size();
					if (changed_count > 0) {

						ExecutorService executor = Executors.newFixedThreadPool(1);
						executor.execute(new Runnable() {
							@Override
							public void run() {
								long start = System.currentTimeMillis();
								try {
									// 사이트 모델 업데이트
									log.info("Changed domain object id array = " + DomainChangeQueue.getInstance().getQueue().toString());
									CEPEngineManager.getInstance().getSite_meta_model_updater().update();
									DomainChangeQueue.getInstance().init();

									// 캐쉬 클리어
									UICache.getInstance().cleanUp();

									// DB 백업
									BackupDAO dao = new BackupDAO();
									dao.backup();

									//
									long end = System.currentTimeMillis() - start;
									EngineLogger.info("도매인 [" + changed_count + "]개가 변경되어 도매인 변경 모델을 동기화하였습니다. 동기화 소요 시간=[" + (end / 1000) + "]초");
								} catch (Exception ex) {
									log.error("DomainChangeSyncTimer async exectuor error : " + ex.getMessage(), ex);
								}
							}

						});
						executor.shutdown();
					}
					;
					//
				} catch (Exception e) {
					log.error("DomainChangeSyncTimer error : " + e.getMessage(), e);
				}

			}

		}, 1000 * 60 * 1, TIMER_PERIOD, TimeUnit.MILLISECONDS);
	}

	public void stop() {
		task.cancel(true);
	}

}
