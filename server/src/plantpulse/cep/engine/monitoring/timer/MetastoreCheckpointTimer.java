package plantpulse.cep.engine.monitoring.timer;

import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import plantpulse.cep.dao.CheckpointDAO;
import plantpulse.cep.engine.timer.TimerExecuterPool;
/**
 * MetastoreCheckpointTimer
 * 
 * @author leesa
 *
 */
public class MetastoreCheckpointTimer implements MonitoringTimer{

	private static final Log log = LogFactory.getLog(MetastoreCheckpointTimer.class);

	private static final int TIMER_PERIOD = (1000 * 60); // 1분마다 마다 실행

	private ScheduledFuture<?> task;
	

	public void start() {
		task = TimerExecuterPool.getInstance().getPool().scheduleAtFixedRate(new Runnable() {
			@Override
			public void run() {
				try {
					CheckpointDAO dao = new CheckpointDAO();
					dao.checkpoint();
					log.debug("Metastore(HSQLDB) checkpoint completed.");
				} catch (Exception e) {
					log.error("Metastore(HSQLDB) checkpoint error : " + e.getMessage() , e);
				}

			}

		}, TIMER_PERIOD, TIMER_PERIOD, TimeUnit.MILLISECONDS);

		log.info("Metastore(HSQLDB) checkpoint timer started.");

	}

	public void stop() {
		task.cancel(true);
	};


}
