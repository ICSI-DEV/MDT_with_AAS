package plantpulse.cep.query;

import java.util.Iterator;
import java.util.Map;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import com.espertech.esper.client.EPStatement;

import plantpulse.cep.domain.Query;
import plantpulse.cep.engine.CEPEngineManager;
import plantpulse.cep.engine.timer.TimerExecuterPool;

public class QueryExpireTimer {

	private static final Log log = LogFactory.getLog(QueryExpireTimer.class);

	private static final int TIMER_PERIOD = (1000 * 30) * 1; // 1분

	private ScheduledFuture<?> timer;

	public void start() {
		timer = TimerExecuterPool.getInstance().getPool().scheduleAtFixedRate(new Runnable() {

			@Override
			public void run() {
				QueryFactory factory = QueryFactory.getInstance();
				Map<String, Query> map = factory.getMap();
				synchronized (map) {
					Iterator<String> keys = map.keySet().iterator();
					while (keys.hasNext()) {
						String key = keys.next();
						Query query = map.get(key);
						if (query.getInsert_date() != null) {
							if ((System.currentTimeMillis() - query.getInsert_date().getTime()) >= (query.getExpire_time() * 1000)) {
								EPStatement stmt = CEPEngineManager.getInstance().getProvider().getEPAdministrator().getStatement(query.getId());
								if (stmt != null && !stmt.isDestroyed()) {
									stmt.stop();
									stmt.destroy();
									factory.removeQuery(query);
									log.info("Query destroyed from timer : id=[" + query.getId() + "]");
								}
							}
						}
					}
				}
			}

		}, 1000 * 30, TIMER_PERIOD, TimeUnit.MILLISECONDS); // 1분에 한번씩
	}

	public void stop() {
		timer.cancel(true);
	}

}
