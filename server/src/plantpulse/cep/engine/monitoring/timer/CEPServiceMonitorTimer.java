package plantpulse.cep.engine.monitoring.timer;

import java.util.List;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.json.JSONObject;

import com.espertech.esper.client.EPServiceProvider;
import com.espertech.esper.client.EPStatement;

import plantpulse.cep.engine.monitoring.MonitorConstants;
import plantpulse.cep.engine.monitoring.event.OSPerformance;
import plantpulse.cep.engine.monitoring.statement.Statement;
import plantpulse.cep.engine.monitoring.statement.StatementList;
import plantpulse.cep.engine.os.OSPerformanceUtils;
import plantpulse.cep.engine.stream.processor.PointStreamStastics;
import plantpulse.cep.engine.stream.type.queue.PointQueueFactory;
import plantpulse.cep.engine.timer.TimerExecuterPool;
import plantpulse.cep.listener.ResultServiceManager;
import plantpulse.cep.listener.push.PushClient;

/**
 * EngineMonitor
 * 
 * @author lsb
 *
 */
public class CEPServiceMonitorTimer  implements MonitoringTimer {
	
	private static final Log log = LogFactory.getLog(CEPServiceMonitorTimer.class);

	private String serviceId;
	private EPServiceProvider provider;
	
	public CEPServiceMonitorTimer(String serviceId, final EPServiceProvider provider) {
		this.serviceId = serviceId;
		this.provider = provider;
	}

	private ScheduledFuture<?> task;

	@Override
	public void start()  throws Exception {

		List<Statement> list = StatementList.getList(serviceId);
		for (int i = 0; i < list.size(); i++) {
			Statement stmt = list.get(i);
			EPStatement ep_statement = provider.getEPAdministrator().createEPL(stmt.getEPL(), stmt.getName());
			ep_statement.addListener(stmt);
		}

		try {
			
			task = TimerExecuterPool.getInstance().getPool().scheduleAtFixedRate(new Runnable() {
				@Override
				public void run() {
					
					// OS 성능 전달
					OSPerformance of = OSPerformanceUtils.getOSPerformance();
					provider.getEPRuntime().sendEvent(of);
					
					//펜딩 큐 및 쓰레드 정보 정보 전달
					int pendding_queue_size = PointQueueFactory.getCEPDataPointQueue("CEP_SERVICE_MONITOR").getQueue().size();
					JSONObject json = new JSONObject();
					json.put("timestamp", System.currentTimeMillis());
					json.put("pendding_queue_size", pendding_queue_size);
					json.put("running_trhead_count", PointStreamStastics.RUNNING_THREAD);
					
					try {
						PushClient client1 = ResultServiceManager.getPushService().getPushClient(MonitorConstants.PUSH_URL_STREAMING);
						client1.sendJSON(json);
					} catch (Exception e) {
						log.error("CEPService mointor event timer failed : " + e.getMessage());
					}

				}
			}, 1000, 1000, TimeUnit.MILLISECONDS);
			
		} catch (Exception e) {
			e.printStackTrace();
		}

	}


	@Override
	public void stop() {
		List<Statement> list = StatementList.getList(serviceId);
		for (int i = 0; i < list.size(); i++) {
			Statement stmt = list.get(i);
			EPStatement ep_statement = provider.getEPAdministrator().getStatement(stmt.getName());
			ep_statement.destroy();
		}
		task.cancel(true);
	}





}
