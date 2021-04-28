package plantpulse.cep.engine.recovery;

import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import plantpulse.cep.engine.deploy.OPCAgentRecoveryDeployer;
import plantpulse.cep.engine.timer.TimerExecuterPool;

/**
 * OPCAgentRecoveryTimer
 * @author leesa
 *
 */
public class OPCAgentRecoveryTimer {

	private static final Log log = LogFactory.getLog(OPCAgentRecoveryTimer.class);

	private ScheduledFuture<?> timer = null;

	public OPCAgentRecoveryTimer() {

	}

	public void start() {
		timer = TimerExecuterPool.getInstance().getPool().scheduleAtFixedRate(new Runnable() {
			@Override
			public void run() {
				// OPC Agent 시작
				OPCAgentRecoveryDeployer recovery = new OPCAgentRecoveryDeployer();
				recovery.deploy();
			}
		}, 0, 1000 * 120, TimeUnit.MILLISECONDS); // 서버시작 완료 후, Fire 되어야 함. 2분 셋업
		log.info("Agent recovery timer is started.");
	}

	public void stop() {
		timer.cancel(true);
	}

}
