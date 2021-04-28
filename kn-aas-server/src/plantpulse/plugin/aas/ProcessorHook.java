package plantpulse.plugin.aas;

import org.apache.log4j.Logger;

import plantpulse.plugin.aas.messaging.PluginMessageListener;
import plantpulse.plugin.aas.server.AASServer;


public class ProcessorHook extends Thread {

	private static final Logger log = Logger.getLogger(ProcessorHook.class);

	private AASServer server;
	private PluginMessageListener p_listener = null;

	public ProcessorHook(AASServer server,  PluginMessageListener p_listener) {
		this.server = server;
		this.p_listener = p_listener;
	}

	@Override
	public void run() {
		try {
			log.info("수집기를 셧다운 중입니다 ...");
			Thread.sleep(5 * 1000);

			if (p_listener != null) {
				p_listener.stopListener();
				p_listener.close();
			};

			
			if (server != null)
				server.stop();
			log.info("AAS 서버 및 네임스페이스가 셧다운 되었습니다.");

		} catch (Exception e) {
			log.error("수집기 셧다운 후크 에러 : " + e.getMessage(), e);
		}
	}
};