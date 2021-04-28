package plantpulse.plugin.opcua;

import org.apache.log4j.Logger;

import plantpulse.plugin.opcua.messaging.PluginMessageListener;
import plantpulse.plugin.opcua.server.PlantPulseNamespace;
import plantpulse.plugin.opcua.server.PlantPulseServer;

public class ProcessorHook extends Thread {

	private static final Logger log = Logger.getLogger(ProcessorHook.class);

	private PlantPulseServer server;
	private PlantPulseNamespace namespace;
	private PluginMessageListener p_listener = null;

	public ProcessorHook(PlantPulseServer server, PlantPulseNamespace namespace, PluginMessageListener p_listener) {
		this.server = server;
		this.namespace = namespace;
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

			if (namespace != null)
				namespace.shutdown();
			if (server != null)
				server.shutdown();
			log.info("OPC-UA 서버 및 네임스페이스가 셧다운 되었습니다.");

		} catch (Exception e) {
			log.error("수집기 셧다운 후크 에러 : " + e.getMessage(), e);
		}
	}
};