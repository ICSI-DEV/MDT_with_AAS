package plantpulse.plugin.opcua.messaging;

import plantpulse.plugin.opcua.server.PlantPulseNamespace;

/**
 * PluginMessageListener
 * @author lsb
 *
 */
public interface PluginMessageListener {
	
	public void init();
	
	public void startListener();
	
	public void stopListener();
	
	public void close();

};
