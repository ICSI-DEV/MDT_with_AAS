package plantpulse.plugin.aas.messaging;

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
