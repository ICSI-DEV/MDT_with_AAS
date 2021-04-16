package plantpulse.cep.engine.messaging.jaas;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import plantpulse.cep.context.EngineContext;

/**
 * JAASConfigurator
 * 
 * @author lenovo
 * 
 */
public class JAASConfigurator {

	private static final Log log = LogFactory.getLog(JAASConfigurator.class);

	private static final String JAAS_FILE_PATH = "/WEB-INF/classes/jaas.config";

	public void config() throws Exception {
		//
		try {
			//
			String base_path = EngineContext.getInstance().getProperty("_ROOT");
			System.setProperty("java.security.auth.login.config", base_path + JAAS_FILE_PATH);

			log.info("JAAS configuration path setted : " + System.getProperty("java.security.auth.login.config"));

		} catch (Exception ex) {
			throw ex;
		}
	}

}
