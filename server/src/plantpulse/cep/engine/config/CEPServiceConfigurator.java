package plantpulse.cep.engine.config;

import java.io.File;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import com.espertech.esper.client.Configuration;

import plantpulse.cep.engine.logging.EngineLogger;
import plantpulse.cep.engine.utils.EngineConstants;

/**
 * CEPServiceConfigurator
 * 
 * @author lenovo
 *
 */
public class CEPServiceConfigurator {

	private static final Log log = LogFactory.getLog(CEPServiceConfigurator.class);

	public static Configuration createConfiguration(String root) {
		Configuration configuration = new Configuration();
		String config_path = root + EngineConstants.ENGINE_CONFIG_XML_PATH;
		try {
			log.info("CEPService configuration path : path=[" + config_path + "]");
			configuration.configure(new File(config_path)); //
			configuration.addImport("plantpulse.event");
			EngineLogger.info("서비스 설정을 완료하였습니다.");

		} catch (Exception ex) {
			EngineLogger.error("서비스 설정 도중 오류가 발생하였습니다 : 에러=" + config_path + " : " + ex.getMessage());
			log.error("ServerConfiguration error : " + ex.getMessage(), ex);
		}
		return configuration;
	}
}
