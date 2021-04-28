package plantpulse.cep.engine.dds;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import plantpulse.cep.engine.dds.suppport.BaseDataDistributionService;
import plantpulse.cep.engine.properties.PropertiesLoader;


/**
 * DataDistributionFactory
 * @author leesa
 *
 */
public class DataDistributionFactory {
	
private static final Log log = LogFactory.getLog(DataDistributionFactory.class);
	
	private static class DataDistributionInitializerHolder {
		static DataDistributionFactory instance = new DataDistributionFactory();
	}

	public static DataDistributionFactory getInstance() {
		return DataDistributionInitializerHolder.instance;
	}

	private DataDistributionService dds;

	/**
	 * DDS 초기화
	 */
	public void init() {
		//
		boolean a = Boolean.parseBoolean(PropertiesLoader.getEngine_properties().getProperty("engine.data.tag.distribution.enabled", "true"));
		boolean b = Boolean.parseBoolean(PropertiesLoader.getEngine_properties().getProperty("engine.data.asset.distribution.enabled", "true"));
		dds = new BaseDataDistributionService(a,b);
		log.info("DDS inited.");
	}
	
	/**
	 * getDataDistributionService
	 * @return
	 */
	public DataDistributionService getDataDistributionService() {
		return dds;
	}

}
