package plantpulse.cep.service.support.tag.forecast;

import java.util.HashMap;
import java.util.Map;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import plantpulse.cep.engine.scheduling.job.TagPointForecastJob;

public class ForecastTimerFactory {
	
	private static final Log log = LogFactory.getLog(ForecastTimerFactory.class);

	private static class ForecastTimerFactoryHolder {
		static ForecastTimerFactory instance = new ForecastTimerFactory();
	}

	public static ForecastTimerFactory getInstance() {
		return ForecastTimerFactoryHolder.instance;
	}
	
	private Map<String, TagPointForecastJob> map = new HashMap<String, TagPointForecastJob>();

	
	public Map<String, TagPointForecastJob> getMap() {
		return map;
	}

	public void setMap(Map<String, TagPointForecastJob> map) {
		this.map = map;
	}
	
	

}
