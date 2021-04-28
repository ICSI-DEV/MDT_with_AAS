package plantpulse.cep.service.support.tag.forecast;

import org.apache.commons.lang.StringUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import plantpulse.cep.engine.scheduling.job.TagPointForecastJob;
import plantpulse.domain.Tag;

public class ForecastTagDeployer {
	
	private static final Log log = LogFactory.getLog(ForecastTagDeployer.class);
	
	public void deploy(Tag tag) throws Exception{
		try{
			
			if(StringUtils.isNotEmpty(tag.getUse_ml_forecast()) && tag.getUse_ml_forecast().equals("Y")) {
				undeploy(tag);
				TagPointForecastJob job = new TagPointForecastJob(
						tag.getTag_id(),
						tag.getMl_forecast_learning_tag_id(), //
						Integer.parseInt(tag.getMl_forecast_learning_before_minutes()),      
						Integer.parseInt(tag.getMl_forecast_term()), 
						Integer.parseInt(tag.getMl_forecast_predict_count()),         
						Long.parseLong(tag.getMl_forecast_timestamp_fit())   
						);
				job.start();
				ForecastTimerFactory.getInstance().getMap().put(tag.getTag_id(), job);
				
				log.info("Forecast tag deploy completed for tag_id=[" + tag.getTag_id() + "], forecast_learning_tag_id=[" + tag.getMl_forecast_learning_tag_id() + "]");
			}
	} catch (Exception ex) {
		log.error("Forecast tag undeploy error : tag_id=[" + tag.getTag_id() + "] : " + ex.getMessage(), ex);
	}
	}
	
	public void undeploy(Tag tag) throws Exception{
		try {
			if(ForecastTimerFactory.getInstance().getMap().get(tag.getTag_id()) != null){
				ForecastTimerFactory.getInstance().getMap().get(tag.getTag_id()).stop();
				ForecastTimerFactory.getInstance().getMap().remove(tag.getTag_id());
				
				log.info("Forecast tag undeploy completed for tag_id=[" + tag.getTag_id() + "]");
			}
			//
		} catch (Exception ex) {
			log.error("Forecast tag undeploy error : tag_id=[" + tag.getTag_id() + "] : " + ex.getMessage(), ex);
		}
	}
	

}
