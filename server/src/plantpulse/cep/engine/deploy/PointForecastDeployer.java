package plantpulse.cep.engine.deploy;

import java.util.List;

import org.apache.commons.lang.StringUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import plantpulse.cep.dao.TagDAO;
import plantpulse.cep.engine.logging.EngineLogger;
import plantpulse.cep.engine.scheduling.job.TagPointForecastJob;
import plantpulse.cep.service.support.tag.forecast.ForecastTagDeployer;
import plantpulse.domain.Tag;

public class PointForecastDeployer implements Deployer {
	
	private static final Log log = LogFactory.getLog(TagPointForecastJob.class);

	public void deploy() {
		try {
			
			TagDAO tag_dao = new TagDAO();
			List<Tag> tag_list = tag_dao.selectTagAll();
			//
			for(int i=0 ; i < tag_list.size(); i++){
				try{
					Tag tag = tag_list.get(i);
					if(StringUtils.isNotEmpty(tag.getUse_ml_forecast()) && tag.getUse_ml_forecast().equals("Y")) {
						ForecastTagDeployer deployer = new ForecastTagDeployer();
						deployer.deploy(tag);
					}
				}catch(Exception ex){
					log.error("Point forecast job start error : " + ex.getMessage(), ex);
				}
			}
			
			log.info("Point forecast jobs deployed.");

		} catch (Exception ex) {
			EngineLogger.error("태그 포인트 예측  처리기를 배치하는 도중 오류가 발생하였습니다 : 에러=" + ex.getMessage());
			log.error("Point forecast jobs deploy error : " + ex.getMessage(), ex);
		}
	}

	
	

}
