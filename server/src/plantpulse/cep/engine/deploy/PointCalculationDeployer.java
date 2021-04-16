package plantpulse.cep.engine.deploy;

import java.util.List;

import org.apache.commons.lang.StringUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import plantpulse.cep.dao.TagDAO;
import plantpulse.cep.engine.logging.EngineLogger;
import plantpulse.cep.service.support.tag.calculation.TagCalculationDeployer;
import plantpulse.domain.Tag;

/**
 * 포인트 계산 (계산 태그) 설정을 배포한다.
 * @author lsb
 *
 */
public class PointCalculationDeployer implements Deployer {

	private static final Log log = LogFactory.getLog(PointCalculationDeployer.class);

	public void deploy() {
		
		try {
			log.info("Point calculation deploy started...");
			
			//태그에 정의된 계산 로직 적용
			TagDAO tag_dao = new TagDAO(); //
			List<Tag> tag_list = tag_dao.selectTagAll();
			//
			for(int i=0; tag_list != null && i < tag_list.size(); i++){
				Tag tag = tag_list.get(i);
				if(StringUtils.isNotEmpty(tag.getUse_calculation()) && tag.getUse_calculation().equals("Y")){
					TagCalculationDeployer deployer = new TagCalculationDeployer();
					deployer.deploy(tag);
				};
		   };
		   
		} catch (Exception ex) {
			EngineLogger.error("태그  포인트 계산 설정을 배치하는 도중 오류가 발생하였습니다 : 에러=" + ex.getMessage());
			log.warn("Point calculationn deploy error : " + ex.getMessage(), ex);
		}
	}
}
