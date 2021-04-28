package plantpulse.cep.engine.model;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import plantpulse.cep.engine.CEPEngineManager;
import plantpulse.cep.engine.logging.EngineLogger;

/**
 * 사이트 메타 모델 업데이터
 * 
 * <pre>
 * 
 * 사이트의 논리 구조가 확정되면, 사이트 모델 확정 작업을 통하여 아래 타스크들을 순차적으로  실행하여
 * 메타 모델을 카산드라 데이터베이스로 저장한다.
 * 
 * 1. 사이트, 에셋, 태그, 유저, 보안 등의 메타 테이블을 카산드라로 복제
 * 2. 사이트 모델 JSON 데이터 생성 및 저장
 *    - SITE_MODEL
 *    - SITE_MODEL_BY_ROLE
 *    - SITE_MODEL_BY_USER
 *    
 * </pre>
 * 
 * @author lsb
 *
 */
public class SiteMetaModelManager {
	
	private static final Log log = LogFactory.getLog(SiteMetaModelManager.class);
	
	/**
	 *  사이트, 에셋, 태그, 유저, 보안, 추가 JSON 모델 등의 메타 테이블을 카산드라로 복제
	 *  
	 * @throws Exception
	 */
	public void updateModel() throws Exception {
		
		try{ 
			
			//
			CEPEngineManager.getInstance().getSite_meta_model_updater().update();

			//
			EngineLogger.info("사이트 구조 모델을 업데이트하였습니다.");
			
			
		}catch(Exception ex){
			EngineLogger.error("사이트 구조 모델을 업데이트하는 도중 오류가 발생하였습니다 : 에러=" + ex.getMessage());
			log.error("Site metadata model update failed : " + ex.getMessage(),  ex);
		}
		
	}


}
