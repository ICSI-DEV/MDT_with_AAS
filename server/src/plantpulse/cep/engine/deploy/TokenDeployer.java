package plantpulse.cep.engine.deploy;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import plantpulse.cep.engine.logging.EngineLogger;
import plantpulse.cep.service.TokenService;

/**
 * TokenDeployer
 * 
 * @author leesa
 *
 */
public class TokenDeployer  implements Deployer  {

	private static final Log log = LogFactory.getLog(TokenDeployer.class);

	private TokenService token_service = new TokenService();
	
	public void deploy() {
		try {
			token_service.deploy();
		} catch (Exception ex) {
			EngineLogger.error("API 인증 토큰을 배포하는  도중 오류가 발생하였습니다 : 에러=" + ex.getMessage());
			log.warn("Trigger deploy error : " + ex.getMessage(), ex);
		}
	}

}
