package plantpulse.cep.engine.deploy;

import java.io.File;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import com.espertech.esper.client.EPServiceProvider;
import com.espertech.esper.client.deploy.DeploymentOptions;
import com.espertech.esper.client.deploy.DeploymentResult;
import com.espertech.esper.client.deploy.Module;

import plantpulse.cep.engine.logging.EngineLogger;

/**
 * ModuleDeployer
 * @author leesa
 *
 */
public class ModuleDeployer implements Deployer {

	private static final Log log = LogFactory.getLog(ModuleDeployer.class);
	
	private EPServiceProvider provider;
	private String root;
	
	public ModuleDeployer(EPServiceProvider provider, String root) {
		this.provider = provider;
		this.root = root;
	}

	public void deploy() {
		
	
		try {
			
			String base_path = root + "/WEB-INF/cep/base.epl";
			String module_path = root + "/WEB-INF/cep/module.epl";
			
			
			Module base = provider.getEPAdministrator().getDeploymentAdmin().read(new File(base_path));
			DeploymentResult base_result = provider.getEPAdministrator().getDeploymentAdmin().deploy( base, new DeploymentOptions());
			log.debug("Base deploy result = " + base_result.getDeploymentId());
			
			//
			Module module = provider.getEPAdministrator().getDeploymentAdmin().read(new File(module_path));
			DeploymentResult module_result = provider.getEPAdministrator().getDeploymentAdmin().deploy(module, new DeploymentOptions());
			log.debug("Module deploy result = " + module_result.getDeploymentId());

			EngineLogger.info("EPL 모듈을 배치하였습니다.");
			//
		} catch (Exception ex) {
			EngineLogger.error("EPL 모듈을 배치하는 도중 오류가 발생하였습니다 : 에러=" + ex.getMessage());
			log.error("EPL Module regist error : " + ex.getMessage(), ex);
		}
	}

}
