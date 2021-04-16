package plantpulse.server.mvc.application;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.springframework.stereotype.Controller;

import plantpulse.cep.service.ApplicationService;

@Controller
public class ApplicationController {

	private static final Log log = LogFactory.getLog(ApplicationController.class);

	private ApplicationService applicationService = new ApplicationService();

}