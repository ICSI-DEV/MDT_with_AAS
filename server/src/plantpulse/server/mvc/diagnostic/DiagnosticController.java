package plantpulse.server.mvc.diagnostic;

import javax.servlet.http.HttpServletRequest;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.springframework.stereotype.Controller;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;

@Controller
public class DiagnosticController {

	private static final Log log = LogFactory.getLog(DiagnosticController.class);

	@RequestMapping(value = "/diagnostic/view", method = RequestMethod.GET)
	public String logout(HttpServletRequest request) throws Exception {
		return "diagnostic/view";
	}
}
