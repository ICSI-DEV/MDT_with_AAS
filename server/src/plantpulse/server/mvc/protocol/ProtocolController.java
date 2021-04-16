package plantpulse.server.mvc.protocol;

import javax.servlet.http.HttpServletRequest;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.springframework.stereotype.Controller;
import org.springframework.ui.Model;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;

@Controller
@RequestMapping(value = "/protocol")
public class ProtocolController {

	private static final Log log = LogFactory.getLog(ProtocolController.class);

	@RequestMapping(value = "/index", method = RequestMethod.GET)
	public String index(Model model, HttpServletRequest request) {
		return "protocol/index";
	}

}