package plantpulse.server.mvc.test;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.springframework.stereotype.Controller;
import org.springframework.ui.Model;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;

@Controller
public class TestController {

	private static final Log log = LogFactory.getLog(TestController.class);

	@RequestMapping(value = "/test/stomp", method = RequestMethod.GET)
	public String stomp(Model model) throws Exception {
		return "test/stomp";
	}

	@RequestMapping(value = "/test/mqtt", method = RequestMethod.GET)
	public String mqtt(Model model) throws Exception {
		return "test/mqtt";
	}
}
