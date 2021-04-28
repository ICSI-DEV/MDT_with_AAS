package plantpulse.server.mvc.performance;

import org.springframework.stereotype.Controller;
import org.springframework.ui.Model;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;

@Controller
public class PerformanceController {

	@RequestMapping(value = "/performance/index", method = RequestMethod.GET)
	public String index(Model model) {
		// model.addAttribute(new Account());
		return "performance/index";
	}

}
