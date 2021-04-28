package plantpulse.server.mvc.enginemanager;

import org.springframework.stereotype.Controller;
import org.springframework.ui.Model;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;

import plantpulse.cep.engine.logging.EngineLogger;

@Controller
public class EngineLogController {

	@RequestMapping(value = "/enginemanager/log/view", method = RequestMethod.GET)
	public String view(Model model) {
		//
		model.addAttribute("size", EngineLogger.getLogs().size());
		model.addAttribute("logs", EngineLogger.getPrintableText());

		//
		return "/enginemanager/log/view";
	}

}
