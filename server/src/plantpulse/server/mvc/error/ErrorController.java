package plantpulse.server.mvc.error;

import org.springframework.stereotype.Controller;
import org.springframework.web.bind.annotation.RequestMapping;

@Controller
public class ErrorController {

	@RequestMapping(value = "/400")
	public String error400() {
		return "error/400";
	}

	@RequestMapping(value = "/404")
	public String error404() {
		return "error/404";
	}

	@RequestMapping(value = "/500")
	public String error500() {
		return "error/500";
	}

}