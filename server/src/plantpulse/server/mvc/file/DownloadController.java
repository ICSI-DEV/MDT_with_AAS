package plantpulse.server.mvc.file;

import javax.servlet.http.HttpServletRequest;

import org.springframework.stereotype.Controller;
import org.springframework.ui.Model;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;

@Controller
public class DownloadController {

	@RequestMapping(value = "/file/download", method = RequestMethod.GET, headers = "Accept=*/*", produces = "application/json")
	public String total(Model model, HttpServletRequest request) throws Exception {
		return "file/download";
	}

}
