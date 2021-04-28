package plantpulse.server.mvc.server;

import javax.servlet.http.HttpServletRequest;

import org.json.JSONObject;
import org.springframework.stereotype.Controller;
import org.springframework.ui.Model;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.ResponseBody;

import plantpulse.cep.listener.ResultServiceManager;
import plantpulse.cep.listener.push.PushClient;

@Controller
public class ServerStatusController {

	// private static final Log log =
	// LogFactory.getLog(ServerStatusController.class);

	public static final String WS_BASE_PATH = "/server/status";

	@RequestMapping(value = "/server/status", method = RequestMethod.GET)
	public @ResponseBody String main(Model model, HttpServletRequest request) throws Exception {
		String q = request.getParameter("q");
		JSONObject json = JSONObject.fromObject(q);
		//
		String agent = (String) json.get("agent");
		//
		PushClient client = ResultServiceManager.getPushService().getPushClient(WS_BASE_PATH + "/" + agent);
		client.sendJSON(json);

		//
		return "OK";
	}
};