package plantpulse.server.mvc.server;

import javax.servlet.http.HttpServletRequest;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Controller;
import org.springframework.ui.Model;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;

import plantpulse.cep.service.OPCService;

@Controller
public class ServerController {

	private static final Log log = LogFactory.getLog(ServerController.class);

	@Autowired
	private OPCService opc_service;

	@RequestMapping(value = "/server/index", method = RequestMethod.GET)
	public String index(Model model, HttpServletRequest request) throws Exception {

		model.addAttribute("opc_list", opc_service.selectOpcList());

		return "server/index";
	}

}