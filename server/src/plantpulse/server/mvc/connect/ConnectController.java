package plantpulse.server.mvc.connect;

import javax.servlet.http.HttpServletRequest;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Controller;
import org.springframework.ui.Model;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;

import plantpulse.cep.service.OPCService;

/**
 * ConnectController
 * 
 * @author lsb
 *
 */
@Controller
public class ConnectController {

	private static final Log log = LogFactory.getLog(ConnectController.class);
	

	@Autowired
	private OPCService opc_service = new OPCService();
	

	@RequestMapping(value = "/connect", method = RequestMethod.GET)
	public String main(Model model, HttpServletRequest request) throws Exception {
		return index(model, request);
	}

	@RequestMapping(value = "/connect/index", method = RequestMethod.GET)
	public String index(Model model, HttpServletRequest request) throws Exception {
		//
		return "connect/index";
	}
	
	
	@RequestMapping(value = "/connect/status", method = RequestMethod.GET)
	public String status(Model model, HttpServletRequest request) throws Exception {
		//
		model.addAttribute("total_connection_count", opc_service.selectOpcCount());
		//
		model.addAttribute("opc_list", opc_service.selectOpcListByType("OPC"));
		model.addAttribute("plc_list", opc_service.selectOpcListByType("PLC"));
		model.addAttribute("modbus_list", opc_service.selectOpcListByType("MODBUS"));
		model.addAttribute("database_list", opc_service.selectOpcListByType("DATABASE"));
		model.addAttribute("file_list", opc_service.selectOpcListByType("FILE"));
		
		//
		return "connect/status";
	}
	
	

}
