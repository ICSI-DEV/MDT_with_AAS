package plantpulse.server.mvc.opc.api;

import java.util.List;

import javax.servlet.http.HttpServletRequest;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.HttpStatus;
import org.springframework.stereotype.Controller;
import org.springframework.ui.Model;
import org.springframework.web.bind.annotation.ModelAttribute;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.ResponseBody;
import org.springframework.web.bind.annotation.ResponseStatus;

import com.google.common.base.Preconditions;

import plantpulse.cep.service.SiteService;
import plantpulse.cep.service.TagService;
import plantpulse.cep.service.VOPCService;
import plantpulse.domain.OPC;
import plantpulse.domain.Tag;
import plantpulse.server.mvc.JSONResult;
import plantpulse.server.mvc.opc.OPConnectionAndCountUtils;

@Controller
public class OAPIController {

	private static final Log log = LogFactory.getLog(OAPIController.class);

	private static final String OPC_TYPE = "API";

	@Autowired
	private SiteService site_service;

	@Autowired
	private VOPCService vopc_service;
	
	@Autowired
	private TagService tag_service;

	@RequestMapping(value = "/oapi/index", method = RequestMethod.GET)
	public String index(Model model, HttpServletRequest request) {
		model.addAttribute("opc_type", OPC_TYPE);
		return "oapi/index";
	}

	@RequestMapping(value = "/oapi/list")
	public @ResponseBody String list(@ModelAttribute OPC opc) {
		List<OPC> opcList = null;
		try {
			opcList = vopc_service.selectOpcList(OPC_TYPE);
		} catch (Exception e) {
			log.error("List Exception : " + e.getMessage(), e);
		}
		return OPConnectionAndCountUtils.addPointCountAndConnectionStatus(opc, opcList);
	}

	@RequestMapping(value = "/oapi/add", method = RequestMethod.GET)
	public String add(@ModelAttribute OPC opc, Model model) {
		Preconditions.checkNotNull(opc, "OPC is null.");
		//
		model.addAttribute("site_list", site_service.selectSites());
		model.addAttribute("opc", opc);
		model.addAttribute("mode", "I");

		return "oapi/form";
	}

	@RequestMapping(value = "/oapi/edit", method = RequestMethod.POST)
	public String edit(@ModelAttribute OPC opc, Model model) {
		try {
			Preconditions.checkNotNull(opc, "OPC is null.");
			//
			opc = vopc_service.selectOpc(opc);
			//
			model.addAttribute("site_list", site_service.selectSites());
			model.addAttribute("opc", opc);
			model.addAttribute("mode", "U");
		} catch (Exception e) {
			log.error("Edit Exception : " + e.getMessage(), e);
		}
		return "oapi/form";
	}

	@RequestMapping(value = "/oapi/insert", method = RequestMethod.POST)
	@ResponseStatus(HttpStatus.OK)
	public @ResponseBody JSONResult insert(@ModelAttribute OPC opc, Model model, HttpServletRequest request) {
		try {

			opc.setOpc_type(OPC_TYPE);
			vopc_service.insertVOPC(opc);
			//
			return new JSONResult(JSONResult.SUCCESS, "API 연결을 저장하였습니다.");
		} catch (Exception e) {
			log.error("Insert Exception : " + e.getMessage(), e);
			return new JSONResult(JSONResult.ERROR, "API 연결을 저장할 수 없습니다. : 메세지 = [" + e.getMessage() + "]");
		}
	}

	@RequestMapping(value = "/oapi/update", method = RequestMethod.POST)
	@ResponseStatus(HttpStatus.OK)
	public @ResponseBody JSONResult update(@ModelAttribute OPC opc, Model model, HttpServletRequest request) {
		try {
			opc.setOpc_type(OPC_TYPE);
			vopc_service.updateVOPC(opc);
			//
			return new JSONResult(JSONResult.SUCCESS, "API 연결을 저장하였습니다.");
		} catch (Exception e) {
			log.error("Insert Exception : " + e.getMessage(), e);
			return new JSONResult(JSONResult.ERROR, "API 연결을 저장할 수 없습니다. : 메세지 = [" + e.getMessage() + "]");
		}
	}

	@RequestMapping(value = "/oapi/delete/{opc_id}", method = RequestMethod.POST)
	@ResponseStatus(HttpStatus.OK)
	public @ResponseBody JSONResult delete(@PathVariable String opc_id, Model model, HttpServletRequest request) {
		try {
			
			//1. 연관 태그 삭제
			List<Tag> opc_tag_list = tag_service.selectTagListByOpcId(opc_id);
			tag_service.deleteTagList(opc_tag_list);
			
			//
			OPC opc = new OPC();
			opc.setOpc_id(opc_id);
			vopc_service.deleteVOPC(opc);
			
			//
			return new JSONResult(JSONResult.SUCCESS, "API 연결을 삭제하였습니다.");
		} catch (Exception e) {
			log.error("Insert Exception : " + e.getMessage(), e);
			return new JSONResult(JSONResult.ERROR, "API 연결을 삭제할 수 없습니다. : 메세지 = [" + e.getMessage() + "]");
		}
	}

}