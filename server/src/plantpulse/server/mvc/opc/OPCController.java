package plantpulse.server.mvc.opc;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import javax.servlet.http.HttpServletRequest;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.json.JSONObject;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.HttpStatus;
import org.springframework.stereotype.Controller;
import org.springframework.ui.Model;
import org.springframework.web.bind.annotation.ModelAttribute;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.ResponseBody;
import org.springframework.web.bind.annotation.ResponseStatus;

import com.google.common.base.Preconditions;

import plantpulse.cep.service.OPCService;
import plantpulse.cep.service.RestService;
import plantpulse.cep.service.SiteService;
import plantpulse.cep.service.TagService;
import plantpulse.cep.service.VOPCService;
import plantpulse.cep.service.support.security.SecurityTools;
import plantpulse.domain.OPC;
import plantpulse.domain.TagDataTableObject;
import plantpulse.server.cache.UICache;
import plantpulse.server.mvc.JSONResult;
import plantpulse.server.mvc.util.RestUtils;

@Controller
@RequestMapping(value = "/opc")
public class OPCController {

	private static final Log log = LogFactory.getLog(OPCController.class);

	@Autowired
	private SiteService site_service;

	@Autowired
	private VOPCService vopc_service;
	
	@Autowired
	private OPCService opc_service;

	@Autowired
	private RestService rest_service;

	@Autowired
	private TagService tag_service;

	@RequestMapping(value = "/index", method = RequestMethod.GET)
	public String index(Model model, HttpServletRequest request) {
		model.addAttribute("opc_type", "OPC");
		return "opc/index";
	}

	
	@RequestMapping(value = "/opcAll")
	public @ResponseBody String opcAll(@ModelAttribute OPC opc) throws Exception {
		Preconditions.checkNotNull(opc, "Asset is null.");
		List<OPC> opcList = vopc_service.selectOpcList("OPC");
		return OPConnectionAndCountUtils.addPointCountAndConnectionStatus(opc, opcList);
	}

	@SuppressWarnings("unchecked")
	@RequestMapping(value = "/opcList")
	public @ResponseBody List<OPC> opcList(@ModelAttribute OPC opc) {
		Preconditions.checkNotNull(opc, "Asset is null.");
		
		//
		UICache cache = UICache.getInstance();
		String _CACHE_KEY = "NAV_OPC_LIST" + ":" + opc.getSite_id() + ":" +  SecurityTools.getRoleAndSecurity().getUser_id();
		if( !cache.hasObject(_CACHE_KEY) ) { //캐쉬되지 않았다면 캐쉬 처리
			List<OPC> opcList = opc_service.selectOpcs(opc);
			cache.putObject(_CACHE_KEY, opcList);
		};
		return (List<OPC>) cache.getObject(_CACHE_KEY);
	}
	
	@SuppressWarnings("unchecked")
	@RequestMapping(value = "/opcListNoCache")
	public @ResponseBody List<OPC> opcListNoCache(@ModelAttribute OPC opc) {
		Preconditions.checkNotNull(opc, "Asset is null.");
		
		List<OPC> opcList = opc_service.selectOpcs(opc);
		return opcList;
	}

	@SuppressWarnings("unchecked")
	@RequestMapping(value = "/opcListForAsset")
	public @ResponseBody List<OPC> opcListForAsset(@ModelAttribute OPC opc) {
		Preconditions.checkNotNull(opc, "Asset is null.");
		//
		UICache cache = UICache.getInstance();
		String _CACHE_KEY = "NAV_OPC_LIST_FOR_ASSET" + ":" + opc.getSite_id() + ":" + SecurityTools.getRoleAndSecurity().getUser_id();
		if( !cache.hasObject(_CACHE_KEY) ) { //캐쉬되지 않았다면 캐쉬 처리
			List<OPC> opcList = opc_service.opcListForAsset(opc);
			cache.putObject(_CACHE_KEY, opcList);
		};
		return (List<OPC>) cache.getObject(_CACHE_KEY);
	}

	@RequestMapping(value = "/addOpc")
	public String addOpc(@ModelAttribute OPC opc, Model model) {

		Preconditions.checkNotNull(opc, "OPC is null.");
		//
		opc.setOpc_type("OPC");
		opc.setOpc_server_ip("127.0.0.1");
		opc.setOpc_agent_ip("127.0.0.1");
		opc.setOpc_agent_port(60000);
		//
		model.addAttribute("site_list", site_service.selectSites());
		model.addAttribute("opc", opc);
		model.addAttribute("mode", "I");

		return "asset/opcForm";
	}

	@RequestMapping(value = "/editOpc", method = RequestMethod.POST)
	public String editOpc(@ModelAttribute OPC opc, Model model) {

		Preconditions.checkNotNull(opc, "OPC is null.");
		opc.setOpc_type("OPC");

		model.addAttribute("site_list", site_service.selectSites());
		model.addAttribute("opc", opc_service.selectOpcInfo(opc)); // OPC info
		model.addAttribute("mode", "U");
		return "asset/opcForm";
	}

	@RequestMapping(value = "/getOpcConnection")
	public @ResponseBody JSONResult getOpcConnection(@ModelAttribute OPC opc) {
		//
		Preconditions.checkNotNull(opc, "OPC is null.");

		try {
			//
			String isConnect = rest_service.getConnection(opc);
			if (!isConnect.equals("SUCCESS"))
				throw new Exception("연결정보를 확인해 주십시오.");
			return new JSONResult(isConnect, "OPC 서버에 연결하였습니다.");
		} catch (Exception e) {
			log.error("OPC Connection getting error : " + e.getMessage(), e);
			return new JSONResult(JSONResult.ERROR, "OPC 서버 연결에 실패하였습니다. : 메세지 = [" + e.getMessage() + "]");
		}
	}

	@RequestMapping(value = "/getOpcStatus")
	public @ResponseBody String getOpcStatus(@RequestParam String q) {
		//
		Preconditions.checkNotNull(q, "Query is null.");

		try {
			JSONObject param = JSONObject.fromObject(q);
			String url = "http://" + param.getString("opc_agent_ip") + ":" + param.getString("opc_agent_port") + "/api/opc/status/" + param.getString("opc_id");
			String result = RestUtils.post(url, param.toString());
			//
			Map<String, Object> data = new HashMap<String, Object>();
			data.put("status", JSONObject.fromObject(result));
			JSONResult jr = new JSONResult(JSONResult.SUCCESS, "OPC 서버의 상태를 조회하였습니다.", data);
			//
			return JSONObject.fromObject(jr).toString();
		} catch (Exception e) {
			log.error("OPC Status getting error  : " + e.getMessage(), e);
			//
			JSONResult jr = new JSONResult(JSONResult.ERROR, "OPC 상태 정보 조회에 실패하였습니다. : 메세지 = [" + e.getMessage() + "]");
			return JSONObject.fromObject(jr).toString();
		}
	}

	@RequestMapping(value = "/getOpcErros")
	public String addOpc(@RequestParam String opc_id) {
		return "opc/errors";
	}

	@RequestMapping(value = "/getOpcTagList")
	public @ResponseBody TagDataTableObject getOpcTagList(@ModelAttribute OPC opc) {
		//
		Preconditions.checkNotNull(opc, "OPC is null.");
		try {
			return rest_service.getTags(opc);
		} catch (Exception e) {
			log.error("OPC Point list getting error : " + e.getMessage(), e);
			return new TagDataTableObject();
		}
	}

	// insertOpc
	@RequestMapping(value = "/insertOpc", method = RequestMethod.POST)
	@ResponseStatus(value = HttpStatus.OK)
	public @ResponseBody JSONResult insertOpc(@RequestBody Map<String, Object> json) {
		//
		Preconditions.checkNotNull(json, "JSON Data is null.");

		try {

			//if (opc_service.duplicateCheckOpc(json)) {
			//	throw new Exception("CLS ID 또는 PROGRAM ID가 중복된 ID입니다.");
			//}
			
			String opc_id = opc_service.insertOpc(json);
			((Map<String, Object>) json.get("opc")).put("opc_id", opc_id); // PK
																			// Put

			opc_service.opcStart(json); //

			return new JSONResult(JSONResult.SUCCESS, "OPC 정보를 등록하였습니다.");
		} catch (Exception e) {
			log.error("OPC Insert error : " + e.getMessage(), e);
			return new JSONResult(JSONResult.ERROR, "OPC 정보 등록 시 오류가 발생하였습니다. : 메세지 = [" + e.getMessage() + "]");
		}
	}

	// updateOpc
	@RequestMapping(value = "/updateOpc", method = RequestMethod.POST)
	@ResponseStatus(value = HttpStatus.OK)
	public @ResponseBody JSONResult updateOpc(@RequestBody Map<String, Object> json) {
		//
		Preconditions.checkNotNull(json, "JSON Data is null.");

		try {
			opc_service.updateOpc(json);
			opc_service.opcRestart(json); // OPC Server Restart --> Rest URL로
											// Agent Server의 Restart 메소드 호출

			return new JSONResult(JSONResult.SUCCESS, "OPC 정보를 수정하였습니다.");
		} catch (Exception e) {
			log.error("OPC Update error : " + e.getMessage(), e);
			return new JSONResult(JSONResult.ERROR, "OPC 정보 수정 시 오류가 발생하였습니다. : 메세지 = [" + e.getMessage() + "]");
		}
	}

	// deleteOpc
	@RequestMapping(value = "/deleteOpc", method = RequestMethod.POST)
	public @ResponseBody JSONResult deleteOpc(@ModelAttribute OPC opc) {
		//
		Preconditions.checkNotNull(opc, "OPC is null.");

		try {
			opc_service.deleteOpc(opc);
			String isSuccess = rest_service.stopOPC(opc); // OPC Server Stop -->
															// Rest URL로 Agent
															// Server의 Stop 메소드
															// 호출
			if (!"SUCCESS".equals(isSuccess))
				throw new Exception("OPC Agent Stop Failed.");
			return new JSONResult(JSONResult.SUCCESS, "OPC 정보를 삭제하였습니다.");
		} catch (Exception e) {
			log.error("OPC Delete error : " + e.getMessage(), e);
			return new JSONResult(JSONResult.ERROR, "OPC 정보 삭제 시 오류가 발생하였습니다. : 메세지 = [" + e.getMessage() + "]");
		}
	}

	@RequestMapping(value = "/restart/{opc_id}", method = RequestMethod.POST)
	public @ResponseBody JSONResult restartOpc(@PathVariable("opc_id") String opc_id, Model model, HttpServletRequest request) throws Exception {
		try {
			OPC opc = new OPC();
			opc.setOpc_id(opc_id);
			opc = opc_service.selectOpcInfo(opc);
			Map<String, Object> param = new HashMap<String, Object>();
			param.put("opc", opc);
			opc_service.opcRestart(param); //
			return new JSONResult(JSONResult.SUCCESS, "OPC 데이터 수집기를 재시작하였습니다.");
		} catch (Exception e) {
			log.error("OPC Restart error : " + e.getMessage(), e);
			return new JSONResult(JSONResult.ERROR, "OPC 데이터 수집기를 재시작 도중 오류가 발생하였습니다. : 메세지 = [" + e.getMessage() + "]");
		}
	}

	@RequestMapping(value = "/stop/{opc_id}", method = RequestMethod.POST)
	public @ResponseBody JSONResult stopOpc(@PathVariable("opc_id") String opc_id, Model model, HttpServletRequest request) throws Exception {
		try {
			OPC opc = new OPC();
			opc.setOpc_id(opc_id);
			opc = opc_service.selectOpcInfo(opc);
			Map<String, Object> param = new HashMap<String, Object>();
			param.put("opc", opc);
			//
			String isSuccess = rest_service.stopOPC(opc); //
			if (!"SUCCESS".equals(isSuccess))
				throw new Exception("OPC Agent Stop Failed.");
			return new JSONResult(JSONResult.SUCCESS, "OPC 데이터 수집기를 정지하였습니다.");
		} catch (Exception e) {
			log.error("OPC Restart error : " + e.getMessage(), e);
			return new JSONResult(JSONResult.ERROR, "OPC 데이터 수집기를 정지 도중 오류가 발생하였습니다. : 메세지 = [" + e.getMessage() + "]");
		}
	}

}