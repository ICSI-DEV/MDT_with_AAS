package plantpulse.server.mvc.scada;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import javax.servlet.http.HttpServletRequest;

import org.apache.commons.lang.StringUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.realdisplay.framework.util.FileUtils;
import org.springframework.stereotype.Controller;
import org.springframework.ui.Model;
import org.springframework.validation.BindingResult;
import org.springframework.web.bind.annotation.ModelAttribute;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.ResponseBody;

import plantpulse.cep.domain.SCADA;
import plantpulse.cep.service.SACADAService;
import plantpulse.cep.service.SecurityService;
import plantpulse.cep.service.UserService;
import plantpulse.domain.Security;
import plantpulse.server.mvc.CRUDKeys;
import plantpulse.server.mvc.JSONResult;
import plantpulse.server.mvc.Result;
import plantpulse.server.mvc.upload.UploadItem;
import plantpulse.server.mvc.util.SessionUtils;
import plantpulse.server.mvc.util.UploadUtils;

@Controller
public class SCADAController {

	private static final Log log = LogFactory.getLog(SCADAController.class);

	private SACADAService scada_service = new SACADAService();
	
	private SecurityService security_service = new SecurityService();
	private UserService user_service = new UserService();

	@RequestMapping(value = "/scada", method = RequestMethod.GET)
	public String main(Model model, HttpServletRequest request) throws Exception {
		return index(model, request);
	}

	@RequestMapping(value = "/scada/index", method = RequestMethod.GET)
	public String index(Model model, HttpServletRequest request) throws Exception {
		List<SCADA> scada_list = scada_service.getSCADAListByUserId(SessionUtils.getUserFromSession(request).getUser_id());
		List<SCADA> share_list = new ArrayList<SCADA>();
		//
		if (scada_list == null || scada_list.size() < 1) { //
			SCADA scada = new SCADA();
			scada.setScada_id("SCADA_" + System.currentTimeMillis());
			scada.setScada_title("기본 스카다");
			scada.setScada_desc("");
			scada.setInsert_user_id(SessionUtils.getUserFromSession(request).getUser_id());
			//
			scada_service.insertSCADA(scada);
		    //
			scada_list = scada_service.getSCADAListByUserId(SessionUtils.getUserFromSession(request).getUser_id());
			
			
		}
		//
		share_list = scada_service.getScadaListBySecurityId(user_service.getUser(SessionUtils.getUserFromSession(request).getUser_id()).getSecurity_id());
		
		//
		SCADA default_scada = null;
		if(share_list.size() == 0){
			default_scada = scada_list.get(0);
		}else{
			default_scada = share_list.get(0); //공유된 스카다가 있으면 먼저 표시
		}
		model.addAttribute("scada_list", scada_list);
		model.addAttribute("share_list", share_list);
		//
		model.addAttribute("scada", scada_service.getSCADAById(default_scada.getScada_id()));
		return "scada/index";
	}

	@RequestMapping(value = "/scada/view/{scada_id}", method = RequestMethod.GET)
	public String view(@PathVariable("scada_id") String scada_id, Model model, HttpServletRequest request) throws Exception {
		//
		List<SCADA> scada_list = scada_service.getSCADAListByUserId(SessionUtils.getUserFromSession(request).getUser_id());
		List<SCADA> share_list = scada_service.getScadaListBySecurityId(user_service.getUser(SessionUtils.getUserFromSession(request).getUser_id()).getSecurity_id());
		model.addAttribute("scada_list", scada_list);
		model.addAttribute("share_list", share_list);
		//
		model.addAttribute("scada", scada_service.getSCADAById(scada_id));
		return "scada/index";
	}
	
	@RequestMapping(value = "/scada/screen/{scada_id}", method = RequestMethod.GET)
	public String screen(@PathVariable("scada_id") String scada_id, Model model, HttpServletRequest request) throws Exception {
		List<SCADA> scada_list = scada_service.getSCADAListByUserId(SessionUtils.getUserFromSession(request).getUser_id());
		model.addAttribute("scada_list", scada_list);
		//
		model.addAttribute("scada", scada_service.getSCADAById(scada_id));
		return "scada/screen";
	}

	@RequestMapping(value = "/scada/add", method = RequestMethod.GET)
	public String add(Model model) throws Exception {
		SCADA scada = new SCADA();
		scada.setMode(CRUDKeys.INSERT);
		model.addAttribute("scada", scada);
		
		List<Security> security_list = security_service.getSecurityList();
		model.addAttribute("security_list", security_list);
		
		return "scada/form";
	}

	@RequestMapping(value = "/scada/edit/{scada_id}", method = RequestMethod.GET)
	public String edit(@ModelAttribute("scada") SCADA scada, Model model) throws Exception {
		SCADA target_scada = scada_service.getSCADAById(scada.getScada_id());
		target_scada.setMode(CRUDKeys.UPDATE);
		model.addAttribute("scada", target_scada);
		
		List<Security> security_list = security_service.getSecurityList();
		model.addAttribute("security_list", security_list);
		
		return "scada/form";
	}

	@RequestMapping(value = "/scada/save", method = RequestMethod.POST)
	public @ResponseBody SCADA save(@ModelAttribute("scada") SCADA scada, BindingResult br, Model model, HttpServletRequest request) throws Exception {
		if (StringUtils.isEmpty(scada.getScada_id())) { //
			scada.setScada_id("SCADA_" + System.currentTimeMillis());
			scada.setInsert_user_id(SessionUtils.getUserFromSession(request).getUser_id());
			model.addAttribute("scada", scada);
			//
			scada_service.insertSCADA(scada);
		} else { //
			model.addAttribute("scada", scada);
			scada_service.updateSCADA(scada);
		}
		return scada;
	}

	@RequestMapping(value = "/scada/delete/{scada_id}", method = RequestMethod.POST)
	public @ResponseBody JSONResult delete(@PathVariable("scada_id") String scada_id, Model model) throws Exception {
		scada_service.deleteSCADA(scada_id);
		return new JSONResult(Result.SUCCESS, "스카다를 삭제하였습니다.");
	}

	@RequestMapping(value = "/scada/svgupload", method = RequestMethod.POST)
	public @ResponseBody JSONResult create(UploadItem uploadItem, BindingResult result) {
		JSONResult json = new JSONResult();
		try {
			String dir = System.getProperty("java.io.tmpdir");
			File file = new File(dir + "/" + uploadItem.getName());
			FileUtils.writeByteArrayToFile(file, uploadItem.getFile().getBytes());
			String svg =  UploadUtils.readText(file);
			
			
			json.setStatus(JSONResult.SUCCESS);
			json.setDataValue("svg", svg);
			return json;
		} catch (IOException e) {
			log.error(e,e);
			json.setStatus(JSONResult.ERROR);
			return json;
		}
	}
}