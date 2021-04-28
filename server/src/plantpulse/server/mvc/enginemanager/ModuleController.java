package plantpulse.server.mvc.enginemanager;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileReader;
import java.io.FileWriter;

import javax.servlet.ServletContext;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Controller;
import org.springframework.ui.Model;
import org.springframework.web.bind.annotation.ModelAttribute;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;

import plantpulse.cep.domain.Config;
import plantpulse.cep.domain.Module;
import plantpulse.cep.service.CEPEngineRestartService;

@Controller
public class ModuleController {

	private static final Log log = LogFactory.getLog(ModuleController.class);

	@Autowired
	private ServletContext context;

	Module module;

	@RequestMapping(value = "/enginemanager/module/form", method = RequestMethod.GET)
	public String form(Model model) {
		//
		try {
			String filePath = context.getRealPath("/WEB-INF/") + "/cep/module.epl";

			BufferedReader reader = new BufferedReader(new FileReader(filePath));
			StringBuffer sb = new StringBuffer();
			String rd;
			while ((rd = reader.readLine()) != null) {
				sb.append(rd + "\n");
			}
			reader.close();
			// log.info("[module epl] : " + sb.toString());

			module = new Module();
			module.setFilePath(filePath);
			module.setEpl(sb.toString());
			model.addAttribute("module", module);
			model.addAttribute("isEdited", "N");
		} catch (Exception e) {
			log.error("[RequestMapping : /enginemanager/module/form] " + e.getMessage(), e);
		}
		return "enginemanager/module/form";
	}

	@RequestMapping(value = "/enginemanager/module/save", method = RequestMethod.POST)
	public String save(@ModelAttribute("module") Module module, Model model) throws Exception {
		try {
			File file = new File(module.getFilePath());
			BufferedWriter output = new BufferedWriter(new FileWriter(file));
			output.write(module.getEpl());
			output.close();
			//
			model.addAttribute("message", "수정이 완료되었습니다.");
			model.addAttribute("isEdited", "Y");
		} catch (Exception e) {
			model.addAttribute("message", "수정 시 오류가 발생하였습니다.");
			model.addAttribute("isEdited", "N");
			log.error("[RequestMapping : /enginemanager/module/save] " + e.getMessage(), e);
		}
		model.addAttribute("resultKey", "U");
		//
		return "enginemanager/module/form";
	}

	@RequestMapping(value = "/enginemanager/module/restart", method = RequestMethod.GET)
	public String restart(@ModelAttribute("config") Config config, Model model) throws Exception {
		try {
			//
			CEPEngineRestartService cepEngineRestartService = new CEPEngineRestartService();
			cepEngineRestartService.restart();
			model.addAttribute("isEdited", "N");
			model.addAttribute("message", "엔진을 재시작 하였습니다.");
		} catch (Exception e) {
			model.addAttribute("message", "엔진을 재시작 시 오류가 발생하였습니다.");
			log.error("[RequestMapping : /enginemanager/module/restart] " + e.getMessage(), e);
		}
		model.addAttribute("resultKey", "U");
		//
		return "enginemanager/config/form";
	}
}