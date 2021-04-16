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
import plantpulse.cep.service.CEPEngineRestartService;

@Controller
public class EngineConfigController {

	private static final Log log = LogFactory.getLog(EngineConfigController.class);

	@Autowired
	private ServletContext context;

	Config config;

	@RequestMapping(value = "/enginemanager/config/form", method = RequestMethod.GET)
	public String form(Model model) {
		//
		try {
			String filePath = context.getRealPath("/WEB-INF/") + "/config/esper-cep-config.xml";

			BufferedReader reader = new BufferedReader(new FileReader(filePath));
			StringBuffer sb = new StringBuffer();
			String rd;
			while ((rd = reader.readLine()) != null) {
				sb.append(rd + "\n");
			}
			reader.close();
			// log.info("[File contents] : " + sb.toString());

			config = new Config();
			config.setFilePath(filePath);
			config.setFileContents(sb.toString());
			model.addAttribute("config", config);
			model.addAttribute("isEdited", "N");
		} catch (Exception e) {
			log.error("[RequestMapping : /enginemanager/config/form] " + e.getMessage(), e);
		}
		return "enginemanager/config/form";
	}

	@RequestMapping(value = "/enginemanager/config/save", method = RequestMethod.POST)
	public String save(@ModelAttribute("config") Config config, Model model) throws Exception {
		try {
			String filePath = context.getRealPath("/WEB-INF/") + "/config/esper-cep-config.xml";
			File file = new File(filePath);
			BufferedWriter output = new BufferedWriter(new FileWriter(file));
			output.write(config.getFileContents());
			output.close();
			//
			model.addAttribute("message", "수정이 완료되었습니다.");
			model.addAttribute("isEdited", "Y");
		} catch (Exception e) {
			model.addAttribute("message", "수정 시 오류가 발생하였습니다.");
			model.addAttribute("isEdited", "N");
			log.error("[RequestMapping : /enginemanager/config/save] " + e.getMessage(), e);
		}
		model.addAttribute("resultKey", "U");
		//
		return "enginemanager/config/form";
	}

	@RequestMapping(value = "/enginemanager/config/restart", method = RequestMethod.GET)
	public String restart(@ModelAttribute("config") Config config, Model model) throws Exception {
		try {
			//
			CEPEngineRestartService cepEngineRestartService = new CEPEngineRestartService();
			cepEngineRestartService.restart();

			model.addAttribute("message", "엔진을 재시작 하였습니다.");
		} catch (Exception e) {
			model.addAttribute("message", "엔진을 재시작 시 오류가 발생하였습니다.");
			log.error("[RequestMapping : /enginemanager/config/restart] " + e.getMessage(), e);
		}
		model.addAttribute("resultKey", "U");
		model.addAttribute("isEdited", "N");
		//
		return "enginemanager/config/form";
	}
}