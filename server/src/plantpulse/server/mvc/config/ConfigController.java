package plantpulse.server.mvc.config;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.springframework.stereotype.Controller;
import org.springframework.ui.Model;
import org.springframework.validation.BindingResult;
import org.springframework.web.bind.annotation.ModelAttribute;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;

import plantpulse.cep.engine.config.ConfigurationManager;
import plantpulse.cep.engine.config.ServerConfiguration;
import plantpulse.server.mvc.Result;

@Controller
public class ConfigController {

	private static final Log log = LogFactory.getLog(ConfigController.class);

	@RequestMapping(value = "/config/form", method = RequestMethod.GET)
	public String index(Model model) throws Exception {
		model.addAttribute("config", ConfigurationManager.getInstance().getServer_configuration());
		return "config/form";
	}

	@RequestMapping(value = "/config/save", method = RequestMethod.POST)
	public String save(@ModelAttribute("config") ServerConfiguration config, BindingResult br, Model model) throws Exception {

		try {
			ConfigurationManager.getInstance().setServer_configuration(config);
			ConfigurationManager.getInstance().update();

			model.addAttribute("result", new Result(Result.SUCCESS, "설정을 저장하였습니다. 변경된 설정을 적용하기 위해 서버를 재시작하십시오."));
			return "config/form";

		} catch (Exception ex) {
			model.addAttribute("result", new Result(Result.ERROR, "설정을 저장할 수 없습니다 : 메세지=[" + ex.getMessage() + "]"));
			log.error(ex.getMessage(), ex);
			return "config/form";
		}
	}

}
