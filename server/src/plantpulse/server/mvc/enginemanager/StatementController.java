package plantpulse.server.mvc.enginemanager;

import java.util.List;

import org.apache.commons.lang.StringUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.springframework.stereotype.Controller;
import org.springframework.ui.Model;
import org.springframework.validation.BindingResult;
import org.springframework.web.bind.annotation.ModelAttribute;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.ResponseBody;

import plantpulse.cep.domain.Statement;
import plantpulse.cep.service.CEPService;
import plantpulse.cep.service.CEPStatementService;
import plantpulse.server.mvc.JSONResult;

@Controller
public class StatementController {

	private static final Log log = LogFactory.getLog(StatementController.class);

	CEPService cep_service = new CEPService();
	List<Statement> statementList;

	public Model setDefault(Model model) throws Exception {
		statementList = cep_service.getStatementList();
		CEPStatementService cepStatementService = new CEPStatementService();
		statementList = cepStatementService.getStatementStatus(statementList);
		model.addAttribute("statementList", statementList);
		return model;
	}

	@RequestMapping(value = "/enginemanager/statement/index", method = RequestMethod.GET)
	public String index(Model model) throws Exception {
		model.addAttribute("resultKey", "V");
		setDefault(model);
		return "enginemanager/statement/index";
	}

	@RequestMapping(value = "/enginemanager/statement/add", method = RequestMethod.GET)
	public String add(Model model) throws Exception {
		Statement statement = new Statement();
		model.addAttribute("statement", statement);
		return "enginemanager/statement/form";
	}

	@RequestMapping(value = "/enginemanager/statement/form", method = RequestMethod.GET)
	public String form(@RequestParam("statementName") String statementName, Model model) throws Exception {
		Statement statement = new Statement();
		if (StringUtils.isNotEmpty(statementName)) {
			statement = cep_service.getStatement(statementName);
			model.addAttribute("statement", statement);
		}
		return "enginemanager/statement/form";
	}

	@RequestMapping(value = "/enginemanager/statement/save", method = RequestMethod.POST)
	public String save(@ModelAttribute("statement") Statement statement, BindingResult br, Model model) throws Exception {
		// if (br.hasErrors()) {
		// return "enginemanager/statement/form";
		// }
		try {
			if (statement.getInsertDate() == null) {
				cep_service.saveStatement(statement);
				model.addAttribute("resultKey", "I");
				model.addAttribute("message", "등록이 완료되었습니다.");
				//
				CEPStatementService cepStatementService = new CEPStatementService();
				cepStatementService.deployStatement(statement);

			} else {
				cep_service.updateStatement(statement);
				model.addAttribute("resultKey", "U");
				model.addAttribute("message", "수정이 완료되었습니다.");

				//
				CEPStatementService cepStatementService = new CEPStatementService();
				cepStatementService.redeployStatement(statement);
			}

		} catch (Exception ex) {
			model.addAttribute("error", "스테이트먼트를 배치할 수 없습니다 : 에러=[" + ex.getMessage() + "]");
			log.warn(ex);
		}

		setDefault(model);

		//
		return "enginemanager/statement/index";
	}

	@RequestMapping(value = "/enginemanager/statement/delete", method = RequestMethod.GET)
	public String delete(@ModelAttribute("statement") Statement statement, Model model) throws Exception {
		cep_service.deleteStatement(statement.getStatementName());
		model.addAttribute("resultKey", "D");
		model.addAttribute("message", "삭제가 완료되었습니다.");
		CEPStatementService cepStatementService = new CEPStatementService();
		cepStatementService.undeployStatement(statement);
		setDefault(model);
		return "enginemanager/statement/index";
	}

	@RequestMapping(value = "/enginemanager/statement/startStatement/{statementNames}", method = RequestMethod.GET)
	public @ResponseBody JSONResult startStatement(@PathVariable String statementNames) {
		//
		JSONResult json = new JSONResult();
		try {

			String[] ids = statementNames.split(",");
			for (int i = 0; i < ids.length; i++) {
				Statement statement = new Statement();
				statement = cep_service.getStatement(ids[i]);

				CEPStatementService cepStatementService = new CEPStatementService();
				cepStatementService.startStatement(statement);
			}
			json.setStatus("SUCCESS");

		} catch (Exception e) {
			log.error("Statement failed to starting : " + e.getMessage(), e);
			json.setStatus("FAILURE");
			json.setMessage("Statement failed to starting : " + e.getMessage());
		}
		return json;
	}

	@RequestMapping(value = "/enginemanager/statement/stopStatement/{statementNames}", method = RequestMethod.GET)
	public @ResponseBody JSONResult stopStatement(@PathVariable String statementNames) {
		//
		JSONResult json = new JSONResult();
		try {

			String[] ids = statementNames.split(",");
			for (int i = 0; i < ids.length; i++) {
				CEPStatementService cepStatementService = new CEPStatementService();
				cepStatementService.stopStatement(ids[i]);
			}
			json.setStatus("SUCCESS");
		} catch (Exception e) {
			log.error("Statement failed to stopping : " + e.getMessage(), e);
			json.setStatus("FAILURE");
			json.setMessage("Statement failed to stopping : " + e.getMessage());
		}
		return json;

	}
}