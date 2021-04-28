package plantpulse.server.mvc.token;

import java.util.List;

import javax.servlet.http.HttpServletRequest;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Controller;
import org.springframework.ui.Model;
import org.springframework.validation.BindingResult;
import org.springframework.web.bind.annotation.ModelAttribute;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.ResponseBody;

import plantpulse.cep.domain.Token;
import plantpulse.cep.service.SiteService;
import plantpulse.cep.service.TagService;
import plantpulse.cep.service.TokenService;
import plantpulse.server.mvc.CRUDKeys;
import plantpulse.server.mvc.JSONResult;
import plantpulse.server.mvc.Result;
import plantpulse.server.mvc.util.SessionUtils;

@Controller
public class TokenController  {

	private static final Log log = LogFactory.getLog(TokenController.class);

	private TokenService token_service = new TokenService();

	@Autowired
	private SiteService site_service;

	@Autowired
	private TagService tag_service;

	//
	public Model setDefault(Model model, String insert_token_id) throws Exception {
		List<Token> token_list = token_service.getTokenList();
		model.addAttribute("token_list", token_list);
		return model;
	}

	@RequestMapping(value = "/token/index", method = RequestMethod.GET)
	public String index(Model model, HttpServletRequest request) throws Exception {
		setDefault(model, SessionUtils.getUserFromSession(request).getUser_id());
		return "token/index";
	}

	@RequestMapping(value = "/token/add", method = RequestMethod.GET)
	public String add(Model model, HttpServletRequest request) throws Exception {
		Token token = new Token();
		token.setMode(CRUDKeys.INSERT);
		model.addAttribute("token", token);
		return "token/form";
	}

	@RequestMapping(value = "/token/edit/{token_id:.+}", method = RequestMethod.GET)
	public String form(@PathVariable("token_id") String token_id, Model model, HttpServletRequest request) throws Exception {
		Token token = token_service.getToken(token_id);
		token.setMode(CRUDKeys.UPDATE);
		model.addAttribute("token", token);
		return "token/form";
	}

	@RequestMapping(value = "/token/save", method = RequestMethod.POST)
	public String save(@ModelAttribute("token") Token token, BindingResult br, Model model, HttpServletRequest request) throws Exception {
		try {
			//
			String insert_user_id = SessionUtils.getUserFromSession(request).getUser_id();
			if (token.getMode().equals(CRUDKeys.INSERT)) {
				token_service.saveToken(token);
			} else if (token.getMode().equals(CRUDKeys.UPDATE)) {
				token_service.updateToken(token);
			}
			//
			setDefault(model, insert_user_id);
			model.addAttribute("result", new Result(Result.SUCCESS, "API 토큰을 저장하였습니다."));
			return "token/index";

		} catch (Exception ex) {
			model.addAttribute("result", new Result(Result.SUCCESS, "API 토큰을 저장할 수 없습니다 : 메세지=[" + ex.getMessage() + "]"));
			log.warn(ex);
			return "token/form";
		}
	}

	@RequestMapping(value = "/token/delete/{token:.+}", method = RequestMethod.POST)
	public @ResponseBody JSONResult delete(@PathVariable String token, Model model, HttpServletRequest request) throws Exception {
		//
		TokenService token_service = new TokenService();
		token_service.deleteToken(token);
		return new JSONResult(Result.SUCCESS, "API 토큰을 삭제하였습니다.");
	}
	
	@RequestMapping(value = "/token/generate", method = RequestMethod.POST)
	public @ResponseBody String generate(Model model, HttpServletRequest request) throws Exception {
		return token_service.generateToken();
	}
	
	@RequestMapping(value = "/token/deploy", method = RequestMethod.POST)
	public @ResponseBody JSONResult deploy(Model model, HttpServletRequest request) throws Exception {
		token_service.deploy();
		return new JSONResult(Result.SUCCESS, "API 토큰을 배포하였습니다.");
	}
	
	
	
	

}