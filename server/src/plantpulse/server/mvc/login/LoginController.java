package plantpulse.server.mvc.login;

import javax.servlet.ServletContext;
import javax.servlet.http.HttpServletRequest;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Controller;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.ResponseBody;

import plantpulse.cep.service.LoginService;
import plantpulse.cep.service.UserService;

@Controller
public class LoginController {

	private static final Log log = LogFactory.getLog(LoginController.class);

	@Autowired
	private ServletContext context;

	private UserService userInfoService = new UserService();
	private LoginService login_service = new LoginService();

	@RequestMapping(value = "/login/login", method = RequestMethod.POST)
	@ResponseBody
	public String login(HttpServletRequest request, @RequestParam("userId") String userId, @RequestParam("password") String password) throws Exception {
		if (userInfoService.checkLogin(userId, password)) {
			log.info("Login success : User ID = " + userId + ", IP = " + request.getRemoteAddr());
			//
			login_service.afterLoginSuccess(request, userId, password);
			//
			return "SUCCESS";
		} else {
			return "FAILURE";
		}
	}

	@RequestMapping(value = "/login/logout", method = RequestMethod.GET)
	public String logout(HttpServletRequest request) throws Exception {
		login_service.beforeLogout(request);
		log.debug("Logout success.");
		return "redirect:/";
	}
}