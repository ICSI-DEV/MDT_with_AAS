package plantpulse.server.mvc.my;

import javax.servlet.http.HttpServletRequest;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.springframework.stereotype.Controller;
import org.springframework.ui.Model;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.ResponseBody;

import plantpulse.cep.service.UserService;
import plantpulse.domain.User;
import plantpulse.server.mvc.JSONResult;
import plantpulse.server.mvc.util.SessionUtils;

@Controller
public class ProfileController {

	private static final Log log = LogFactory.getLog(ProfileController.class);

	private UserService user_service = new UserService();

	@RequestMapping(value = "/my/profile", method = RequestMethod.GET)
	public String view(Model model, HttpServletRequest request) throws Exception {
		String insert_user_id = SessionUtils.getUserFromSession(request).getUser_id();
		;
		User user = user_service.getUser(insert_user_id);
		model.addAttribute("user", user);
		return "my/profile";
	}

	@RequestMapping(value = "/my/profile/save", method = RequestMethod.POST)
	public @ResponseBody JSONResult save(User user, Model model, HttpServletRequest request) throws Exception {
		user_service.updateUser(user);
		return new JSONResult(JSONResult.SUCCESS, "프로필을 수정하였습니다.");
	}

}
