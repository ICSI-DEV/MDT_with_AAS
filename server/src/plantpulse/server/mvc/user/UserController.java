package plantpulse.server.mvc.user;

import java.util.List;

import javax.servlet.http.HttpServletRequest;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.json.JSONArray;
import org.json.JSONObject;
import org.springframework.stereotype.Controller;
import org.springframework.ui.Model;
import org.springframework.validation.BindingResult;
import org.springframework.web.bind.annotation.ModelAttribute;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.ResponseBody;

import plantpulse.cep.service.SecurityService;
import plantpulse.cep.service.UserService;
import plantpulse.domain.Security;
import plantpulse.domain.User;
import plantpulse.server.mvc.CRUDKeys;
import plantpulse.server.mvc.JSONResult;
import plantpulse.server.mvc.Result;
import plantpulse.server.mvc.util.SessionUtils;

@Controller
public class UserController {

	private static final Log log = LogFactory.getLog(UserController.class);

	private UserService user_service = new UserService();

	private SecurityService security_service = new SecurityService();

	//
	public Model setDefault(Model model, String insert_user_id) throws Exception {
		List<User> user_list = user_service.getUserList();
		model.addAttribute("user_list", user_list);
		return model;
	}

	@RequestMapping(value = "/user/index", method = RequestMethod.GET)
	public String index(Model model, HttpServletRequest request) throws Exception {
		setDefault(model, SessionUtils.getUserFromSession(request).getUser_id());
		return "user/index";
	}

	@RequestMapping(value = "/user/array", method = RequestMethod.GET, produces = "application/json; charset=UTF-8")
	public @ResponseBody String array(Model model, HttpServletRequest request) throws Exception {
		List<User> user_list = user_service.getUserList();
		JSONArray array = new JSONArray();
		for (int i = 0; i < user_list.size(); i++) {
			User user = user_list.get(i);
			JSONObject obj = new JSONObject();
			obj.put("user_id", user.getUser_id());
			obj.put("user_name", user.getUser_id());
			array.add(obj);
		}
		return array.toString();
	}

	@RequestMapping(value = "/user/add", method = RequestMethod.GET)
	public String add(Model model, HttpServletRequest request) throws Exception {
		User user = new User();
		user.setMode(CRUDKeys.INSERT);
		model.addAttribute("user", user);

		List<Security> security_list = security_service.getSecurityList();
		model.addAttribute("security_list", security_list);

		return "user/form";
	}

	@RequestMapping(value = "/user/edit/{user_id:.+}", method = RequestMethod.GET)
	public String form(@PathVariable("user_id") String user_id, Model model, HttpServletRequest request) throws Exception {
		User user = user_service.getUser(user_id);
		user.setMode(CRUDKeys.UPDATE);
		model.addAttribute("user", user);

		List<Security> security_list = security_service.getSecurityList();
		model.addAttribute("security_list", security_list);

		return "user/form";
	}

	@RequestMapping(value = "/user/save", method = RequestMethod.POST)
	public String save(@ModelAttribute("user") User user, BindingResult br, Model model, HttpServletRequest request) throws Exception {

		try {
			String insert_user_id = SessionUtils.getUserFromSession(request).getUser_id();
			//
			if (user.getMode().equals(CRUDKeys.INSERT)) {
				//
				user.setInsert_user_id(insert_user_id);
				//
				user_service.saveUser(user);
			} else if (user.getMode().equals(CRUDKeys.UPDATE)) {
				user_service.updateUser(user);
			}
			//
			setDefault(model, insert_user_id);
			model.addAttribute("result", new Result(Result.SUCCESS, "사용자을 저장하였습니다."));
			return "user/index";
		} catch (Exception ex) {
			model.addAttribute("result", new Result(Result.SUCCESS, "사용자을 저장할 수 없습니다 : 메세지=[" + ex.getMessage() + "]"));
			log.warn(ex);
			return "user/form";
		}
	}

	@RequestMapping(value = "/user/delete/{user_id:.+}", method = RequestMethod.POST)
	public @ResponseBody JSONResult delete(@PathVariable String user_id, Model model, HttpServletRequest request) throws Exception {
		//
		UserService user_service = new UserService();
		//
		user_service.deleteUser(user_id);
		//
		return new JSONResult(Result.SUCCESS, "사용자을 삭제하였습니다.");
	}

}