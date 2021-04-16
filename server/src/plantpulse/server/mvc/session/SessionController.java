package plantpulse.server.mvc.session;

import java.util.HashMap;
import java.util.Map;

import javax.servlet.http.HttpServletRequest;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.json.JSONObject;
import org.springframework.stereotype.Controller;
import org.springframework.ui.Model;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.ResponseBody;

import plantpulse.cep.service.UserService;
import plantpulse.cep.service.support.security.SecurityTools;
import plantpulse.domain.User;
import plantpulse.server.mvc.util.SessionUtils;


@Controller
public class SessionController {

	private static final Log log = LogFactory.getLog(SessionController.class);
	
	private UserService user_service = new UserService();

	@RequestMapping(value = "/session/continue", method = RequestMethod.GET)
	public @ResponseBody String index(Model model, HttpServletRequest request) throws Exception {
		request.getSession().isNew();
		return "OK";
	}

	
	@RequestMapping(value = "/session/property", method = RequestMethod.POST)
	public @ResponseBody String setProperty(Model model, HttpServletRequest request) throws Exception {
		String key = request.getParameter("key");
		String value = request.getParameter("value");
		request.getSession().setAttribute(key, value);
		//
		User user = SessionUtils.getUserFromSession(request);
		user_service.updateSessionProperty(user.getUser_id(), key, value);
		return "OK";
	}
	
	@RequestMapping(value = "/session/property", method = RequestMethod.GET)
	public @ResponseBody String getProperty(Model model, HttpServletRequest request) throws Exception {
	    String key = request.getParameter("key");
		//
		User user = SessionUtils.getUserFromSession(request);
		String value = user_service.getSessionProperty(user.getUser_id(), key);
		if(value == null) value = "";
		return value;
	}
	
	@RequestMapping(value = "/session/storage", method = RequestMethod.GET)
	public @ResponseBody String storage(Model model, HttpServletRequest request) throws Exception {
		User user = SessionUtils.getUserFromSession(request);
		if(user == null) {
			throw new Exception("Session user is NULL.");
		}
		
		// 0. 사용자 저장 세션 테이블 로딩 
		// 1. 지도 포지션 로딩
		// 2. 기본 사이트 지정 로딩 
		// 3. 사이트별 오브젝트 트리 JSON 로딩
		// 4. 보안 권한 로딩
		
		Map<String,String> prop_map = user_service.getSessionPropertyMap(user.getUser_id());
		if(prop_map == null) prop_map = new HashMap<String,String>();
		return JSONObject.fromObject(prop_map).toString();
	}

	@RequestMapping(value = "/session/load", method = RequestMethod.GET)
	public  String load(Model model, HttpServletRequest request) throws Exception {
		return "session/load";
	};
	
	
}