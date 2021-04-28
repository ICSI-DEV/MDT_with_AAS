package plantpulse.cep.service;

import java.util.Date;

import javax.servlet.http.HttpServletRequest;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import plantpulse.domain.User;
import plantpulse.server.mvc.util.Constants;

public class LoginService {

	private static final Log log = LogFactory.getLog(LoginService.class);

	private UserService user_service = new UserService();

	public void afterLoginSuccess(HttpServletRequest request, String user_id, String password) {
		//
		try {

			User user = user_service.getUser(user_id);

			user.setLogined(true);
			user.setLogin_date(new Date());
			//
			request.getSession().setAttribute(Constants._SESSION_USER_ID, user.getUser_id());
			request.getSession().setAttribute(Constants._SESSION_USER, user);
			log.debug("afterLoginSuccess invoked.");
		} catch (Exception ex) {
			log.error("User save to session failed : " + ex.getMessage(), ex);
			;
		}
	}

	public void beforeLogout(HttpServletRequest request) {
		request.getSession().invalidate();
		log.debug("beforeLogout invoked.");
	}
}
