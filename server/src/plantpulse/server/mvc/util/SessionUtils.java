package plantpulse.server.mvc.util;

import javax.servlet.http.HttpServletRequest;

import plantpulse.domain.User;

/**
 * SessionUtils
 * 
 * @author lenovo
 *
 */
public class SessionUtils {

	public static boolean isLogined(HttpServletRequest request) {
		return (request.getSession().getAttribute(Constants._SESSION_USER) != null && getUserFromSession(request).isLogined()) ? true : false;
	}
	
	public static User getUserFromSession(HttpServletRequest request) {
		return (User) request.getSession().getAttribute(Constants._SESSION_USER);
	}
}
