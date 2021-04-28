package plantpulse.cep.service.support.security;

import javax.servlet.http.HttpServletRequest;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import plantpulse.cep.service.SecurityService;
import plantpulse.cep.service.UserService;
import plantpulse.domain.Security;
import plantpulse.domain.User;
import plantpulse.server.mvc.util.SessionUtils;

/**
 * SecurityTools
 * 
 * @author lsb
 *
 */
public class SecurityTools {

	private static final Log log = LogFactory.getLog(SecurityTools.class);
	
	
	public static final String ADMIN_ROLE = "ADMIN";

	public static RoleAndSecurity getRoleAndSecurity() {
		return SecurityContext.getRoleAndSecurity();
	}
	
	
	/**
	 * 오브젝트 ID에 대한 권한이 있는지 확인한다.
	 * 
	 * @param object_id
	 * @return
	 */
	public static boolean isAdminRole() {
		//
		RoleAndSecurity rs = SecurityContext.getRoleAndSecurity();
		if (rs.getRole().equals(ADMIN_ROLE)) { // 관리자 롤은 무조건 트루
			return true;
		}else {
			return false;
		}
	}

	/**
	 * 오브젝트 ID에 대한 권한이 있는지 확인한다.
	 * 
	 * @param object_id
	 * @return
	 */
	public static boolean hasPermission(String object_id) {
		//
		RoleAndSecurity rs = SecurityContext.getRoleAndSecurity();

		if (rs.getRole().equals(ADMIN_ROLE)) { // 관리자 롤은 무조건 트루
			return true;
		}
		;

		if (rs.getSecurity() == null) {
			return false;
		}
		//
		boolean has_permission = false;
		String[] object_permission_array = rs.getSecurity().getObject_permission_array().split(",");
		for (int i = 0; object_permission_array != null && i < object_permission_array.length; i++) {
			if (object_permission_array[i].equals(object_id)) {
				has_permission = true;
				break;
			}
		}
		log.debug("object_id=[" + object_id + "], hasPermission=[" + has_permission + "]");
		return has_permission;
	}

	public RoleAndSecurity getRoleAndSecurity(HttpServletRequest request) {
		RoleAndSecurity rs = new RoleAndSecurity();

		try {

			String user_id = SessionUtils.getUserFromSession(request).getUser_id();
			rs.setUser_id(user_id);
			//
			UserService user_service = new UserService();
			User user = user_service.getUser(user_id);
			rs.setRole(user.getRole());
			//
			SecurityService security_service = new SecurityService();
			Security security = security_service.getSecurity(user.getSecurity_id());
			rs.setSecurity(security);

		} catch (Exception ex) {
			log.error("RoleAndSecurity getting error from request : " + ex.getMessage(), ex);
		}

		return rs;
	}

}
