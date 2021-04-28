package plantpulse.cep.service.support.security;

/**
 * SecurityContext
 * 
 * @author lsb
 *
 */
public class SecurityContext {

	private static final ThreadLocal<RoleAndSecurity> thread_local_security = new ThreadLocal<RoleAndSecurity>();

	public static RoleAndSecurity getRoleAndSecurity() {
		return thread_local_security.get();
	}

	public static void setRoleAndSecurity(RoleAndSecurity security) {
		thread_local_security.set(security);
	}

	public static void removeRoleAndSecurity() {
		thread_local_security.remove();
	}

}
