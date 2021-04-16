package plantpulse.cep.service.support.security;

import plantpulse.domain.Security;

public class RoleAndSecurity {

	private String user_id;
	private String role;
	private Security security;

	public String getUser_id() {
		return user_id;
	}

	public void setUser_id(String user_id) {
		this.user_id = user_id;
	}

	public String getRole() {
		return role;
	}

	public void setRole(String role) {
		this.role = role;
	}

	public Security getSecurity() {
		return security;
	}

	public void setSecurity(Security security) {
		this.security = security;
	}

	@Override
	public String toString() {
		return "RoleAndSecurity [user_id=" + user_id + ", role=" + role + ", security=" + security + "]";
	}

}
