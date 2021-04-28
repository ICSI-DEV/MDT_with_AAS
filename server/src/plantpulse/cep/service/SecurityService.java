package plantpulse.cep.service;

import java.util.List;

import plantpulse.cep.dao.SecurityDAO;
import plantpulse.domain.Security;

public class SecurityService {

	private SecurityDAO dao = new SecurityDAO();

	public List<Security> getSecurityList() throws Exception {
		return dao.getSecurityList();
	}

	public Security getSecurity(String security_id) throws Exception {
		return dao.getSecurity(security_id);
	}

	public void saveSecurity(Security security) throws Exception {
		dao.saveSecurity(security);
	}

	public void updateSecurity(Security security) throws Exception {
		dao.updateSecurity(security);
	}

	public void deleteSecurity(String security_id) throws Exception {
		dao.deleteSecurity(security_id);
	}
}
