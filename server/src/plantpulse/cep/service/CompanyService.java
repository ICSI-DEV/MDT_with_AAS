package plantpulse.cep.service;

import java.util.List;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import plantpulse.cep.dao.CompanyDAO;
import plantpulse.domain.Company;

@Service
public class CompanyService {

	private static final Log log = LogFactory.getLog(CompanyService.class);

	@Autowired
	private CompanyDAO companyDAO;

	public List<Company> selectCompany() {
		try {
			return companyDAO.selectCompanyList();
		} catch (Exception e) {
			log.error("\n CompanyService.selectCompany Exception : " + e.getMessage(), e);
			return null;
		}
	}

	public void insertCompany(Company company) throws Exception {
		companyDAO.insertCompany(company);
	}

	public void updateCompany(Company company) throws Exception {
		companyDAO.updateCompany(company);
	}

	public void deleteCompany(Company company) throws Exception {
		companyDAO.deleteCompany(company);
	}

}
