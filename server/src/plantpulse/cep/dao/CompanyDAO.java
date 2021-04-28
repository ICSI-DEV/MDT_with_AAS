package plantpulse.cep.dao;

import java.sql.Connection;
import java.util.List;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.springframework.stereotype.Repository;

import plantpulse.dbutils.QueryRunner;
import plantpulse.dbutils.handlers.BeanHandler;
import plantpulse.dbutils.handlers.BeanListHandler;
import plantpulse.domain.Company;

@Repository
public class CompanyDAO extends CommonDAO {

	private static final Log log = LogFactory.getLog(CompanyDAO.class);

	public Company selectCompany() throws Exception {
		Connection connection = null;
		Company company = null;
		//
		try {
			//
			StringBuffer query = new StringBuffer();
			query.append("SELECT *    \n");
			query.append("  FROM MM_COMPANY    \n");
			query.append(" ORDER BY COMPANY_NAME  LIMIT 1  \n");

			log.debug("\n query.toString() : {\n" + query.toString() + "}");

			QueryRunner run = new QueryRunner();
			connection = super.getConnection();
			company = run.query(connection, query.toString(), new BeanHandler<Company>(Company.class), new Object[] {});

			//
		} catch (Exception ex) {
			log.error("\n CompanyDAO.selectCompany Exception : " + ex.getMessage());
			throw ex;
		} finally {
			super.closeConnection(connection);
		}

		return company;
	}

	
	public List<Company> selectCompanyList() throws Exception {
		Connection connection = null;
		List<Company> list = null;
		//
		try {
			//
			StringBuffer query = new StringBuffer();
			query.append("SELECT *    \n");
			query.append("  FROM MM_COMPANY    \n");
			query.append(" ORDER BY COMPANY_NAME   \n");

			log.debug("\n query.toString() : {\n" + query.toString() + "}");

			QueryRunner run = new QueryRunner();
			connection = super.getConnection();
			list = run.query(connection, query.toString(), new BeanListHandler<Company>(Company.class), new Object[] {});

			//log.info(list.toString());
			//
		} catch (Exception ex) {
			log.error("\n CompanyDAO.selectCompany Exception : " + ex.getMessage());
			throw ex;
		} finally {
			super.closeConnection(connection);
		}

		return list;
	}

	public void insertCompany(Company company) throws Exception {
		//
		Connection connection = null;
		//
		try {
			connection = super.getConnection();
			connection.setAutoCommit(false);

			//
			StringBuffer query = new StringBuffer();
			query.append("INSERT INTO MM_COMPANY (COMPANY_ID, COMPANY_NAME)    \n");
			query.append("VALUES (		     			     \n");
			query.append("         'COMP_' || LPAD(NEXT VALUE FOR COMP_SEQ, 5, '0'),    \n");
			query.append("         ?     \n");
			query.append("       )    \n");

			log.debug("\n query.toString() : {\n" + query.toString() + "}");
			//
			QueryRunner run = new QueryRunner();
			run.update(connection, query.toString(), new Object[] { company.getCompany_name() });

			connection.commit();

		} catch (Exception ex) {
			log.error("\n CompanyDAO.insertCompany Exception : " + ex.getMessage());
			connection.rollback();
			throw ex;
		} finally {
			connection.setAutoCommit(true);
			super.closeConnection(connection);
		}
	}

	public void updateCompany(Company company) throws Exception {
		//
		Connection connection = null;
		//
		try {
			//
			StringBuffer query = new StringBuffer();
			query.append("UPDATE MM_COMPANY    \n");
			query.append("   SET COMPANY_NAME = ?    \n");
			query.append(" WHERE COMPANY_ID = ?    \n");

			log.debug("\n query.toString() : {\n" + query.toString() + "}");
			//
			QueryRunner run = new QueryRunner();
			connection = super.getConnection();
			run.update(connection, query.toString(), new Object[] { company.getCompany_name(), company.getCompany_id() });

		} catch (Exception ex) {
			log.error("\n CompanyDAO.updateCompany Exception : " + ex.getMessage());
			throw ex;
		} finally {
			super.closeConnection(connection);
		}
	}

	public void deleteCompany(Company company) throws Exception {
		//
		Connection connection = null;
		//
		try {
			//
			StringBuffer query = new StringBuffer();
			query.append("DELETE FROM MM_COMPANY    \n");
			query.append(" WHERE COMPANY_ID = ?   \n");

			log.debug("\n query.toString() : {\n" + query.toString() + "}");
			//
			QueryRunner run = new QueryRunner();
			connection = super.getConnection();
			run.update(connection, query.toString(), new Object[] { company.getCompany_id() });

		} catch (Exception ex) {
			log.error("\n CompanyDAO.updateCompany Exception : " + ex.getMessage());
			throw ex;
		} finally {
			super.closeConnection(connection);
		}
	}

}
