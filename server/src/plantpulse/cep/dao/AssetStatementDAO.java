package plantpulse.cep.dao;

import java.sql.Connection;
import java.util.List;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.springframework.stereotype.Repository;

import plantpulse.cep.domain.AssetStatement;
import plantpulse.dbutils.QueryRunner;
import plantpulse.dbutils.handlers.BeanHandler;
import plantpulse.dbutils.handlers.BeanListHandler;

@Repository
public class AssetStatementDAO  extends CommonDAO {
	
	private static final Log log = LogFactory.getLog(AssetStatementDAO.class);
	
	public List<AssetStatement>  selectAssetStatements() throws Exception {

		Connection connection = null;
		List<AssetStatement> list = null;
		//
		try {
			//
			StringBuffer query = new StringBuffer();
			query.append("SELECT * ");
			query.append("  FROM MM_ASSET_STATEMENT A    \n");


			QueryRunner run = new QueryRunner();
			connection = super.getConnection();
			list = run.query(connection, query.toString(), 
					new BeanListHandler<AssetStatement>(AssetStatement.class));
			//
		} catch (Exception ex) {
			log.error("\n AssetDAO.selectAssetStatements Exception : " + ex.getMessage(), ex);
			throw ex;
		} finally {
			super.closeConnection(connection);
		}
		return list;
	}
	
	public AssetStatement selectAssetStatement(String asset_id, String statement_name) throws Exception {
		Connection connection = null;
		AssetStatement one = null;
		//
		try {
			//
			StringBuffer query = new StringBuffer();
			query.append("SELECT * ");
			query.append("  FROM MM_ASSET_STATEMENT A    \n");
			query.append(" WHERE A.ASSET_ID = ?    \n");
			query.append("   AND A.STATEMENT_NAME = ?    \n");

			QueryRunner run = new QueryRunner();
			connection = super.getConnection();
			one = run.query(connection, query.toString(), 
					new BeanHandler<AssetStatement>(AssetStatement.class),
					new Object[] { asset_id,  statement_name});
			//
		} catch (Exception ex) {
			log.error("\n AssetDAO.selectAssetStatementByAssetId Exception : " + ex.getMessage(), ex);
			throw ex;
		} finally {
			super.closeConnection(connection);
		}
		return one;
	}
	
	public List<AssetStatement>  selectAssetStatementByAssetId(String asset_id) throws Exception {

		Connection connection = null;
		List<AssetStatement> list = null;
		//
		try {
			//
			StringBuffer query = new StringBuffer();
			query.append("SELECT * ");
			query.append("  FROM MM_ASSET_STATEMENT A    \n");
			query.append(" WHERE A.ASSET_ID = ?    \n");

			log.debug("\n AssetDAO.selectAssets query :{\n"+query.toString()+"}");
			log.debug("\n asset_id=" + asset_id);

			QueryRunner run = new QueryRunner();
			connection = super.getConnection();
			list = run.query(connection, query.toString(), 
					new BeanListHandler<AssetStatement>(AssetStatement.class),
					new Object[] { asset_id });
			//
		} catch (Exception ex) {
			log.error("\n AssetDAO.selectAssetStatementByAssetId Exception : " + ex.getMessage(), ex);
			throw ex;
		} finally {
			super.closeConnection(connection);
		}
		return list;
	}
	
	public void insertAssetStatement(AssetStatement asset_statement) throws Exception {
		//
		Connection connection = null;

		//
		try {
			//
			connection = super.getConnection();
			connection.setAutoCommit(false);

			
			StringBuffer query = new StringBuffer();
			query.append("INSERT INTO MM_ASSET_STATEMENT    \n");
			query.append("       (    \n");
			query.append("         ASSET_ID,    \n");
			query.append("         STATEMENT_NAME,    \n");
			query.append("         TYPE,    \n");
			query.append("         EQL,    \n");
			query.append("         DESCRIPTION,    \n");
			query.append("         INSERT_DATE    \n");
			query.append("       )    \n");
			query.append("VALUES    \n");
			query.append("       (    \n");
			query.append("         ?,    \n");
			query.append("         ?,    \n");
			query.append("         ?,    \n");
			query.append("         ?,    \n");
			query.append("         ?,    \n");
			query.append("         SYSDATE    \n");
			query.append("       )    \n");

			log.debug("\n query.toString() : {\n" + query.toString() + "}");
			log.debug("\n Parameter : [" + asset_statement.toString() + "]");
			//
			QueryRunner run = new QueryRunner();
			run.update(connection, query.toString(),
					new Object[] { 
							asset_statement.getAsset_id(), 
							asset_statement.getStatement_name(),
							asset_statement.getType(), 
							asset_statement.getEql(),
							asset_statement.getDescription() });

			connection.commit();

		} catch (Exception ex) {
			log.error("\n AssetStatementDAO.insertAssetStatement Exception : " + ex.getMessage());
			connection.rollback();
			throw ex;
		} finally {
			connection.setAutoCommit(true);
			super.closeConnection(connection);

		}
	}
	
	public void updateAssetStatement(AssetStatement asset_statement) throws Exception {
		//
		Connection connection = null;

		//
		try {
			//
			connection = super.getConnection();
			connection.setAutoCommit(false);

			//
			StringBuffer query = new StringBuffer();
			query.append("UPDATE MM_ASSET_STATEMENT  SET   \n");
			query.append("         STATEMENT_NAME = ?,    \n");
			query.append("         TYPE = ?,    \n");
			query.append("         EQL = ?,    \n");
			query.append("         DESCRIPTION = ?    \n");
			query.append(" WHERE ASSET_ID = ?    \n");
			query.append("   AND STATEMENT_NAME = ?    \n");

			log.debug("\n query.toString() : {\n" + query.toString() + "}");
			log.debug("\n Parameter : [" + asset_statement.toString() + "]");
			//
			QueryRunner run = new QueryRunner();
			run.update(connection, query.toString(),
					new Object[] { 
							asset_statement.getStatement_name(),
							asset_statement.getType(), 
							asset_statement.getEql(),
							asset_statement.getDescription(),
							asset_statement.getAsset_id(),
							asset_statement.getStatement_name()
					});

			connection.commit();

		} catch (Exception ex) {
			log.error("\n AssetStatementDAO.updateAssetStatement Exception : " + ex.getMessage());
			connection.rollback();
			throw ex;
		} finally {
			connection.setAutoCommit(true);
			super.closeConnection(connection);
		}
	}
	
	
	
	public void deleteAssetStatement(AssetStatement asset_statement) throws Exception {
		//
		Connection connection = null;
		//
		try {
			connection = super.getConnection();
			connection.setAutoCommit(false);

			// Asset 삭제
			StringBuffer query = new StringBuffer();
			query.append("DELETE FROM MM_ASSET_STATEMENT    \n");
			query.append(" WHERE ASSET_ID = ?    \n");
			query.append("   AND STATEMENT_NAME = ?    \n");

			log.debug("\n query.toString() : {\n" + query.toString() + "}");
			//
			QueryRunner run = new QueryRunner();
			run.update(connection, query.toString(), new Object[] { 
					asset_statement.getAsset_id(),
					asset_statement.getStatement_name()});

			connection.commit();
			//
		} catch (Exception ex) {
			connection.rollback();
			log.error(ex);
			throw ex;
		} finally {
			connection.setAutoCommit(true);
			super.closeConnection(connection);
		}
	}


}


