package plantpulse.cep.dao;

import java.math.BigDecimal;
import java.sql.Connection;
import java.util.List;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.springframework.stereotype.Repository;

import plantpulse.cep.engine.config.ConfigurationManager;
import plantpulse.cep.engine.config.ServerConfiguration;
import plantpulse.dbutils.QueryRunner;
import plantpulse.dbutils.handlers.BeanHandler;
import plantpulse.dbutils.handlers.BeanListHandler;
import plantpulse.dbutils.handlers.ScalarHandler;
import plantpulse.domain.Site;

@Repository
public class SiteDAO extends CommonDAO {

	private static final Log log = LogFactory.getLog(SiteDAO.class);
	

	public List<Site> selectSites() throws Exception {
		Connection connection = null;
		List<Site> list = null;
		//
		try {
			//
			StringBuffer query = new StringBuffer();

			query.append("SELECT S.SITE_ID,    \n");
			query.append("       S.SITE_NAME,    \n");
			query.append("       S.LAT,    \n");
			query.append("       S.LNG,    \n");
			query.append("       S.COMPANY_ID,    \n");
			query.append("       S.HTTP_HOST,    \n");
			query.append("       S.HTTP_PORT,    \n");
			query.append("       S.HTTP_USER,    \n");
			query.append("       S.HTTP_PASSWORD,    \n");
			query.append("       S.HTTP_DESTINATION,    \n");
			query.append("       S.MQ_HOST,    \n");
			query.append("       S.MQ_PORT,    \n");
			query.append("       S.MQ_USER,    \n");
			query.append("       S.MQ_PASSWORD,    \n");
			query.append("       S.MQ_DESTINATION,    \n");
			query.append("       S.STORAGE_HOST,    \n");
			query.append("       S.STORAGE_PORT,    \n");
			query.append("       S.STORAGE_USER,    \n");
			query.append("       S.STORAGE_PASSWORD,    \n");
			query.append("       S.STORAGE_KEYSPACE,    \n");
			query.append("       S.INSERT_DATE,    \n");
			query.append("       S.UPDATE_DATE,    \n");
			query.append("       S.DESCRIPTION,    \n");
			query.append("       C.COMPANY_ID,    \n");
			query.append("       C.COMPANY_NAME    \n");
			query.append("  FROM MM_SITES S    \n");
			query.append(" INNER JOIN MM_COMPANY C ON S.COMPANY_ID = C.COMPANY_ID    \n");
			query.append(" ORDER BY S.SITE_NAME   \n");

			log.debug("\n query.toString() : {\n" + query.toString() + "}");

			QueryRunner run = new QueryRunner();
			connection = super.getConnection();
			list = run.query(connection, query.toString(), new BeanListHandler<Site>(Site.class), new Object[] {});

			for (int i = 0; list != null && i < list.size(); i++) {
				Site site = list.get(i);
				
				//에셋 정보 추가
				AssetDAO asset_dao = new AssetDAO();
				site.setTotal_area_count(asset_dao.selectAssetsCountBySiteIdAndAssetType(site.getSite_id(), AssetDAO.ASSET_TYPE_AREA));
				site.setTotal_asset_count(asset_dao.selectAssetsCountBySiteIdAndAssetType(site.getSite_id(), AssetDAO.ASSET_TYPE_ASSET));
				//태그 건수
				TagDAO tag_dao = new TagDAO();
				site.setTotal_tag_count(tag_dao.selectTagsCountBySiteId(site.getSite_id()));
			}

			log.debug(list.toString());
			//
		} catch (Exception ex) {
			log.error(ex);
			throw ex;
		} finally {
			super.closeConnection(connection);
		}

		return list;
	}

	public Site selectSite(String siteId) throws Exception {
		Connection connection = null;
		Site site = new Site();
		//
		try {
			//
			StringBuffer query = new StringBuffer();
			query.append("SELECT *    \n");
			query.append("  FROM MM_SITES   \n");
			query.append(" WHERE SITE_ID = ?   \n");

			log.debug("\n query.toString() : {\n" + query.toString() + "}");

			QueryRunner run = new QueryRunner();
			connection = super.getConnection();
			site = run.query(connection, query.toString(), new BeanHandler<Site>(Site.class), new Object[] { siteId });

			if(site != null){
				TagDAO tag_dao = new TagDAO();
				site.setTotal_tag_count(tag_dao.selectTagsCountBySiteId(site.getSite_id()));
			}else{
				site = new Site();
			}

			site = overrideSiteDefautInfo(site);
			log.debug(site.toString());
			//
		} catch (Exception ex) {
			log.error("\n SiteDAO.selectSiteInfo Exception : " + ex.getMessage());
			throw ex;
		} finally {
			super.closeConnection(connection);
		}

		return site;
	}

	
	public Site selectSiteInfo(String siteId) throws Exception {
		Connection connection = null;
		Site site = new Site();
		//
		try {
			//
			StringBuffer query = new StringBuffer();
			query.append("SELECT *    \n");
			query.append("  FROM MM_SITES   \n");
			query.append(" WHERE SITE_ID = ?   \n");

			log.debug("\n query.toString() : {\n" + query.toString() + "}");

			QueryRunner run = new QueryRunner();
			connection = super.getConnection();
			site = run.query(connection, query.toString(), new BeanHandler<Site>(Site.class), new Object[] { siteId });

			if(site != null){
				AssetDAO asset_dao = new AssetDAO();
				site.setTotal_area_count(asset_dao.selectAssetsCountBySiteIdAndAssetType(site.getSite_id(), AssetDAO.ASSET_TYPE_AREA));
				site.setTotal_asset_count(asset_dao.selectAssetsCountBySiteIdAndAssetType(site.getSite_id(), AssetDAO.ASSET_TYPE_ASSET));
				//태그 건수
				TagDAO tag_dao = new TagDAO();
				site.setTotal_tag_count(tag_dao.selectTagsCountBySiteId(site.getSite_id()));
			}else{
				site = new Site();
			}

			site = overrideSiteDefautInfo(site);
			log.debug(site.toString());
			//
		} catch (Exception ex) {
			log.error("\n SiteDAO.selectSiteInfo Exception : " + ex.getMessage());
			throw ex;
		} finally {
			super.closeConnection(connection);
		}

		return site;
	}
	
	
	public boolean hasSite(String site_id) throws Exception {
		Connection connection = null;
		boolean has = false;
		//
		try {
			//
			StringBuffer query = new StringBuffer();
			query.append("SELECT *    \n");
			query.append("  FROM MM_SITES   \n");
			query.append(" WHERE SITE_ID = ?   \n");

			log.debug("\n query.toString() : {\n" + query.toString() + "}");

			QueryRunner run = new QueryRunner();
			connection = super.getConnection();
			Site site = run.query(connection, query.toString(), new BeanHandler<Site>(Site.class), new Object[] { site_id });

			if(site != null){
				has = true;
			}else{
				has = false;
			}
			//
		} catch (Exception ex) {
			log.error("Site has xception : " + ex.getMessage());
			throw ex;
		} finally {
			super.closeConnection(connection);
		}
		return has;
	}

	public void insertSite(Site site) throws Exception {
		//
		Connection connection = null;
		//
		try {
			connection = super.getConnection();
			connection.setAutoCommit(false);
			
			
			//
			StringBuffer nquery = new StringBuffer();
			QueryRunner nrun = new QueryRunner();
			nquery.append("SELECT 'SITE_' || LPAD(NEXT VALUE FOR SITE_SEQ, 5, '0') AS SITE_ID FROM DUAL   \n");
			String site_id = nrun.query(connection, nquery.toString(), new ScalarHandler<String>(), new Object[] {});
			site.setSite_id(site_id);
			
			//
			StringBuffer query = new StringBuffer();
			query.append("INSERT INTO MM_SITES    \n");
			query.append("	     (    \n");
			query.append("	       SITE_ID,     \n");
			query.append("	       SITE_NAME,     \n");
			query.append("	       LAT,     \n");
			query.append("	       LNG,     \n");
			query.append("	       DESCRIPTION,     \n");
			query.append("	       COMPANY_ID,     \n");
			query.append("         HTTP_HOST,    \n");
			query.append("         HTTP_PORT,    \n");
			query.append("         HTTP_USER,    \n");
			query.append("         HTTP_PASSWORD,    \n");
			query.append("         HTTP_DESTINATION,    \n");
			query.append("	       MQ_HOST,     \n");
			query.append("	       MQ_PORT,     \n");
			query.append("	       MQ_USER,     \n");
			query.append("	       MQ_PASSWORD,     \n");
			query.append("	       MQ_DESTINATION,     \n");
			query.append("	       STORAGE_DB_TYPE,     \n");
			query.append("	       STORAGE_HOST,     \n");
			query.append("	       STORAGE_PORT,     \n");
			query.append("	       STORAGE_USER,     \n");
			query.append("	       STORAGE_PASSWORD,     \n");
			query.append("	       STORAGE_KEYSPACE,     \n");
			query.append("	       INSERT_DATE    \n");
			query.append("	     )    \n");
			query.append("VALUES     \n");
			query.append("	     (    \n");
			query.append("	       ?,    \n");
			query.append("	       ?,     \n");
			query.append("	       ?,     \n");
			query.append("	       ?,     \n");
			query.append("	       ?,     \n");
			query.append("	       ?,     \n");
			query.append("	       ?,     \n");
			query.append("	       ?,     \n");
			query.append("	       ?,     \n");
			query.append("	       ?,     \n");
			query.append("	       ?,     \n");
			query.append("	       ?,     \n");
			query.append("	       ?,     \n");
			query.append("	       ?,     \n");
			query.append("	       ?,     \n");
			query.append("	       ?,     \n");
			query.append("	       ?,     \n");
			query.append("	       ?,     \n");
			query.append("	       ?,     \n");
			query.append("	       ?,     \n");
			query.append("	       ?,     \n");
			query.append("	       ?,     \n");
			query.append("	       SYSDATE    \n");
			query.append("	     )    \n");

			log.debug("\n query.toString() : {\n" + query.toString() + "}");
			//
			QueryRunner run = new QueryRunner();
			run.update(connection, query.toString(),
					new Object[] { site.getSite_id(), site.getSite_name(), site.getLat(), site.getLng(), site.getDescription(), site.getCompany_id(), site.getHttp_host(), site.getHttp_port(), site.getHttp_user(),
							site.getHttp_password(), site.getHttp_destination(), site.getMq_host(), site.getMq_port(), site.getMq_user(), site.getMq_password(), site.getMq_destination(),
							site.getStorage_db_type(), site.getStorage_host(), site.getStorage_port(), site.getStorage_user(), site.getStorage_password(), site.getStorage_keyspace() });

			connection.commit();
			
			//
			super.getDomainChangeEventBus().onSiteChanged(site.getSite_id());

		} catch (Exception ex) {
			log.error("Site insert exception : " + ex.getMessage());
			connection.rollback();
			throw ex;
		} finally {
			connection.setAutoCommit(true);
			super.closeConnection(connection);
		}
	}

	public void updateSite(Site site) throws Exception {
		//
		Connection connection = null;
		//
		try {
			//
			StringBuffer query = new StringBuffer();
			query.append("UPDATE MM_SITES    \n");
			query.append("   SET SITE_NAME = ?,    \n");
			query.append("       LAT = ?,    \n");
			query.append("       LNG = ?,    \n");
			query.append("       DESCRIPTION = ?,    \n");
			query.append("       COMPANY_ID = ?,    \n");
			query.append("       HTTP_HOST = ?,    \n");
			query.append("       HTTP_PORT = ?,    \n");
			query.append("       HTTP_USER = ?,    \n");
			query.append("       HTTP_PASSWORD = ?,    \n");
			query.append("       HTTP_DESTINATION = ?,    \n");
			query.append("       MQ_HOST = ?,    \n");
			query.append("       MQ_PORT = ?,    \n");
			query.append("       MQ_USER = ?,    \n");
			query.append("       MQ_PASSWORD = ?,    \n");
			query.append("       MQ_DESTINATION = ?,    \n");
			query.append("       STORAGE_DB_TYPE = ?,    \n");
			query.append("       STORAGE_HOST = ?,    \n");
			query.append("       STORAGE_PORT = ?,    \n");
			query.append("       STORAGE_USER = ?,    \n");
			query.append("       STORAGE_PASSWORD = ?,    \n");
			query.append("       STORAGE_KEYSPACE = ?,    \n");
			query.append("       UPDATE_DATE = SYSDATE   \n");
			query.append(" WHERE SITE_ID = ?    \n");

			log.debug("\n query.toString() : {\n" + query.toString() + "}");
			//
			QueryRunner run = new QueryRunner();
			connection = super.getConnection();
			run.update(connection, query.toString(),
					new Object[] { site.getSite_name(), site.getLat(), site.getLng(), site.getDescription(), site.getCompany_id(), site.getHttp_host(), site.getHttp_port(), site.getHttp_user(),
							site.getHttp_password(), site.getHttp_destination(), site.getMq_host(), site.getMq_port(), site.getMq_user(), site.getMq_password(), site.getMq_destination(),
							site.getStorage_db_type(), site.getStorage_host(), site.getStorage_port(), site.getStorage_user(), site.getStorage_password(), site.getStorage_keyspace(),
							site.getSite_id() });

			//
			super.getDomainChangeEventBus().onSiteChanged(site.getSite_id());
			
		} catch (Exception ex) {
			log.error("\n SiteDAO.updateSite Exception : " + ex.getMessage());
			throw ex;
		} finally {
			super.closeConnection(connection);
		}
	}

	public void deleteSite(Site site) throws Exception {
		//
		Connection connection = null;
		//
		try {
			//
			StringBuffer query = new StringBuffer();
			query.append("DELETE FROM MM_SITES    \n");
			query.append(" WHERE SITE_ID = ?    \n");

			log.debug("\n query.toString() : {\n" + query.toString() + "}");
			//
			QueryRunner run = new QueryRunner();
			connection = super.getConnection();
			run.update(connection, query.toString(), new Object[] { site.getSite_id() });
			
			//
			super.getDomainChangeEventBus().onSiteChanged(site.getSite_id());

		} catch (Exception ex) {
			log.error("\n SiteDAO.deleteSite Exception : " + ex.getMessage());
			throw ex;
		} finally {
			super.closeConnection(connection);
		}
	}

	public void updateMoveSite(Site site) {

	}

	public boolean companyIsDuplicated(String company_name) throws Exception {
		long resultCnt = 0;

		Connection connection = null;

		try {
			StringBuffer query = new StringBuffer();
			query.append("SELECT count(*)    		\n");
			query.append("  FROM MM_company  	 	\n");
			query.append(" WHERE company_name = ? 	\n");

			// log.info("\n query.toString() : {\n"+query.toString()+"}");
			QueryRunner run = new QueryRunner();
			connection = super.getConnection();
			resultCnt = run.query(connection, query.toString(), new ScalarHandler<Long>(), new Object[] { company_name });
		} catch (Exception ex) {
			log.error("\n SiteDAO.selectSiteInfo Exception : " + ex.getMessage());
			throw ex;
		} finally {
			super.closeConnection(connection);
		}

		if (resultCnt > 0) {
			return true;
		} else {
			return false;
		}
	}

	public boolean companyHasSite(String company_id) throws Exception {
		long resultCnt = 0;

		Connection connection = null;

		try {
			StringBuffer query = new StringBuffer();
			query.append("SELECT count(*)    		\n");
			query.append("  FROM MM_sites  	 	\n");
			query.append(" WHERE company_id = ? 	\n");

			// log.info("\n query.toString() : {\n"+query.toString()+"}");
			QueryRunner run = new QueryRunner();
			connection = super.getConnection();
			resultCnt = run.query(connection, query.toString(), new ScalarHandler<Long>(), new Object[] { company_id });
		} catch (Exception ex) {
			log.error("\n SiteDAO.companyHasSite Exception : " + ex.getMessage());
			throw ex;
		} finally {
			super.closeConnection(connection);
		}

		if (resultCnt > 0) {
			return true;
		} else {
			return false;
		}
	}

	public boolean siteHasAssetOrOPC(String site_id) throws Exception {
		long resultCnt = 0;

		Connection connection = null;

		try {
			StringBuffer query = new StringBuffer();
			query.append("SELECT a.count_opcs + b.count_asset   \n");
			query.append("  FROM                       			\n");
			query.append("	  (SELECT count(*) as count_opcs    \n");
			query.append("	    FROM MM_opcs                   \n");
			query.append("	   WHERE site_id = ?                \n");
			query.append("	  ) as a,                      		\n");
			query.append("	  (SELECT count(*) as count_asset   \n");
			query.append("	    FROM MM_asset_tree             \n");
			query.append("	   WHERE site_id = ?                \n");
			query.append("	     AND asset_type = 'A'           \n");
			query.append("	  ) as b                      		\n");

			// log.info("\n query.toString() : {\n"+query.toString()+"}");
			QueryRunner run = new QueryRunner();
			connection = super.getConnection();
			resultCnt = run.query(connection, query.toString(), new ScalarHandler<BigDecimal>(), new Object[] { site_id, site_id }).longValue();
		} catch (Exception ex) {
			log.error("\n SiteDAO.siteHasAssetOrOPC Exception : " + ex.getMessage());
			throw ex;
		} finally {
			super.closeConnection(connection);
		}

		if (resultCnt > 0) {
			return true;
		} else {
			return false;
		}
	}

	public boolean assetHasModel(String asset_id) throws Exception {
		long resultCnt = 0;

		Connection connection = null;

		try {
			StringBuffer query = new StringBuffer();
			query.append("SELECT count(*)				\n");
			query.append("  FROM MM_asset_tree			\n");
			query.append(" WHERE asset_type = 'M'		\n");
			query.append("   AND parent_asset_id = ?	\n");

			// log.info("\n query.toString() : {\n"+query.toString()+"}");
			QueryRunner run = new QueryRunner();
			connection = super.getConnection();
			resultCnt = run.query(connection, query.toString(), new ScalarHandler<Long>(), new Object[] { asset_id });
		} catch (Exception ex) {
			log.error("\n SiteDAO.assetHasModel Exception : " + ex.getMessage());
			throw ex;
		} finally {
			super.closeConnection(connection);
		}

		if (resultCnt > 0) {
			return true;
		} else {
			return false;
		}
	}

	public boolean modelHasTag(String asset_id) throws Exception {
		long resultCnt = 0;

		Connection connection = null;

		try {
			StringBuffer query = new StringBuffer();
			query.append("SELECT count(*) 				\n");
			query.append("  FROM MM_tags				\n");
			query.append(" WHERE linked_asset_id = ?	\n");

			// log.info("\n query.toString() : {\n"+query.toString()+"}");
			QueryRunner run = new QueryRunner();
			connection = super.getConnection();
			resultCnt = run.query(connection, query.toString(), new ScalarHandler<Long>(), new Object[] { asset_id });
		} catch (Exception ex) {
			log.error("\n SiteDAO.modelHasTag Exception : " + ex.getMessage());
			throw ex;
		} finally {
			super.closeConnection(connection);
		}

		if (resultCnt > 0) {
			return true;
		} else {
			return false;
		}
	}

	public boolean opcHasTag(String opc_id) throws Exception {
		long resultCnt = 0;

		Connection connection = null;

		try {
			StringBuffer query = new StringBuffer();
			query.append("SELECT count(*) 	\n");
			query.append("  FROM MM_tags	\n");
			query.append(" WHERE opc_id = ?	\n");

			// log.info("\n query.toString() : {\n"+query.toString()+"}");
			QueryRunner run = new QueryRunner();
			connection = super.getConnection();
			resultCnt = run.query(connection, query.toString(), new ScalarHandler<Long>(), new Object[] { opc_id });
		} catch (Exception ex) {
			log.error("\n SiteDAO.opcHasTag Exception : " + ex.getMessage());
			throw ex;
		} finally {
			super.closeConnection(connection);
		}

		if (resultCnt > 0) {
			return true;
		} else {
			return false;
		}
	}

	
	
	
	
	public void insertVirtualSite(Site site) throws Exception {
		//
		Connection connection = null;
		//
		try {
			connection = super.getConnection();
			connection.setAutoCommit(false);
			
			site = overrideSiteDefautInfo(site);
			
			//
			StringBuffer query = new StringBuffer();
			query.append("INSERT INTO MM_SITES    \n");
			query.append("	     (    \n");
			query.append("	       SITE_ID,     \n");
			query.append("	       SITE_NAME,     \n");
			query.append("	       LAT,     \n");
			query.append("	       LNG,     \n");
			query.append("	       DESCRIPTION,     \n");
			query.append("	       COMPANY_ID,     \n");
			query.append("         HTTP_HOST,    \n");
			query.append("         HTTP_PORT,    \n");
			query.append("         HTTP_USER,    \n");
			query.append("         HTTP_PASSWORD,    \n");
			query.append("         HTTP_DESTINATION,    \n");
			query.append("	       MQ_HOST,     \n");
			query.append("	       MQ_PORT,     \n");
			query.append("	       MQ_USER,     \n");
			query.append("	       MQ_PASSWORD,     \n");
			query.append("	       MQ_DESTINATION,     \n");
			query.append("	       STORAGE_DB_TYPE,     \n");
			query.append("	       STORAGE_HOST,     \n");
			query.append("	       STORAGE_PORT,     \n");
			query.append("	       STORAGE_USER,     \n");
			query.append("	       STORAGE_PASSWORD,     \n");
			query.append("	       STORAGE_KEYSPACE,     \n");
			query.append("	       INSERT_DATE    \n");
			query.append("	     )    \n");
			query.append("VALUES     \n");
			query.append("	     (    \n");
			query.append("	       ?,    \n");
			query.append("	       ?,     \n");
			query.append("	       ?,     \n");
			query.append("	       ?,     \n");
			query.append("	       ?,     \n");
			query.append("	       ?,     \n");
			query.append("	       ?,     \n");
			query.append("	       ?,     \n");
			query.append("	       ?,     \n");
			query.append("	       ?,     \n");
			query.append("	       ?,     \n");
			query.append("	       ?,     \n");
			query.append("	       ?,     \n");
			query.append("	       ?,     \n");
			query.append("	       ?,     \n");
			query.append("	       ?,     \n");
			query.append("	       ?,     \n");
			query.append("	       ?,     \n");
			query.append("	       ?,     \n");
			query.append("	       ?,     \n");
			query.append("	       ?,     \n");
			query.append("	       ?,     \n");
			query.append("	       SYSDATE    \n");
			query.append("	     )    \n");

			log.debug("\n query.toString() : {\n" + query.toString() + "}");
			//
			QueryRunner run = new QueryRunner();
			run.update(connection, query.toString(),
					new Object[] { site.getSite_id(), site.getSite_name(), site.getLat(), site.getLng(), site.getDescription(), site.getCompany_id(), site.getHttp_host(), site.getHttp_port(), site.getHttp_user(),
							site.getHttp_password(), site.getHttp_destination(), site.getMq_host(), site.getMq_port(), site.getMq_user(), site.getMq_password(), site.getMq_destination(),
							site.getStorage_db_type(), site.getStorage_host(), site.getStorage_port(), site.getStorage_user(), site.getStorage_password(), site.getStorage_keyspace() });

			connection.commit();
			
			//
			super.getDomainChangeEventBus().onSiteChanged(site.getSite_id());

		} catch (Exception ex) {
			log.error("Site insert exception : " + ex.getMessage());
			connection.rollback();
			throw ex;
		} finally {
			connection.setAutoCommit(true);
			super.closeConnection(connection);
		}
	}

	public void updateVirtualSite(Site site) throws Exception {
		//
		Connection connection = null;
		//
		try {
			
			site = overrideSiteDefautInfo(site);
			
			//
			StringBuffer query = new StringBuffer();
			query.append("UPDATE MM_SITES    \n");
			query.append("   SET SITE_NAME = ?,    \n");
			query.append("       LAT = ?,    \n");
			query.append("       LNG = ?,    \n");
			query.append("       DESCRIPTION = ?,    \n");
			query.append("       COMPANY_ID = ?,    \n");
			query.append("       HTTP_HOST = ?,    \n");
			query.append("       HTTP_PORT = ?,    \n");
			query.append("       HTTP_USER = ?,    \n");
			query.append("       HTTP_PASSWORD = ?,    \n");
			query.append("       HTTP_DESTINATION = ?,    \n");
			query.append("       MQ_HOST = ?,    \n");
			query.append("       MQ_PORT = ?,    \n");
			query.append("       MQ_USER = ?,    \n");
			query.append("       MQ_PASSWORD = ?,    \n");
			query.append("       MQ_DESTINATION = ?,    \n");
			query.append("       STORAGE_DB_TYPE = ?,    \n");
			query.append("       STORAGE_HOST = ?,    \n");
			query.append("       STORAGE_PORT = ?,    \n");
			query.append("       STORAGE_USER = ?,    \n");
			query.append("       STORAGE_PASSWORD = ?,    \n");
			query.append("       STORAGE_KEYSPACE = ?,    \n");
			query.append("       UPDATE_DATE = SYSDATE   \n");
			query.append(" WHERE SITE_ID = ?    \n");

			log.debug("\n query.toString() : {\n" + query.toString() + "}");
			//
			QueryRunner run = new QueryRunner();
			connection = super.getConnection();
			run.update(connection, query.toString(),
					new Object[] { site.getSite_name(), site.getLat(), site.getLng(), site.getDescription(), site.getCompany_id(), site.getHttp_host(), site.getHttp_port(), site.getHttp_user(),
							site.getHttp_password(), site.getHttp_destination(), site.getMq_host(), site.getMq_port(), site.getMq_user(), site.getMq_password(), site.getMq_destination(),
							site.getStorage_db_type(), site.getStorage_host(), site.getStorage_port(), site.getStorage_user(), site.getStorage_password(), site.getStorage_keyspace(),
							site.getSite_id() });

			//
			super.getDomainChangeEventBus().onSiteChanged(site.getSite_id());
			
		} catch (Exception ex) {
			log.error("\n SiteDAO.updateSite Exception : " + ex.getMessage());
			throw ex;
		} finally {
			super.closeConnection(connection);
		}
	}

	public void deleteVirtualSite(Site site) throws Exception {
		//
		Connection connection = null;
		//
		try {
			//
			StringBuffer query = new StringBuffer();
			query.append("DELETE FROM MM_SITES    \n");
			query.append(" WHERE SITE_ID = ?    \n");

			log.debug("\n query.toString() : {\n" + query.toString() + "}");
			//
			QueryRunner run = new QueryRunner();
			connection = super.getConnection();
			run.update(connection, query.toString(), new Object[] { site.getSite_id() });
			
			//
			super.getDomainChangeEventBus().onSiteChanged(site.getSite_id());

		} catch (Exception ex) {
			log.error("\n SiteDAO.deleteSite Exception : " + ex.getMessage());
			throw ex;
		} finally {
			super.closeConnection(connection);
		}
	};
	
	
	public Site overrideSiteDefautInfo(Site site) throws Exception {

		if (site == null) return new Site();

		ServerConfiguration configuration = ConfigurationManager.getInstance().getServer_configuration();
		//

		// http
		site.setHttp_host(configuration.getHttp_host());
		site.setHttp_port(Integer.parseInt(configuration.getHttp_port()));
		site.setHttp_user(configuration.getHttp_user());
		site.setHttp_password(configuration.getHttp_password());
		site.setHttp_destination(configuration.getHttp_destination());

		// mq
		site.setMq_host(configuration.getMq_host());
		site.setMq_port(Integer.parseInt(configuration.getMq_port()));
		site.setMq_user(configuration.getMq_user());
		site.setMq_password(configuration.getMq_password());
		site.setMq_destination("");

		// storage
		site.setStorage_db_type(configuration.getStorage_db_type());
		site.setStorage_host(configuration.getStorage_host());
		site.setStorage_port(Integer.parseInt(configuration.getStorage_port()));
		site.setStorage_user(configuration.getStorage_user());
		site.setStorage_password(configuration.getStorage_password());
		site.setStorage_keyspace(configuration.getStorage_keyspace());

		// api
		String api_key = ConfigurationManager.getInstance().getApplication_properties().getProperty("api.key");
		site.setApi_key(api_key);

		return site;
	};
}
