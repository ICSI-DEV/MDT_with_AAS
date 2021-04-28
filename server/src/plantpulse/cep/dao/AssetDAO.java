package plantpulse.cep.dao;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.util.List;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.springframework.stereotype.Repository;

import plantpulse.cep.engine.config.ConfigurationManager;
import plantpulse.dbutils.QueryRunner;
import plantpulse.dbutils.handlers.BeanHandler;
import plantpulse.dbutils.handlers.BeanListHandler;
import plantpulse.dbutils.handlers.ScalarHandler;
import plantpulse.domain.Asset;
import plantpulse.domain.Site;

@Repository
public class AssetDAO extends CommonDAO {

	private static final Log log = LogFactory.getLog(AssetDAO.class);
	
	public static final String ASSET_TYPE_AREA = "A";
	
	public static final String ASSET_TYPE_ASSET = "M";
	
	
	public boolean hasAsset(String asset_id) throws Exception {

		Connection con = null;
		PreparedStatement pstmt = null;
		ResultSet rs = null;

		boolean aleady_inserted = false;
		//
		try {
			con = super.getConnection();
			//
			StringBuffer query = new StringBuffer();
			query.append("SELECT *    \n");
			query.append("  FROM MM_ASSET_TREE    \n");
			query.append(" WHERE ASSET_ID = '" + asset_id + "'   \n");
			//
			//
			pstmt = con.prepareStatement(query.toString());
			rs = pstmt.executeQuery();
			log.debug("[getStatementList query] : " + query.toString());
			//
			if (rs.next()) {
				aleady_inserted = true;
			}
			//
		} catch (Exception ex) {
			log.error(ex);
			throw ex;
		} finally {
			super.closeConnection(rs, pstmt, con);
		}
		//
		return aleady_inserted;
	}

	
	public List<Asset> selectAreaList(String site_id) throws Exception {

		Connection connection = null;
		List<Asset> list = null;
		//
		try {
			//
			StringBuffer query = new StringBuffer();
			query.append("SELECT ASSET_ID,     \n");
			query.append("       ASSET_NAME,     \n");
			query.append("       PARENT_ASSET_ID,     \n");
			query.append("       ASSET_ORDER,    \n");
			query.append("       ASSET_TYPE,     \n");
			query.append("       DESCRIPTION,     \n");
			query.append("       SITE_ID,    \n");
			query.append("       TABLE_TYPE    \n");
			query.append("  FROM MM_ASSET_TREE A    \n");
			query.append("  WHERE A.SITE_ID = '" + site_id + "' \n");
			query.append("   AND  A.ASSET_TYPE = 'A' \n");
			query.append("  ORDER BY ASSET_NAME \n");
			
			QueryRunner run = new QueryRunner();
			connection = super.getConnection();
			list = run.query(connection, query.toString(), new BeanListHandler<Asset>(Asset.class));

		} catch (Exception ex) {
			log.error("AssetDAO.selectAreaList Exception : " + ex.getMessage(), ex);
			throw ex;
		} finally {
			super.closeConnection(connection);
		}
		return list;
	};
	
	public List<Asset> selectEquipmentList(String parent_asset_id) throws Exception {

		Connection connection = null;
		List<Asset> list = null;
		//
		try {
			//
			StringBuffer query = new StringBuffer();
			query.append("SELECT ASSET_ID,     \n");
			query.append("       ASSET_NAME,     \n");
			query.append("       PARENT_ASSET_ID,     \n");
			query.append("       ASSET_ORDER,    \n");
			query.append("       ASSET_TYPE,     \n");
			query.append("       DESCRIPTION,     \n");
			query.append("       SITE_ID,    \n");
			query.append("       TABLE_TYPE    \n");
			query.append("  FROM MM_ASSET_TREE A    \n");
			query.append("  WHERE A.PARENT_ASSET_ID = '" + parent_asset_id + "' \n");
			query.append("   AND  A.ASSET_TYPE = 'M' \n");
			query.append("  ORDER BY ASSET_NAME \n");
			
			QueryRunner run = new QueryRunner();
			connection = super.getConnection();
			list = run.query(connection, query.toString(), new BeanListHandler<Asset>(Asset.class));

		} catch (Exception ex) {
			log.error("AssetDAO.selectAssetList Exception : " + ex.getMessage(), ex);
			throw ex;
		} finally {
			super.closeConnection(connection);
		}
		return list;
	};
	

	
	public List<Asset> selectAssets() throws Exception {

		Connection connection = null;
		List<Asset> list = null;
		//
		try {
			//
			StringBuffer query = new StringBuffer();
			query.append("SELECT ASSET_ID,     \n");
			query.append("       ASSET_NAME,     \n");
			query.append("       PARENT_ASSET_ID,     \n");
			query.append("       ASSET_ORDER,    \n");
			query.append("       ASSET_TYPE,     \n");
			query.append("       DESCRIPTION,     \n");
			query.append("       SITE_ID,    \n");
			query.append("       TABLE_TYPE    \n");
			query.append("  FROM MM_ASSET_TREE A    \n");
			// log.info("AssetDAO.selectAssets query :
			// {\n"+query.toString()+"}");

			QueryRunner run = new QueryRunner();
			connection = super.getConnection();
			list = run.query(connection, query.toString(), new BeanListHandler<Asset>(Asset.class));

			// log.info(list.toString());
			//
		} catch (Exception ex) {
			log.error("AssetDAO.selectAssets Exception : " + ex.getMessage(), ex);
			throw ex;
		} finally {
			super.closeConnection(connection);
		}
		return list;
	}

	

	public List<Asset> selectModels() throws Exception {

		Connection connection = null;
		List<Asset> list = null;
		//
		try {
			//
			StringBuffer query = new StringBuffer();
			query.append("SELECT ASSET_ID,     \n");
			query.append("       ASSET_NAME,     \n");
			query.append("       PARENT_ASSET_ID,     \n");
			query.append("       ASSET_ORDER,    \n");
			query.append("       ASSET_TYPE,     \n");
			query.append("       DESCRIPTION,     \n");
			query.append("       SITE_ID,    \n");
			query.append("       TABLE_TYPE    \n");
			query.append("  FROM MM_ASSET_TREE A    \n");
			query.append(" WHERE A.ASSET_TYPE = 'M'    \n");
			query.append(" ORDER BY ASSET_NAME    \n");

			// log.info("AssetDAO.selectAssets query :
			// {\n"+query.toString()+"}");

			QueryRunner run = new QueryRunner();
			connection = super.getConnection();
			list = run.query(connection, query.toString(), new BeanListHandler<Asset>(Asset.class));

			// log.info(list.toString());
			//
		} catch (Exception ex) {
			log.error("AssetDAO.selectModels Exception : " + ex.getMessage(), ex);
			throw ex;
		} finally {
			super.closeConnection(connection);
		}
		return list;
	}
	
	public List<Asset> selectParts() throws Exception {

		Connection connection = null;
		List<Asset> list = null;
		//
		try {
			//
			StringBuffer query = new StringBuffer();
			query.append("SELECT ASSET_ID,     \n");
			query.append("       ASSET_NAME,     \n");
			query.append("       PARENT_ASSET_ID,     \n");
			query.append("       ASSET_ORDER,    \n");
			query.append("       ASSET_TYPE,     \n");
			query.append("       DESCRIPTION,     \n");
			query.append("       SITE_ID,    \n");
			query.append("       TABLE_TYPE    \n");
			query.append("  FROM MM_ASSET_TREE A    \n");
			query.append(" WHERE A.ASSET_TYPE = 'A'    \n");
			query.append(" ORDER BY ASSET_NAME    \n");

			// log.info("AssetDAO.selectAssets query :
			// {\n"+query.toString()+"}");

			QueryRunner run = new QueryRunner();
			connection = super.getConnection();
			list = run.query(connection, query.toString(), new BeanListHandler<Asset>(Asset.class));

			// log.info(list.toString());
			//
		} catch (Exception ex) {
			log.error("AssetDAO.selectParts Exception : " + ex.getMessage(), ex);
			throw ex;
		} finally {
			super.closeConnection(connection);
		}
		return list;
	}

	public List<Asset> selectCustomAssets(String opcId1, String opcId2, String exceptTagId, Asset asset) throws Exception {

		Connection connection = null;
		List<Asset> list = null;
		
		
		
		//
		try {

			//
			String description_column = ConfigurationManager.getInstance().getApplication_properties().getProperty("storage.display.description.column");

			//
			StringBuffer query = new StringBuffer();
			//LEVEL 1
			query.append("SELECT ASSET_ID AS ID,  ");
			if (asset.isShow_description()) {
				query.append("      ( CASE WHEN (NVL(" + description_column + ", '') = '') THEN ASSET_NAME ELSE " + description_column + " END ) AS TEXT, ");
			} else {
				query.append("      ( CASE WHEN (NVL(" + description_column + ", '') = '') THEN ASSET_NAME ELSE " + description_column + " END ) AS TEXT, ");
				//query.append("       ASSET_NAME AS TEXT, ");
			}
			query.append("       '#' AS PARENT,  ");
			query.append("       0 AS ASSET_ORDER,   ");
			query.append("       'S' AS TYPE,   ");
			query.append("       SITE_ID   ");
			query.append("  FROM MM_ASSET_TREE A   ");
			query.append(" WHERE A.SITE_ID = ?  ");
			query.append("   AND A.ASSET_ID = ? ");
			//LEVEL 2
			query.append(" UNION ALL			  ");
			query.append("SELECT DISTINCT  ");
			query.append("       SUBSTRING(TAG_NAME, 0, INSTR(TAG_NAME, '.')), ");
			query.append("       SUBSTRING(TAG_NAME, 0, INSTR(TAG_NAME, '.')), ");
			query.append("       LINKED_ASSET_ID,   ");
			query.append("       DECODE(SUBSTRING(TAG_NAME, 0, INSTR(TAG_NAME, '.')), 'VIBRATION', 10, 20),   ");
			query.append("       'A',   ");
			query.append("       ?   ");
			query.append("  FROM MM_TAGS T   ");
			query.append(" WHERE T.OPC_ID IN (?, ?)   ");
			query.append("   AND T.TAG_ID != ?   ");
			//LEVEL 3
			query.append(" UNION ALL ");
			query.append("SELECT DISTINCT ");
			query.append("       SUBSTRING(TAG_NAME, 0, INSTR(TAG_NAME, '.')), ");
			query.append("       SUBSTRING(TAG_NAME, 0, INSTR(TAG_NAME, '.')), ");
			query.append("       PARENT, ");
			query.append("       DECODE(ASSET_ORDER, NULL, DECODE(SUBSTRING(TAG_NAME, 0, INSTR(TAG_NAME, '.')), 'PARAMETER', 100, 200), ASSET_ORDER) + 1000,   ");
			query.append("       'M',   ");
			query.append("       ? ");
			query.append("  FROM ( ");
			query.append("         SELECT SUBSTRING(TAG_NAME, INSTR(TAG_NAME, '.') + 1, LENGTH(TAG_NAME)) AS TAG_NAME, ");
			query.append("                SUBSTRING(TAG_NAME, 0, INSTR(TAG_NAME, '.')) AS PARENT, ");
			query.append("                TO_NUMBER(REGEXP_SUBSTRING(REGEXP_SUBSTRING(TAG_NAME, '.[A-Z]{2}[0-9]{1,2}.'), '[0-9]{1,2}')) AS ASSET_ORDER ");
			query.append("	       FROM MM_TAGS   ");
			query.append("	      WHERE OPC_ID IN (?, ?)     ");
			query.append("          AND TAG_ID != ?   ");
			query.append("       )          ");
			//LEVEL 4
			query.append(" UNION ALL        ");
			query.append("SELECT TAG_ID,    ");
			if (asset.isShow_description()) {
				query.append("      ( CASE WHEN (NVL(" + description_column + ", '') = '') THEN TAG_NAME ELSE " + description_column + " END ) AS TAG_NAME,   ");
			} else {
				query.append("      ( CASE WHEN (NVL(" + description_column + ", '') = '') THEN TAG_NAME ELSE " + description_column + " END ) AS TAG_NAME,   ");
				//query.append("      ( CASE WHEN (NVL(ALIAS_NAME, '') = '') THEN SUBSTRING(TAG_NAME, INSTR(TAG_NAME, '.') +1, LENGTH(TAG_NAME)) ELSE SUBSTRING(TAG_NAME, INSTR(TAG_NAME, '.') +1, LENGTH(TAG_NAME)) || ' [' || ALIAS_NAME || ']' END ) AS TAG_NAME,   ");
			}
			query.append("	   SUBSTRING(TAG_NAME, 0, INSTR(TAG_NAME, '.')),   ");
			query.append("	   ASSET_ORDER,   ");
			query.append("	   'P',   ");
			query.append("	   ? ");
			query.append(" FROM ( ");
			query.append("		SELECT TAG_ID, ");
			query.append("		       SUBSTRING(TAG_NAME, INSTR(TAG_NAME, '.') + 1, LENGTH(TAG_NAME)) AS TAG_NAME, ");
			query.append("		       DESCRIPTION, ");
			query.append("		       ALIAS_NAME, ");
			query.append("		       TO_NUMBER(REGEXP_SUBSTRING(TAG_ID, '[0-9]+')) + 10000 AS ASSET_ORDER ");
			query.append("		  FROM MM_TAGS  ");
			query.append("		 WHERE LINKED_ASSET_ID IN (SELECT ASSET_ID FROM MM_ASSET_TREE WHERE SITE_ID = ?)   ");
			query.append("		   AND ((OPC_ID = ? AND SUBSTRING(TAG_NAME, LENGTH(TAG_NAME) -1, 2) = 'PV') OR OPC_ID = ?) ");
			query.append("		   AND TAG_ID != ? ");
			query.append("	    )  ");
			query.append("ORDER BY 4 ");
			
			log.info("AssetDAO.selectCustomAssets query : {\n" + query.toString() + "}");
			log.debug("Parameters : [" + asset.toString() + "]");

			QueryRunner run = new QueryRunner();
			connection = super.getConnection();
			list = run.query(connection, query.toString(), new BeanListHandler<Asset>(Asset.class),
					new Object[] { 
							asset.getSite_id(), 
							asset.getAsset_id(), 
							asset.getSite_id(), 
							opcId1,
							opcId2,
							exceptTagId,
							asset.getSite_id(),
							opcId1,
							opcId2,
							exceptTagId,
							asset.getSite_id(), 
							asset.getSite_id(),
							opcId1,
							opcId2,
							exceptTagId
					});

			log.debug(list.toString());
			//
		} catch (Exception ex) {
			log.error("AssetDAO.selectAssets Exception : " + ex.getMessage(), ex);
			throw ex;
		} finally {
			super.closeConnection(connection);
		}
		return list;
	}
	
	public List<Asset> selectCustomAssets2(String opcId, Asset asset) throws Exception {
		
		Connection connection = null;
		List<Asset> list = null;
		
		
		try {
			
			//
			String description_column = ConfigurationManager.getInstance().getApplication_properties().getProperty("storage.display.description.column");
			
			//
			StringBuffer query = new StringBuffer();
			//LEVEL 1
			query.append("SELECT ASSET_ID AS ID,  ");
			if (asset.isShow_description()) {
				query.append("      ( CASE WHEN (NVL(" + description_column + ", '') = '') THEN ASSET_NAME ELSE " + description_column + " END ) AS TEXT, ");
			} else {
				query.append("      ( CASE WHEN (NVL(" + description_column + ", '') = '') THEN ASSET_NAME ELSE " + description_column + " END ) AS TEXT, ");
				//query.append("       ASSET_NAME AS TEXT, ");
			}
			query.append("       '#' AS PARENT,  ");
			query.append("       0 AS ASSET_ORDER,   ");
			query.append("       'S' AS TYPE,   ");
			query.append("       SITE_ID   ");
			query.append("  FROM MM_ASSET_TREE A   ");
			query.append(" WHERE A.SITE_ID = ?  ");
			query.append("   AND A.ASSET_ID = ? ");
			//LEVEL 2
			query.append(" UNION ALL			  ");
			query.append("SELECT DISTINCT  ");
			query.append("       SUBSTRING(TAG_NAME, 0, INSTR(TAG_NAME, '.')), ");
			query.append("       SUBSTRING(TAG_NAME, 0, INSTR(TAG_NAME, '.')), ");
			query.append("       LINKED_ASSET_ID,   ");
			query.append("       DECODE(SUBSTRING(TAG_NAME, 0, INSTR(TAG_NAME, '.')), 'VIBRATION', 10, 20),   ");
			query.append("       'A',   ");
			query.append("       ?   ");
			query.append("  FROM MM_TAGS T   ");
			query.append(" WHERE T.OPC_ID IN (?)   ");
			//LEVEL 3
			query.append(" UNION ALL ");
			query.append("SELECT DISTINCT ");
			query.append("       SUBSTRING(TAG_NAME, 0, INSTR(TAG_NAME, '.')), ");
			query.append("       SUBSTRING(TAG_NAME, 0, INSTR(TAG_NAME, '.')), ");
			query.append("       PARENT, ");
			query.append("       DECODE(ASSET_ORDER, NULL, DECODE(SUBSTRING(TAG_NAME, 0, INSTR(TAG_NAME, '.')), 'PARAMETER', 100, 200), ASSET_ORDER) + 1000,   ");
			query.append("       'M',   ");
			query.append("       ? ");
			query.append("  FROM ( ");
			query.append("         SELECT SUBSTRING(TAG_NAME, INSTR(TAG_NAME, '.') + 1, LENGTH(TAG_NAME)) AS TAG_NAME, ");
			query.append("                SUBSTRING(TAG_NAME, 0, INSTR(TAG_NAME, '.')) AS PARENT, ");
			query.append("                TO_NUMBER(REGEXP_SUBSTRING(REGEXP_SUBSTRING(TAG_NAME, '.[A-Z]{2}[0-9]{1,2}.'), '[0-9]{1,2}')) AS ASSET_ORDER ");
			query.append("	       FROM MM_TAGS   ");
			query.append("	      WHERE OPC_ID IN (?)     ");
			query.append("        ) ");
			//LEVEL 4
			query.append(" UNION ALL        ");
			query.append("SELECT TAG_ID,   ");
			
			if (asset.isShow_description()) {
				query.append("      ( CASE WHEN (NVL(" + description_column + ", '') = '') THEN TAG_NAME ELSE " + description_column + " END ) AS TAG_NAME,   ");
			} else {
				query.append("      ( CASE WHEN (NVL(" + description_column + ", '') = '') THEN TAG_NAME ELSE " + description_column + " END ) AS TAG_NAME,   ");
				//query.append("      ( CASE WHEN (NVL(ALIAS_NAME, '') = '') THEN SUBSTRING(TAG_NAME, INSTR(TAG_NAME, '.') +1, LENGTH(TAG_NAME)) ELSE SUBSTRING(TAG_NAME, INSTR(TAG_NAME, '.') +1, LENGTH(TAG_NAME)) || ' [' || ALIAS_NAME || ']' END ) AS TAG_NAME,   ");
			}
			query.append("	   SUBSTRING(TAG_NAME, 0, INSTR(TAG_NAME, '.')),   ");
			query.append("	   ASSET_ORDER,   ");
			query.append("	   'P',   ");
			query.append("	   ? ");
			query.append(" FROM ( ");
			query.append("		SELECT TAG_ID, ");
			query.append("		       SUBSTRING(TAG_NAME, INSTR(TAG_NAME, '.') + 1, LENGTH(TAG_NAME)) AS TAG_NAME, ");
			query.append("		       DESCRIPTION, ");
			query.append("		       ALIAS_NAME, ");
			query.append("		       TO_NUMBER(REGEXP_SUBSTRING(TAG_ID, '[0-9]+')) + 10000 AS ASSET_ORDER ");
			query.append("		  FROM MM_TAGS  ");
			query.append("		 WHERE LINKED_ASSET_ID IN (SELECT ASSET_ID FROM MM_ASSET_TREE WHERE SITE_ID = ?)   ");
			query.append("		   AND OPC_ID = ? ");
			query.append("	  )  ");
			query.append("ORDER BY 4 ");
			
			log.info("AssetDAO.selectCustomAssets2 query : {\n" + query.toString() + "}");
			log.debug("Parameters : [" + asset.toString() + "]");
			
			QueryRunner run = new QueryRunner();
			connection = super.getConnection();
			list = run.query(connection, query.toString(), new BeanListHandler<Asset>(Asset.class),
					new Object[] { 
							asset.getSite_id(), 
							asset.getAsset_id(), 
							asset.getSite_id(), 
							opcId,
							asset.getSite_id(), 
							opcId,
							asset.getSite_id(), 
							asset.getSite_id(),
							opcId
			});
			
			log.debug(list.toString());
			//
		} catch (Exception ex) {
			log.error("AssetDAO.selectAssets Exception : " + ex.getMessage(), ex);
			throw ex;
		} finally {
			super.closeConnection(connection);
		}
		return list;
	}
	
	public List<Asset> selectAssets(Asset asset) throws Exception {
		
		Connection connection = null;
		List<Asset> list = null;
		//
		try {
			
			//
			String description_column = ConfigurationManager.getInstance().getApplication_properties().getProperty("storage.display.description.column");
			
			//
			StringBuffer query = new StringBuffer();
			query.append("SELECT SITE_ID AS ID,     \n");
			if (asset.isShow_description()) {
				query.append("      ( CASE WHEN (NVL(S.SITE_NAME, '') = '') THEN S.SITE_NAME ELSE S.DESCRIPTION END ) AS TEXT,    \n");
			} else {
				query.append("       S.SITE_NAME AS TEXT,     \n");
			}
			;
			query.append("       '#' AS PARENT,    \n");
			query.append("       0 AS ASSET_ORDER,    \n");
			query.append("       'S' AS TYPE,     \n");
			query.append("       SITE_ID     \n");
			query.append("  FROM MM_SITES S    \n");
			query.append(" WHERE SITE_ID = ?    \n");
			query.append(" UNION ALL    \n");
			query.append("SELECT ASSET_ID,     \n");
			if (asset.isShow_description()) {
				query.append("      ( CASE WHEN (NVL(" + description_column + ", '') = '') THEN ASSET_NAME ELSE " + description_column + " END ) AS ASSET_NAME,    \n");
			} else {
				query.append("       ASSET_NAME,    \n");
			}
			;
			query.append("       PARENT_ASSET_ID,     \n");
			query.append("       ASSET_ORDER,    \n");
			query.append("       ASSET_TYPE,     \n");
			query.append("       SITE_ID    \n");
			query.append("  FROM MM_ASSET_TREE A    \n");
			query.append(" WHERE A.SITE_ID = ?    \n");
			query.append(" UNION ALL    \n");
			query.append("SELECT TAG_ID,    \n");
			if (asset.isShow_description()) {
				query.append("      ( CASE WHEN (NVL(" + description_column + ", '') = '') THEN TAG_NAME ELSE " + description_column + " END ) AS TAG_NAME,    \n");
			} else {
				query.append("      ( CASE WHEN (NVL(ALIAS_NAME, '') = '') THEN TAG_NAME ELSE '[' || ALIAS_NAME || '] ' || TAG_NAME   END ) AS TAG_NAME,    \n");
			}
			;
			query.append("       LINKED_ASSET_ID,    \n");
			query.append("       2,    \n");
			query.append("       'P',    \n");
			query.append("       ?    \n");
			query.append("  FROM MM_TAGS T    \n");
			query.append(" WHERE T.LINKED_ASSET_ID IN (SELECT ASSET_ID FROM MM_ASSET_TREE WHERE SITE_ID = ?)       \n");
			query.append(" ORDER BY 3,4,2    \n");
			
			log.debug("AssetDAO.selectAssets query : {\n" + query.toString() + "}");
			log.debug("\n Parameters : [" + asset.toString() + "]");
			
			QueryRunner run = new QueryRunner();
			connection = super.getConnection();
			list = run.query(connection, query.toString(), new BeanListHandler<Asset>(Asset.class),
					new Object[] { asset.getSite_id(), asset.getSite_id(), asset.getSite_id(), asset.getSite_id() });
			
			log.debug(list.toString());
			//
		} catch (Exception ex) {
			log.error("AssetDAO.selectAssets Exception : " + ex.getMessage(), ex);
			throw ex;
		} finally {
			super.closeConnection(connection);
		}
		return list;
	}
	
	
	public List<Asset> selectAssetsNoTags(Asset asset) throws Exception {

		Connection connection = null;
		List<Asset> list = null;
		//
		try {

			//
			String description_column = ConfigurationManager.getInstance().getApplication_properties().getProperty("storage.display.description.column");

			//
			StringBuffer query = new StringBuffer();
			query.append("SELECT SITE_ID AS ID,     \n");
			if (asset.isShow_description()) {
				query.append("      ( CASE WHEN (NVL(S.SITE_NAME, '') = '') THEN S.SITE_NAME ELSE S.DESCRIPTION END ) AS TEXT,    \n");
			} else {
				query.append("       S.SITE_NAME AS TEXT,     \n");
			}
			;
			query.append("       '#' AS PARENT,    \n");
			query.append("       0 AS ASSET_ORDER,    \n");
			query.append("       'S' AS TYPE,     \n");
			query.append("       SITE_ID     \n");
			query.append("  FROM MM_SITES S    \n");
			query.append(" WHERE SITE_ID = ?    \n");
			query.append(" UNION ALL    \n");
			query.append("SELECT ASSET_ID,     \n");
			if (asset.isShow_description()) {
				query.append("      ( CASE WHEN (NVL(" + description_column + ", '') = '') THEN ASSET_NAME ELSE " + description_column + " END ) AS ASSET_NAME,    \n");
			} else {
				query.append("       ASSET_NAME,    \n");
			}
			;
			query.append("       PARENT_ASSET_ID,     \n");
			query.append("       ASSET_ORDER,    \n");
			query.append("       ASSET_TYPE,     \n");
			query.append("       SITE_ID    \n");
			query.append("  FROM MM_ASSET_TREE A    \n");
			query.append(" WHERE A.SITE_ID = ?    \n");
			
			query.append(" ORDER BY 2,3    \n");

			log.debug("AssetDAO.selectAssets query : {\n" + query.toString() + "}");
			log.debug("\n Parameters : [" + asset.toString() + "]");

			QueryRunner run = new QueryRunner();
			connection = super.getConnection();
			list = run.query(connection, query.toString(), new BeanListHandler<Asset>(Asset.class),
					new Object[] { asset.getSite_id(), asset.getSite_id() });

			log.debug(list.toString());
			//
		} catch (Exception ex) {
			log.error("AssetDAO.selectAssetsNoTags Exception : " + ex.getMessage(), ex);
			throw ex;
		} finally {
			super.closeConnection(connection);
		}
		return list;
	}
	
	public int selectAssetsCountBySiteIdAndAssetType(String site_id, String asset_type) throws Exception {

		Connection connection = null;
		List<Asset> assets = null;
		int count = 0;
		//
		try {
			//
			StringBuffer query = new StringBuffer();
			query.append("SELECT SITE_ID, ASSET_ID, PARENT_ASSET_ID, ASSET_NAME, ASSET_ORDER, ASSET_TYPE, TABLE_TYPE, DESCRIPTION  ");
			query.append("  FROM MM_ASSET_TREE     \n");
			query.append(" WHERE SITE_ID = ?    \n");
			query.append("   AND ASSET_TYPE = ?    \n");

			QueryRunner run = new QueryRunner();
			connection = super.getConnection();
			assets = run.query(connection, query.toString(), new BeanListHandler<Asset>(Asset.class), new Object[] { site_id, asset_type });
			if(assets != null) {
				count = assets.size();
			}

		} catch (Exception ex) {
			log.error("AssetDAO.selectAssetsBySiteIdAndAssetType Exception : " + ex.getMessage(), ex);
			throw ex;
		} finally {
			super.closeConnection(connection);
		}
		return count;
	}
	
	public List<Asset> selectAssetsBySiteIdAndAssetType(String site_id, String asset_type) throws Exception {

		Connection connection = null;
		List<Asset> assets = null;
		//
		try {
			//
			StringBuffer query = new StringBuffer();
			query.append("SELECT SITE_ID, ASSET_ID, PARENT_ASSET_ID, ASSET_NAME, ASSET_ORDER, ASSET_TYPE, TABLE_TYPE, DESCRIPTION  ");
			query.append("  FROM MM_ASSET_TREE     \n");
			query.append(" WHERE SITE_ID = ?    \n");
			query.append("   AND ASSET_TYPE = ?    \n");

			QueryRunner run = new QueryRunner();
			connection = super.getConnection();
			assets = run.query(connection, query.toString(), new BeanListHandler<Asset>(Asset.class), new Object[] { site_id, asset_type });

			// log.info(list.toString());
			//
		} catch (Exception ex) {
			log.error("AssetDAO.selectAssetsBySiteIdAndAssetType Exception : " + ex.getMessage(), ex);
			throw ex;
		} finally {
			super.closeConnection(connection);
		}
		return assets;
	}
	
	
	public List<Asset> selectAssetsByParentAssetId(String site_id, String parent_asset_id) throws Exception {

		Connection connection = null;
		List<Asset> assets = null;
		//
		try {
			//
			StringBuffer query = new StringBuffer();
			query.append("SELECT SITE_ID, ASSET_ID, PARENT_ASSET_ID, ASSET_NAME, ASSET_ORDER, ASSET_TYPE, TABLE_TYPE, DESCRIPTION  ");
			query.append("  FROM MM_ASSET_TREE     \n");
			query.append(" WHERE SITE_ID = ?    \n");
			query.append("   AND PARENT_ASSET_ID = ?    \n");

			QueryRunner run = new QueryRunner();
			connection = super.getConnection();
			assets = run.query(connection, query.toString(), new BeanListHandler<Asset>(Asset.class), new Object[] { site_id, parent_asset_id });

			// log.info(list.toString());
			//
		} catch (Exception ex) {
			log.error("AssetDAO.selectAssetsBySiteIdAndAssetType Exception : " + ex.getMessage(), ex);
			throw ex;
		} finally {
			super.closeConnection(connection);
		}
		return assets;
	}
	

	public Asset selectAsset(String asset_id) throws Exception {

		Connection connection = null;
		Asset asset = null;
		//
		try {
			//
			StringBuffer query = new StringBuffer();
			query.append("SELECT *     \n");
			query.append("  FROM MM_ASSET_TREE A    \n");
			query.append(" WHERE A.ASSET_ID = ?    \n");

			QueryRunner run = new QueryRunner();
			connection = super.getConnection();
			asset = run.query(connection, query.toString(), new BeanHandler<Asset>(Asset.class), new Object[] { asset_id });

		} catch (Exception ex) {
			log.error("AssetDAO.selectAsset Exception : " + ex.getMessage(), ex);
			throw ex;
		} finally {
			super.closeConnection(connection);
		}
		return asset;
	}
	
   public Asset selectAssetOrSite(String asset_id) throws Exception {
		Asset asset = this.selectAsset(asset_id);
	    if(asset == null){ //에셋이 없을 경우, 사이트에서 정보 조회
	    	 Site site = new SiteDAO().selectSiteInfo(asset_id);
	    	 if(site != null){
	        	 asset = new Asset();
	        	 asset.setAsset_id(site.getSite_id());
	        	 asset.setAsset_name(site.getSite_name());
	        	 asset.setDescription(site.getDescription());
	    	 }
	    };
	    if(asset == null){
	    	throw new Exception("Asset is null.");
	    }
	    return asset;
	};
	

	public void insertAsset(Asset asset) throws Exception {
		//
		Connection connection = null;

		//
		try {
			//
			connection = super.getConnection();
			connection.setAutoCommit(false);
			
			//
			StringBuffer nquery = new StringBuffer();
			QueryRunner nrun = new QueryRunner();
			nquery.append("SELECT 'ASSET_' || LPAD(NEXT VALUE FOR ASSET_SEQ, 5, '0') AS ASSET_ID FROM DUAL   \n");
			String asset_id = nrun.query(connection, nquery.toString(), new ScalarHandler<String>(), new Object[] {});
			asset.setAsset_id(asset_id);

			StringBuffer query = new StringBuffer();
			query.append("INSERT INTO MM_ASSET_TREE    \n");
			query.append("       (    \n");
			query.append("         ASSET_ID,    \n");
			query.append("         ASSET_NAME,    \n");
			query.append("         PARENT_ASSET_ID,    \n");
			query.append("         ASSET_ORDER,    \n");
			query.append("         ASSET_TYPE,    \n");
			query.append("         SITE_ID,    \n");
			query.append("         TABLE_TYPE,    \n");
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
			query.append("         ?,    \n");
			query.append("         ?,    \n");
			query.append("         ?,    \n");
			query.append("         SYSDATE    \n");
			query.append("       )    \n");

			log.debug("\n query.toString() : {\n" + query.toString() + "}");
			log.debug("\n Parameter : [" + asset.toString() + "]");
			//
			QueryRunner run = new QueryRunner();
			run.update(connection, query.toString(),
					new Object[] { asset.getAsset_id(), asset.getAsset_name(), asset.getParent_asset_id(), asset.getAsset_order(), asset.getAsset_type(),
							asset.getSite_id(), 
							asset.getTable_type(),
							asset.getDescription() });

			connection.commit();

			//
			super.getDomainChangeEventBus().onAssetChanged(asset.getAsset_id());
			
		} catch (Exception ex) {
			log.error("AssetDAO.insertAsset Exception : " + ex.getMessage());
			connection.rollback();
			throw ex;
		} finally {
			connection.setAutoCommit(true);
			super.closeConnection(connection);
		}
	};
	
	
	public void insertVAsset(Asset asset) throws Exception {
		//
		Connection connection = null;

		//
		try {
			//
			connection = super.getConnection();
			connection.setAutoCommit(false);

			StringBuffer query = new StringBuffer();
			query.append("INSERT INTO MM_ASSET_TREE    \n");
			query.append("       (    \n");
			query.append("         ASSET_ID,    \n");
			query.append("         ASSET_NAME,    \n");
			query.append("         PARENT_ASSET_ID,    \n");
			query.append("         ASSET_ORDER,    \n");
			query.append("         ASSET_TYPE,    \n");
			query.append("         SITE_ID,    \n");
			query.append("         TABLE_TYPE,    \n");
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
			query.append("         ?,    \n");
			query.append("         ?,    \n");
			query.append("         ?,    \n");
			query.append("         SYSDATE    \n");
			query.append("       )    \n");

			log.debug("\n query.toString() : {\n" + query.toString() + "}");
			log.debug("\n Parameter : [" + asset.toString() + "]");
			//
			QueryRunner run = new QueryRunner();
			run.update(connection, query.toString(),
					new Object[] { asset.getAsset_id(), asset.getAsset_name(), asset.getParent_asset_id(), asset.getAsset_order(), asset.getAsset_type(),
							asset.getSite_id(), 
							asset.getTable_type(),
							asset.getDescription() });

			connection.commit();

			//
			super.getDomainChangeEventBus().onAssetChanged(asset.getAsset_id());
			
		} catch (Exception ex) {
			log.error("AssetDAO.insertAsset Exception : " + ex.getMessage());
			connection.rollback();
			throw ex;
		} finally {
			connection.setAutoCommit(true);
			super.closeConnection(connection);
		}
	}

	public void updateAssetWidhEquipment(Asset asset) throws Exception {
		//
		Connection connection = null;
		//
		try {
			//
			StringBuffer query = new StringBuffer();
			query.append("UPDATE MM_ASSET_TREE    \n");
			query.append("   SET ASSET_NAME  = ?,    \n");
			query.append("       DESCRIPTION = ?,    \n");
			query.append("       TABLE_TYPE = ?,    \n");
			query.append("       ASSET_SVG_IMG = ?,    \n");
			query.append("       ATTRIBUTE_01 = ?,    \n");
			query.append("       ATTRIBUTE_02 = ?,    \n");
			query.append("       ATTRIBUTE_03 = ?,    \n");
			query.append("       ATTRIBUTE_04 = ?,    \n");
			query.append("       ATTRIBUTE_05 = ?,    \n");
			query.append("       ATTRIBUTE_06 = ?,    \n");
			query.append("       ATTRIBUTE_07 = ?,    \n");
			query.append("       ATTRIBUTE_08 = ?,    \n");
			query.append("       ATTRIBUTE_09 = ?,    \n");
			query.append("       ATTRIBUTE_10 = ?,    \n");
			query.append("       UPDATE_DATE = SYSDATE   \n");
			query.append(" WHERE ASSET_ID = ?    \n");

			log.debug("\n query.toString() : {\n" + query.toString() + "}");
			//
			QueryRunner run = new QueryRunner();
			connection = super.getConnection();
			run.update(connection, 
					query.toString(), 
					new Object[] { 
					asset.getAsset_name(), 
					asset.getDescription(), 
					asset.getTable_type(), 
					asset.getAsset_svg_img(),
					asset.getAttribute_01(),
					asset.getAttribute_02(),
					asset.getAttribute_03(),
					asset.getAttribute_04(),
					asset.getAttribute_05(),
					asset.getAttribute_06(),
					asset.getAttribute_07(),
					asset.getAttribute_08(),
					asset.getAttribute_09(),
					asset.getAttribute_10(),
					asset.getAsset_id() 
					});

			//
			super.getDomainChangeEventBus().onAssetChanged(asset.getAsset_id());
			
		} catch (Exception ex) {
			log.error("AssetDAO.updateAssetWidthEquipment Exception : " + ex.getMessage());
			throw ex;
		} finally {
			super.closeConnection(connection);
		}
	};
	
	
	public void updateAsset(Asset asset) throws Exception {
		//
		Connection connection = null;
		//
		try {
			//
			StringBuffer query = new StringBuffer();
			query.append("UPDATE MM_ASSET_TREE    \n");
			query.append("   SET ASSET_NAME  = ?,    \n");
			query.append("       DESCRIPTION = ?,    \n");
			query.append("       TABLE_TYPE = ?,    \n");
			query.append("       UPDATE_DATE = SYSDATE   \n");
			query.append(" WHERE ASSET_ID = ?    \n");

			log.debug("\n query.toString() : {\n" + query.toString() + "}");
			//
			QueryRunner run = new QueryRunner();
			connection = super.getConnection();
			run.update(connection, query.toString(), new Object[] { 
					asset.getAsset_name(), 
					asset.getDescription(), 
					asset.getTable_type(), 
					asset.getAsset_id() });
			
			//
			super.getDomainChangeEventBus().onAssetChanged(asset.getAsset_id());

		} catch (Exception ex) {
			log.error("AssetDAO.updateAsset Exception : " + ex.getMessage());
			throw ex;
		} finally {
			super.closeConnection(connection);
		}
	};

	public void deleteAsset(Asset asset) throws Exception {
		//
		Connection connection = null;
		//
		try {
			connection = super.getConnection();
			connection.setAutoCommit(false);

			// Asset 삭제
			StringBuffer query = new StringBuffer();
			query.append("DELETE FROM MM_ASSET_TREE    \n");
			query.append(" WHERE ASSET_ID = ?    \n");

			log.debug("\n query.toString() : {\n" + query.toString() + "}");
			//
			QueryRunner run = new QueryRunner();
			run.update(connection, query.toString(), new Object[] { asset.getAsset_id() });

			connection.commit();
			//
			
			//
			super.getDomainChangeEventBus().onAssetChanged(asset.getAsset_id());
			
		} catch (Exception ex) {
			connection.rollback();
			log.error(ex);
			throw ex;
		} finally {
			connection.setAutoCommit(true);
			super.closeConnection(connection);
		}
	}

	public void updateMoveAsset(Asset asset) {

	}

	public void updateMoveAssetOrder(Asset asset) {

	}

}
