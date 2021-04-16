package plantpulse.cep.dao;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.commons.lang.StringUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.springframework.stereotype.Repository;

import plantpulse.cep.db.ConnectionUtils;
import plantpulse.dbutils.QueryRunner;
import plantpulse.dbutils.handlers.BeanHandler;
import plantpulse.dbutils.handlers.BeanListHandler;
import plantpulse.dbutils.handlers.MapListHandler;
import plantpulse.domain.Tag;
import plantpulse.server.mvc.util.Constants;

@Repository
public class TagDAO extends CommonDAO {

	private static final Log log = LogFactory.getLog(TagDAO.class);

	public List<Tag> selectTagAll() throws Exception {

		Connection connection = null;
		List<Tag> list = null;

		try {

			StringBuffer query = new StringBuffer();
			query.append("  SELECT c.site_id,    \n");
			query.append("        a.*            \n");
			query.append("  FROM mm_tags a,      \n");
			query.append("    mm_opcs b,   \n");
			query.append("     mm_sites c    \n");
			query.append("  WHERE a.opc_id = b.OPC_ID    \n");
			query.append("  AND b.site_id = c.site_id    \n");
			query.append("  ORDER BY tag_id    \n");

			QueryRunner run = new QueryRunner();
			connection = super.getConnection();
			list = run.query(connection, query.toString(), new BeanListHandler<Tag>(Tag.class));

		} catch (Exception e) {
			log.error(e);
			throw e;
		} finally {
			super.closeConnection(connection);
		}

		return list;
	}

	public List<Map<String, String>> selectTagAllForSelect() throws Exception {
		//
		List<Map<String, String>> list = new ArrayList<Map<String, String>>();
		//
		ConnectionUtils utils = new ConnectionUtils();
		Connection con = null;
		PreparedStatement pstmt = null;
		ResultSet rs = null;
		try {
			//
			con = utils.getConnection();
			StringBuffer query = new StringBuffer();
			query.append("SELECT A.TAG_ID,          \n");
			query.append("       A.TAG_NAME,     \n");
			query.append("       A.JAVA_TYPE,     \n");
			query.append("       A.UNIT,     \n");
			query.append("       A.DESCRIPTION,     \n");
			query.append("       A.MIN_VALUE,     \n");
			query.append("       A.MAX_VALUE     \n");
			query.append("  FROM MM_TAGS A,   \n");
			query.append("       MM_OPCS B    \n");
			query.append(" WHERE  A.OPC_ID = B.OPC_ID    \n");
			query.append("  ORDER BY A.TAG_NAME     \n");
			//
			pstmt = con.prepareStatement(query.toString());
			rs = pstmt.executeQuery();
			//
			while (rs.next()) {
				Map<String, String> data = new HashMap<String, String>();
				String tag_id = rs.getString(1);
				String tag_name = rs.getString(2);
				String java_type = rs.getString(3);
				String unit = rs.getString(4);
				String description = rs.getString(5);
				
				String min_value = rs.getString("min_value");
				String max_value = rs.getString("max_value");
				
				data.put("id", tag_id);
				data.put("name", tag_name);
				data.put("java_type", java_type);
				data.put("unit", unit);
				data.put("min_value", min_value);
				data.put("max_value", max_value);
				data.put("description", description);
				list.add(data);
			}
		} catch (Exception ex) {
			log.error("[UserDAO.selectTagAllForSelect] " + ex.getMessage(), ex);
		} finally {
			utils.closeConnection(rs, pstmt, con);
		}
		return list;
	}
	
	

	public long selectTagCountTotal() throws Exception {
		//
		long count = 0;
		//
		ConnectionUtils utils = new ConnectionUtils();
		Connection con = null;
		PreparedStatement pstmt = null;
		ResultSet rs = null;
		try {
			//
			con = utils.getConnection();
			StringBuffer query = new StringBuffer();
			query.append("\n");
			query.append("SELECT COUNT(*) AS CNT                            \n");
			query.append("  FROM MM_TAGS                     \n");
			//
			pstmt = con.prepareStatement(query.toString());
			rs = pstmt.executeQuery();
			//
			if (rs.next()) {
				count = rs.getLong(1);
			}
		} catch (Exception ex) {
			log.error("[UserDAO.selectTagCountTotal] " + ex.getMessage(), ex);
		} finally {
			utils.closeConnection(rs, pstmt, con);
		}
		return count;
	}
	
	public long selectTagCountByOpcId(String opc_id) throws Exception {
		//
		long count = 0;
		//
		ConnectionUtils utils = new ConnectionUtils();
		Connection con = null;
		PreparedStatement pstmt = null;
		ResultSet rs = null;
		try {
			//
			con = utils.getConnection();
			StringBuffer query = new StringBuffer();
			query.append("\n");
			query.append("SELECT COUNT(*) AS CNT                            \n");
			query.append("  FROM MM_TAGS                     \n");
			query.append("  WHERE OPC_ID = ?                     \n");
			//
			pstmt = con.prepareStatement(query.toString());
			pstmt.setString(1, opc_id);
			rs = pstmt.executeQuery();
			//
			if (rs.next()) {
				count = rs.getLong(1);
			}
		} catch (Exception ex) {
			log.error("[UserDAO.selectTagCountByOpcId] " + ex.getMessage(), ex);
		} finally {
			utils.closeConnection(rs, pstmt, con);
		}
		return count;
	}
	
	
	public long selectTagCountByLinkedAssetId(String asset_id) throws Exception {
		//
		long count = 0;
		//
		ConnectionUtils utils = new ConnectionUtils();
		Connection con = null;
		PreparedStatement pstmt = null;
		ResultSet rs = null;
		try {
			//
			con = utils.getConnection();
			StringBuffer query = new StringBuffer();
			query.append("\n");
			query.append("SELECT COUNT(*) AS CNT                            \n");
			query.append("  FROM MM_TAGS                     \n");
			query.append("  WHERE LINKED_ASSET_ID = ?                     \n");
			//
			pstmt = con.prepareStatement(query.toString());
			pstmt.setString(1, asset_id);
			rs = pstmt.executeQuery();
			//
			if (rs.next()) {
				count = rs.getLong(1);
			}
		} catch (Exception ex) {
			log.error("[UserDAO.selectTagCountByLinkedAssetId] " + ex.getMessage(), ex);
		} finally {
			utils.closeConnection(rs, pstmt, con);
		}
		return count;
	}

	public List<Tag> selectTagList(Tag tag, String searchType) throws Exception {

		Connection connection = null;
		List<Tag> list = null;

		StringBuffer query = new StringBuffer();
		query.append("SELECT *    \n");
		query.append("  FROM MM_TAGS    \n");
		query.append(" WHERE 1=1    \n");

		String id = "";
		if (searchType.equals(Constants.TYPE_OPC)) {
			query.append("   AND OPC_ID = ?    \n");
			id = tag.getOpc_id();
		} else if (searchType.equals(Constants.TYPE_ASSET)) {
			query.append("   AND LINKED_ASSET_ID = ?    \n");
			id = tag.getLinked_asset_id();
		}
		query.append(" ORDER BY TAG_NAME ASC ");

		log.debug("\n TagDAO.selectTagList query : {\n" + query.toString() + "}");
		log.debug("\n Parameters : [" + tag.toString() + "]");

		try {
			QueryRunner run = new QueryRunner();
			connection = super.getConnection();
			list = run.query(connection, query.toString(), new BeanListHandler<Tag>(Tag.class), new Object[] { id });

			log.debug("\n list : [" + list.toString() + "]");

		} catch (Exception e) {
			log.error(e);
			throw e;
		} finally {
			super.closeConnection(connection);
		}
		return list;
	};
	
	
	public List<Tag> selectTagListByAssetId(String asset_id) throws Exception {
		Connection connection = null;
		List<Tag> list = null;
		try {
		StringBuffer query = new StringBuffer();
		query.append("SELECT *    \n");
		query.append("  FROM MM_TAGS    \n");
		query.append(" WHERE LINKED_ASSET_ID = ?    \n");
		query.append(" ORDER BY TAG_NAME ASC ");

			QueryRunner run = new QueryRunner();
			connection = super.getConnection();
			list = run.query(connection, query.toString(), new BeanListHandler<Tag>(Tag.class), new Object[] { asset_id });

			log.debug("\n list : [" + list.toString() + "]");

		} catch (Exception e) {
			log.error(e);
			throw e;
		} finally {
			super.closeConnection(connection);
		}
		return list;
	};
	
	public List<Tag> selectTagListByAssetId(String asset_id, int limit) throws Exception {
		Connection connection = null;
		List<Tag> list = null;
		try {
		StringBuffer query = new StringBuffer();
		query.append("SELECT *    \n");
		query.append("  FROM MM_TAGS    \n");
		query.append(" WHERE LINKED_ASSET_ID = ?    \n");
		query.append(" ORDER BY TAG_NAME ASC ");
		query.append(" LIMIT " + limit);

			QueryRunner run = new QueryRunner();
			connection = super.getConnection();
			list = run.query(connection, query.toString(), new BeanListHandler<Tag>(Tag.class), new Object[] { asset_id });

			log.debug("\n list : [" + list.toString() + "]");

		} catch (Exception e) {
			log.error(e);
			throw e;
		} finally {
			super.closeConnection(connection);
		}
		return list;
	};
	
	
	public List<Tag> selectTagListByOpcId(String opc_id) throws Exception {
		Connection connection = null;
		List<Tag> list = null;
		try {
		StringBuffer query = new StringBuffer();
		query.append("SELECT *    \n");
		query.append("  FROM MM_TAGS    \n");
		query.append(" WHERE OPC_ID = ?    \n");
		query.append(" ORDER BY TAG_NAME ASC ");

		QueryRunner run = new QueryRunner();
		connection = super.getConnection();
		list = run.query(connection, query.toString(), new BeanListHandler<Tag>(Tag.class), new Object[] { opc_id });

		log.debug("\n list : [" + list.toString() + "]");

		} catch (Exception e) {
			log.error(e);
			throw e;
		} finally {
			super.closeConnection(connection);
		}
		return list;
	};

	public List<Tag> selectTagList(Tag tag, String searchType, int offset) throws Exception {

		Connection connection = null;
		List<Tag> list = null;

		StringBuffer query = new StringBuffer();
		query.append("SELECT *    \n");
		query.append("  FROM MM_TAGS    \n");
		query.append(" WHERE 1=1    \n");

		String id = "";
		if (searchType.equals(Constants.TYPE_OPC)) {
			query.append("   AND OPC_ID = ?    \n");
			id = tag.getOpc_id();
		} else if (searchType.equals(Constants.TYPE_ASSET)) {
			query.append("   AND LINKED_ASSET_ID = ?    \n");
			id = tag.getLinked_asset_id();
		}
		query.append(" ORDER BY TAG_NAME ASC ");

		log.debug("\n TagDAO.selectTagList query : {\n" + query.toString() + "}");
		log.debug("\n Parameters : [" + tag.toString() + "]");

		try {
			QueryRunner run = new QueryRunner();
			connection = super.getConnection();
			list = run.query(connection, query.toString(), new BeanListHandler<Tag>(Tag.class), new Object[] { id });

			log.debug("\n list : [" + list.toString() + "]");

		} catch (Exception e) {
			log.error(e);
			throw e;
		} finally {
			super.closeConnection(connection);
		}
		return list;
	}

	public Tag selectTag(String tag_id) throws Exception {

		Connection connection = null;
		Tag tag = null;

		try {

			StringBuffer query = new StringBuffer();
			query.append("SELECT A.*,          \n");
			query.append("       B.SITE_ID     \n");
			query.append("  FROM MM_TAGS A,   \n");
			query.append("       MM_OPCS B    \n");
			query.append(" WHERE A.TAG_ID = ?    \n");
			query.append("   AND A.OPC_ID = B.OPC_ID    \n");

			QueryRunner run = new QueryRunner();
			connection = super.getConnection();
			tag = run.query(connection, query.toString(), new BeanHandler<Tag>(Tag.class), new Object[] { tag_id });

		} catch (Exception e) {
			log.error(e);
			throw e;
		} finally {
			super.closeConnection(connection);
		}

		return tag;
	}

	public Tag selectTagInfoWithSiteAndOpc(String tag_id) throws Exception {

		Connection connection = null;
		Tag tag = null;

		try {

			StringBuffer query = new StringBuffer();
			query.append("SELECT A.*,          \n");
			query.append("       B.OPC_ID,     \n");
			query.append("       B.OPC_NAME,     \n");
			query.append("       C.SITE_ID,     \n");
			query.append("       C.SITE_NAME     \n");
			query.append("  FROM MM_TAGS  A,   \n");
			query.append("       MM_OPCS  B,    \n");
			query.append("       MM_SITES C    \n");
			query.append(" WHERE A.TAG_ID = ?    \n");
			query.append("   AND A.OPC_ID = B.OPC_ID    \n");
			query.append("   AND B.SITE_ID = C.SITE_ID    \n");

			QueryRunner run = new QueryRunner();
			connection = super.getConnection();
			tag = run.query(connection, query.toString(), new BeanHandler<Tag>(Tag.class), new Object[] { tag_id });

		} catch (Exception e) {
			log.error(e);
			throw e;
		} finally {
			super.closeConnection(connection);
		}

		return tag;
	}

	public List<Tag> selectTagsByLinkAssetId(String link_id) throws Exception {

		Connection connection = null;
		List<Tag> list = null;

		try {

			StringBuffer query = new StringBuffer();
			query.append("SELECT *    \n");
			query.append("  FROM MM_TAGS    \n");
			query.append(" WHERE LINKED_ASSET_ID = ?    \n");

			QueryRunner run = new QueryRunner();
			connection = super.getConnection();
			list = run.query(connection, query.toString(), new BeanListHandler<Tag>(Tag.class), new Object[] { link_id });

		} catch (Exception e) {
			log.error(e);
			throw e;
		} finally {
			super.closeConnection(connection);
		}

		return list;
	}
	
	
	public List<Tag> selectTagsBySiteId(String site_id) throws Exception {

		Connection connection = null;
		List<Tag> list = null;

		try {

			StringBuffer query = new StringBuffer();
			query.append("SELECT A.*,          \n");
			query.append("       B.OPC_ID,     \n");
			query.append("       B.OPC_NAME,     \n");
			query.append("       C.SITE_ID,     \n");
			query.append("       C.SITE_NAME     \n");
			query.append("  FROM MM_TAGS  A,   \n");
			query.append("       MM_OPCS  B,    \n");
			query.append("       MM_SITES C    \n");
			query.append(" WHERE A.OPC_ID  = B.OPC_ID    \n");
			query.append("   AND B.SITE_ID = C.SITE_ID    \n");
			query.append("   AND C.SITE_ID = ?    \n");

			QueryRunner run = new QueryRunner();
			connection = super.getConnection();
			list = run.query(connection, query.toString(), new BeanListHandler<Tag>(Tag.class), new Object[] { site_id });

		} catch (Exception e) {
			log.error(e);
			throw e;
		} finally {
			super.closeConnection(connection);
		}

		return list;
	}

	public int selectTagsCountBySiteId(String site_id) {
		//
		ConnectionUtils utils = new ConnectionUtils();
		Connection con = null;
		PreparedStatement pstmt = null;
		ResultSet rs = null;

		//
		int count = 0;
		try {
			//
			con = utils.getConnection();
			StringBuffer query = new StringBuffer();
			query.append("\n");
			query.append("SELECT count(*) AS TAG_COUNT                              \n");
			query.append("  FROM MM_TAGS  A,                   \n");
			query.append("       MM_OPCS  B                   \n");
			query.append(" WHERE A.OPC_ID = B.OPC_ID         \n");
			query.append("   AND B.SITE_ID = ?        \n");
			query.append(";\n");
			//
			pstmt = con.prepareStatement(query.toString());
			pstmt.setString(1, site_id);
			rs = pstmt.executeQuery();
			log.debug("[selectTagsCountBySiteId query] : " + query.toString());
			//
			if (rs.next()) {
				count = rs.getInt(1);
			}
		} catch (Exception ex) {
			log.error("[selectTagsCountBySiteId] " + ex.getMessage(), ex);
		} finally {
			utils.closeConnection(rs, pstmt, con);
		}
		return count;
	};
	
	public List<Map<String, Object>> selectTagConfigList() throws Exception {

		Connection connection = null;
		List<Map<String, Object>> list = null;

		try {

			StringBuffer query = new StringBuffer();
			query.append("SELECT *    \n");
			query.append("  FROM MM_COMPANY C,    \n");
			query.append("       MM_SITES S,    \n");
			query.append("       MM_OPCS O,    \n");
			query.append("       MM_TAGS T    \n");
			query.append(" WHERE C.COMPANY_ID = S.COMPANY_ID    \n");
			query.append("   AND S.SITE_ID = O.SITE_ID    \n");
			query.append("   AND O.OPC_ID = T.OPC_ID    \n");
			query.append(" ORDER BY C.COMPANY_NAME, S.SITE_NAME, O.OPC_NAME, T.TAG_NAME    \n");

			log.debug("Query : {\n" + query.toString() + "}");

			QueryRunner run = new QueryRunner();
			connection = super.getConnection();
			list = run.query(connection, query.toString(), new MapListHandler());

		} catch (Exception e) {
			log.error(e);
			throw e;
		} finally {
			super.closeConnection(connection);
		}

		return list;
	}

	public List<Map<String, Object>> searchTagConfigList(Tag tag, int limit) throws Exception {

		List<Map<String, Object>> list = null;
		Object[] obj = null;
		Connection connection = null;

		try {

			List<Object> parmaList = new ArrayList<Object>();

			StringBuffer query = new StringBuffer();
			query.append("SELECT *    \n");
			query.append("  FROM MM_COMPANY C,    \n");
			query.append("       MM_SITES S,    \n");
			query.append("       MM_OPCS O,    \n");
			query.append("       MM_TAGS T    \n");
			query.append(" WHERE C.COMPANY_ID = S.COMPANY_ID    \n");
			query.append("   AND S.SITE_ID = O.SITE_ID    \n");
			query.append("   AND O.OPC_ID = T.OPC_ID    \n");

			if (StringUtils.isNotEmpty(tag.getSite_name())) {
				query.append("   AND UPPER(S.SITE_NAME) LIKE '%' ||  UPPER(?) || '%'    \n");
				parmaList.add(tag.getSite_name());
			}
			if (StringUtils.isNotEmpty(tag.getOpc_name())) {
				query.append("   AND UPPER(O.OPC_NAME) LIKE '%' ||  UPPER(?) || '%'    \n");
				parmaList.add(tag.getOpc_name());
			}
			if (StringUtils.isNotEmpty(tag.getTag_id())) {
				query.append("   AND UPPER(T.TAG_ID) LIKE '%' ||  UPPER(?) || '%'    \n");
				parmaList.add(tag.getTag_id());
			}
			if (StringUtils.isNotEmpty(tag.getTag_name())) {
				query.append("   AND UPPER(T.TAG_NAME) LIKE '%' ||  UPPER(?) || '%'    \n");
				parmaList.add(tag.getTag_name());
			}
			if (StringUtils.isNotEmpty(tag.getJava_type())) {
				query.append("   AND UPPER(T.JAVA_TYPE) LIKE '%' || UPPER(?) || '%'    \n");
				parmaList.add(tag.getJava_type());
			}
			if (StringUtils.isNotEmpty(tag.getLinked_asset_id())) {
				query.append("   AND UPPER(T.LINKED_ASSET_ID) LIKE '%' || UPPER(?) || '%'    \n");
				parmaList.add(tag.getLinked_asset_id());
			}
			if (StringUtils.isNotEmpty(tag.getDescription())) {
				query.append("   AND T.DESCRIPTION LIKE '%' || ? || '%'    \n");
				parmaList.add(tag.getDescription());
			}
			;

			query.append(" ORDER BY C.COMPANY_NAME ASC, S.SITE_NAME ASC, O.OPC_NAME ASC, T.TAG_NAME ASC   \n");
			
			
			if (limit > 0) { query.append(" LIMIT " + limit + ""); } 
			
			log.debug("Query : " + query.toString() + "");

			if (parmaList.size() > 0) {
				obj = new Object[parmaList.size()];
				for (int i = 0; i < parmaList.size(); i++) {
					String str = (String) parmaList.get(i);
					obj[i] = str;
				}
			}

			QueryRunner run = new QueryRunner();
			connection = super.getConnection();
			list = run.query(connection, query.toString(), new MapListHandler(), obj);

		} catch (Exception e) {
			log.error(e);
			throw e;
		} finally {
			super.closeConnection(connection);
		}

		return list;
	}
	
	
	public List<Map<String, Object>> searchTagConfigListPermissioned(Tag tag, int limit, String user_id) throws Exception {
        //
		List<Map<String, Object>> list = null;
		Object[] obj = null;
		Connection connection = null;
        //
		try {

			List<Object> parmaList = new ArrayList<Object>();
			
			/*
			 -- 퍼미션 --
			SELECT *
			  FROM MM_COMPANY C,
			       MM_SITES S,
			       MM_OPCS O,
			       MM_TAGS T,
			       (SELECT p.object_permission_array
								   FROM user_login u,
								        mm_security p
								   WHERE u.user_id = 'manager'
								   AND u.security_id = p.security_id ) P
			 WHERE C.COMPANY_ID = S.COMPANY_ID
			   AND S.SITE_ID = O.SITE_ID
			   AND O.OPC_ID = T.OPC_ID
			   AND P.object_permission_array LIKE '%' || t.tag_id || '%' 
			 ORDER BY C.COMPANY_NAME, S.SITE_NAME, O.OPC_NAME, T.TAG_NAME
			 LIMIT 100;
			*/

			StringBuffer query = new StringBuffer();
			query.append("SELECT *    \n");
			query.append("  FROM MM_COMPANY C,    \n");
			query.append("       MM_SITES S,    \n");
			query.append("       MM_OPCS O,    \n");
			query.append("       MM_TAGS T    \n");
			query.append(" WHERE C.COMPANY_ID = S.COMPANY_ID    \n");
			query.append("   AND S.SITE_ID = O.SITE_ID    \n");
			query.append("   AND O.OPC_ID = T.OPC_ID    \n");

			if (StringUtils.isNotEmpty(tag.getSite_name())) {
				query.append("   AND UPPER(S.SITE_NAME) LIKE '%' ||  UPPER(?) || '%'    \n");
				parmaList.add(tag.getSite_name());
			}
			if (StringUtils.isNotEmpty(tag.getOpc_name())) {
				query.append("   AND UPPER(O.OPC_NAME) LIKE '%' ||  UPPER(?) || '%'    \n");
				parmaList.add(tag.getOpc_name());
			}
			if (StringUtils.isNotEmpty(tag.getTag_id())) {
				query.append("   AND UPPER(T.TAG_ID) LIKE '%' ||  UPPER(?) || '%'    \n");
				parmaList.add(tag.getTag_id());
			}
			if (StringUtils.isNotEmpty(tag.getTag_name())) {
				query.append("   AND UPPER(T.TAG_NAME) LIKE '%' ||  UPPER(?) || '%'    \n");
				parmaList.add(tag.getTag_name());
			}
			if (StringUtils.isNotEmpty(tag.getJava_type())) {
				query.append("   AND UPPER(T.JAVA_TYPE) LIKE '%' || UPPER(?) || '%'    \n");
				parmaList.add(tag.getJava_type());
			}
			if (StringUtils.isNotEmpty(tag.getLinked_asset_id())) {
				query.append("   AND UPPER(T.LINKED_ASSET_ID) LIKE '%' || UPPER(?) || '%'    \n");
				parmaList.add(tag.getLinked_asset_id());
			}
			if (StringUtils.isNotEmpty(tag.getDescription())) {
				query.append("   AND T.DESCRIPTION LIKE '%' || ? || '%'    \n");
				parmaList.add(tag.getDescription());
			}
			;

			query.append(" ORDER BY C.COMPANY_NAME ASC, S.SITE_NAME ASC, O.OPC_NAME ASC, T.TAG_NAME ASC    \n");
			
			
			if (limit > 0) { query.append(" LIMIT " + limit + ""); } 
			
			log.debug("Query : " + query.toString() + "");

			if (parmaList.size() > 0) {
				obj = new Object[parmaList.size()];
				for (int i = 0; i < parmaList.size(); i++) {
					String str = (String) parmaList.get(i);
					obj[i] = str;
				}
			}

			QueryRunner run = new QueryRunner();
			connection = super.getConnection();
			list = run.query(connection, query.toString(), new MapListHandler(), obj);

		} catch (Exception e) {
			log.error(e);
			throw e;
		} finally {
			super.closeConnection(connection);
		}

		return list;
	}

	public List<Map<String, Object>> searchTagConfigList(Tag tag) throws Exception {

		List<Map<String, Object>> list = null;
		Object[] obj = null;
		Connection connection = null;

		try {

			List<Object> parmaList = new ArrayList<Object>();

			StringBuffer query = new StringBuffer();
			query.append("SELECT *    \n");
			query.append("  FROM MM_COMPANY C,    \n");
			query.append("       MM_SITES S,    \n");
			query.append("       MM_OPCS O,    \n");
			query.append("       MM_TAGS T    \n");
			query.append(" WHERE C.COMPANY_ID = S.COMPANY_ID    \n");
			query.append("   AND S.SITE_ID = O.SITE_ID    \n");
			query.append("   AND O.OPC_ID = T.OPC_ID    \n");

			if (StringUtils.isNotEmpty(tag.getSite_name())) {
				query.append("   AND UPPER(S.SITE_NAME) LIKE '%' ||  UPPER(?) || '%'    \n");
				parmaList.add(tag.getSite_name());
			}
			if (StringUtils.isNotEmpty(tag.getOpc_name())) {
				query.append("   AND UPPER(O.OPC_NAME) LIKE '%' ||  UPPER(?) || '%'    \n");
				parmaList.add(tag.getOpc_name());
			}
			if (StringUtils.isNotEmpty(tag.getTag_id())) {
				query.append("   AND UPPER(T.TAG_ID) LIKE '%' ||  UPPER(?) || '%'    \n");
				parmaList.add(tag.getTag_id());
			}
			if (StringUtils.isNotEmpty(tag.getTag_name())) {
				query.append("   AND UPPER(T.TAG_NAME) LIKE '%' ||  UPPER(?) || '%'    \n");
				parmaList.add(tag.getTag_name());
			}
			if (StringUtils.isNotEmpty(tag.getJava_type())) {
				query.append("   AND UPPER(T.JAVA_TYPE) LIKE '%' || UPPER(?) || '%'    \n");
				parmaList.add(tag.getJava_type());
			}
			if (StringUtils.isNotEmpty(tag.getDescription())) {
				query.append("   AND T.DESCRIPTION LIKE '%' || ? || '%'    \n");
				parmaList.add(tag.getDescription());
			}
			;
			query.append(" ORDER BY C.COMPANY_NAME ASC, S.SITE_NAME ASC, O.OPC_NAME ASC, T.TAG_NAME ASC   \n");

			log.debug("Query : {\n" + query.toString() + "}");

			if (parmaList.size() > 0) {
				obj = new Object[parmaList.size()];
				for (int i = 0; i < parmaList.size(); i++) {
					String str = (String) parmaList.get(i);
					obj[i] = str;
				}
			}

			QueryRunner run = new QueryRunner();
			connection = super.getConnection();
			list = run.query(connection, query.toString(), new MapListHandler(), obj);

		} catch (Exception e) {
			log.error(e);
			throw e;
		} finally {
			super.closeConnection(connection);
		}

		return list;
	}


	public void insertTag(Tag tag) throws Exception {
		//
		Connection connection = null;
		//
		try {
			//
			StringBuffer query = new StringBuffer();
			query.append("INSERT INTO MM_TAGS    \n");
			query.append("       (   \n");
			query.append("       OPC_ID  ,    \n");
			query.append("       TAG_ID  ,    \n");
			query.append("       TAG_NAME ,    \n");
			query.append("       DESCRIPTION ,    \n");
			query.append("       JAVA_TYPE ,    \n");
			query.append("       TRIP_HI ,    \n");
			query.append("       HI_HI ,    \n");
			query.append("       HI ,    \n");
			query.append("       LO ,    \n");
			query.append("       LO_LO ,    \n");
			query.append("       TRIP_LO ,    \n");
			query.append("       IMPORTANCE ,    \n");
			query.append("       DISPLAY_FORMAT ,    \n");
			query.append("       LAT  ,    \n");
			query.append("       LNG  ,    \n");
			query.append("       MIN_VALUE  ,    \n");
			query.append("       MAX_VALUE  ,    \n");
			query.append("       INTERVAL  ,    \n");
			query.append("       ALIAS_NAME  ,    \n");
			query.append("       LINKED_ASSET_ID,  \n");
			query.append("       UNIT  ,    \n");
			query.append("       NOTE  ,    \n");
			query.append("       RECIEVE_ME  ,    \n");
			query.append("       RECIEVE_OTHERS  ,    \n");
			query.append("       USER_DEFINED_ALARM_CLASS  ,    \n");
			//
			query.append("       USE_CALCULATION  ,    \n");
			query.append("       CALCULATION_EQL  ,    \n");
			//
			query.append("       USE_ML_FORECAST  ,    \n");
			query.append("       ML_FORECAST_LEARNING_TAG_ID  ,    \n");
			query.append("       ML_FORECAST_LEARNING_BEFORE_MINUTES  ,    \n");
			query.append("       ML_FORECAST_TERM  ,    \n");
			query.append("       ML_FORECAST_PREDICT_COUNT  ,    \n");
			query.append("       ML_FORECAST_TIMESTAMP_FIT  ,    \n");
			//
			query.append("       USE_AGGREGATION  ,    \n");
			query.append("       AGGREGATION_1_MINUTES  ,    \n");
			query.append("       AGGREGATION_5_MINUTES  ,    \n");
			query.append("       AGGREGATION_10_MINUTES  ,    \n");
			query.append("       AGGREGATION_30_MINUTES  ,    \n");
			query.append("       AGGREGATION_1_HOURS ,    \n");
			
			query.append("       PUBLISH_MQTT  ,    \n");
			query.append("       PUBLISH_STOMP  ,    \n");
			query.append("       PUBLISH_KAFKA ,    \n");
			
			
			query.append("       BOOL_TRUE  ,    \n");
			query.append("       BOOL_TRUE_PRIORITY  ,  \n");
			query.append("       BOOL_TRUE_MESSAGE ,    \n");
			query.append("       BOOL_FALSE  ,    \n");
			query.append("       BOOL_FALSE_PRIORITY  ,  \n");
			query.append("       BOOL_FALSE_MESSAGE ,    \n");
			
			query.append("       INSERT_DATE  \n");  
			query.append("       ) VALUES (  \n");  
			query.append("       ?,    \n");
			query.append("       ?,    \n");
			query.append("       ?,    \n");
			query.append("       ?,    \n");
			query.append("       ?,    \n");
			query.append("       ?,    \n");
			query.append("       ?,    \n");
			query.append("       ?,    \n");
			query.append("       ?,    \n");
			query.append("       ?,    \n");
			query.append("       ?,    \n");
			query.append("       ?,    \n");
			query.append("       ?,    \n");
			query.append("       ?,    \n");
			query.append("       ?,    \n");
			query.append("       ?,    \n");
			query.append("       ?,    \n");
			query.append("       ?,    \n");
			query.append("       ?,    \n");
			query.append("       ?,    \n");
			query.append("       ?,    \n");
			query.append("       ?,    \n");
			query.append("       ?,    \n");
			query.append("       ?,    \n");
			query.append("       ?,    \n");
			//
			query.append("       ?,    \n");
			query.append("       ?,    \n");
			//
			query.append("       ?,    \n");
			query.append("       ?,    \n");
			query.append("       ?,    \n");
			query.append("       ?,    \n");
			query.append("       ?,    \n");
			query.append("       ?,    \n");
			//
			query.append("       ?,    \n");
			query.append("       ?,    \n");
			query.append("       ?,    \n");
			query.append("       ?,    \n");
			query.append("       ?,    \n");
			query.append("       ?,    \n");
			
			query.append("       ?,    \n");
			query.append("       ?,    \n");
			query.append("       ?,    \n");
			
			query.append("       ?,    \n");
			query.append("       ?,    \n");
			query.append("       ?,    \n");
			query.append("       ?,    \n");
			query.append("       ?,    \n");
			query.append("       ?,    \n");
		
			query.append("       SYSDATE  \n");  
			query.append("       )  \n");  

			log.debug("\n query.toString() : {\n" + query.toString() + "}");
			//
			QueryRunner run = new QueryRunner();
			connection = super.getConnection();
			run.update(connection, query.toString(),
					new Object[] { 
							tag.getOpc_id(),
							
							tag.getTag_id(),
							tag.getTag_name(), 
							tag.getDescription(), 
							tag.getJava_type(), 
							
							StringUtils.isEmpty(tag.getTrip_hi()) ? null : tag.getTrip_hi(), 
							StringUtils.isEmpty(tag.getHi_hi()) ? null : tag.getHi_hi(),
							StringUtils.isEmpty(tag.getHi()) ? null : tag.getHi(), 
							StringUtils.isEmpty(tag.getLo()) ? null : tag.getLo(), 
							StringUtils.isEmpty(tag.getLo_lo()) ? null : tag.getLo_lo(),
							StringUtils.isEmpty(tag.getTrip_lo()) ? null : tag.getTrip_lo(), 
									
							tag.getImportance(), 
							tag.getDisplay_format(), 
							tag.getLat(), 
							tag.getLng(), 
							tag.getMin_value(),
							tag.getMax_value(), 
							StringUtils.isEmpty(tag.getInterval()) ? "1000" : tag.getInterval(), 
							tag.getAlias_name(), 
							tag.getLinked_asset_id(),
							tag.getUnit(), 
							tag.getNote(), 
							tag.isRecieve_me(),
							tag.getRecieve_others(),
							tag.getUser_defined_alarm_class(),
							tag.getUse_calculation(),
							tag.getCalculation_eql(),
							
							tag.getUse_ml_forecast(),
							tag.getMl_forecast_learning_tag_id(),
							tag.getMl_forecast_learning_before_minutes(),
							tag.getMl_forecast_term(),
							tag.getMl_forecast_predict_count(),
							tag.getMl_forecast_timestamp_fit(),
							
							tag.getUse_aggregation(),
							tag.getAggregation_1_minutes(),
							tag.getAggregation_5_minutes(),
							tag.getAggregation_10_minutes(),
							tag.getAggregation_30_minutes(),
							tag.getAggregation_1_hours(),
							
							tag.getPublish_mqtt(),
							tag.getPublish_stomp(),
							tag.getPublish_kafka(),
							
							tag.getBool_true(),
							tag.getBool_true_priority(),
							tag.getBool_true_message(),
							tag.getBool_false(),
							tag.getBool_false_priority(),
							tag.getBool_false_message()
							
							}
			);

			
			super.getDomainChangeEventBus().onTagChanged(tag.getTag_id());
			
		} catch (Exception ex) {
			log.error("\n TagDAO.insertTag Exception : " + ex.getMessage(), ex);
			throw ex;
		} finally {
			super.closeConnection(connection);
		}
	}
	
	public void updateTag(Tag tag) throws Exception {
		//
		Connection connection = null;
		//
		try {
			//
			StringBuffer query = new StringBuffer();
			query.append("UPDATE MM_TAGS    \n");
			query.append("   SET   \n");
			query.append("       TAG_NAME = ?,    \n");
			query.append("       DESCRIPTION = ?,    \n");
			query.append("       JAVA_TYPE = ?,    \n");
			query.append("       TRIP_HI = ?,    \n");
			query.append("       HI_HI = ?,    \n");
			query.append("       HI = ?,    \n");
			query.append("       LO = ?,    \n");
			query.append("       LO_LO = ?,    \n");
			query.append("       TRIP_LO = ?,    \n");
			query.append("       IMPORTANCE = ?,    \n");
			query.append("       DISPLAY_FORMAT = ?,    \n");
			query.append("       LAT = ?,    \n");
			query.append("       LNG = ?,    \n");
			query.append("       MIN_VALUE = ?,    \n");
			query.append("       MAX_VALUE = ?,    \n");
			query.append("       INTERVAL = ?,    \n");
			query.append("       ALIAS_NAME = ?,    \n");
			query.append("       LINKED_ASSET_ID = ?,    \n");
			query.append("       UNIT = ?,    \n");
			query.append("       NOTE = ?,    \n");
			query.append("       RECIEVE_ME = ?,    \n");
			query.append("       RECIEVE_OTHERS = ?,    \n");
			query.append("       USER_DEFINED_ALARM_CLASS = ?,    \n");
			//
			query.append("       USE_CALCULATION = ?,    \n");
			query.append("       CALCULATION_EQL = ?,    \n");
			//
			query.append("       USE_ML_FORECAST = ?,    \n");
			query.append("       ML_FORECAST_LEARNING_TAG_ID = ?,    \n");
			query.append("       ML_FORECAST_LEARNING_BEFORE_MINUTES = ?,    \n");
			query.append("       ML_FORECAST_TERM = ?,    \n");
			query.append("       ML_FORECAST_PREDICT_COUNT = ?,    \n");
			query.append("       ML_FORECAST_TIMESTAMP_FIT = ?,    \n");
			
			//
			query.append("       USE_AGGREGATION = ?,    \n");
			query.append("       AGGREGATION_1_MINUTES = ?,    \n");
			query.append("       AGGREGATION_5_MINUTES = ?,    \n");
			query.append("       AGGREGATION_10_MINUTES = ?,    \n");
			query.append("       AGGREGATION_30_MINUTES = ?,    \n");
			query.append("       AGGREGATION_1_HOURS = ?,    \n");
			
			query.append("       PUBLISH_MQTT  = ? ,    \n");
			query.append("       PUBLISH_STOMP = ?  ,    \n");
			query.append("       PUBLISH_KAFKA = ? ,    \n");
			
			query.append("       BOOL_TRUE = ?  ,    \n");
			query.append("       BOOL_TRUE_PRIORITY = ?  ,  \n");
			query.append("       BOOL_TRUE_MESSAGE = ? ,    \n");
			query.append("       BOOL_FALSE = ?  ,    \n");
			query.append("       BOOL_FALSE_PRIORITY = ?  ,  \n");
			query.append("       BOOL_FALSE_MESSAGE = ? ,    \n");
			
			
			query.append("       UPDATE_DATE = SYSDATE  \n");  
			query.append(" WHERE TAG_ID = ?    \n");

			log.debug("\n query.toString() : {\n" + query.toString() + "}");
			//
			QueryRunner run = new QueryRunner();
			connection = super.getConnection();
			run.update(connection, query.toString(),
					new Object[] { 

							tag.getTag_name(), 
							tag.getDescription(), 
							tag.getJava_type(), 
							
							StringUtils.isEmpty(tag.getTrip_hi()) ? null : tag.getTrip_hi(), 
							StringUtils.isEmpty(tag.getHi_hi()) ? null : tag.getHi_hi(),
							StringUtils.isEmpty(tag.getHi()) ? null : tag.getHi(), 
							StringUtils.isEmpty(tag.getLo()) ? null : tag.getLo(), 
							StringUtils.isEmpty(tag.getLo_lo()) ? null : tag.getLo_lo(),
							StringUtils.isEmpty(tag.getTrip_lo()) ? null : tag.getTrip_lo(), 
									
							tag.getImportance(), 
							tag.getDisplay_format(), 
							tag.getLat(), 
							tag.getLng(), 
							tag.getMin_value(),
							tag.getMax_value(), 
							StringUtils.isEmpty(tag.getInterval()) ? "1000" : tag.getInterval(), 
							tag.getAlias_name(), 
							tag.getLinked_asset_id(),
							tag.getUnit(), 
							tag.getNote(), 
							tag.isRecieve_me(),
							tag.getRecieve_others(),
							tag.getUser_defined_alarm_class(),
							tag.getUse_calculation(),
							tag.getCalculation_eql(),
							
							tag.getUse_ml_forecast(),
							tag.getMl_forecast_learning_tag_id(),
							tag.getMl_forecast_learning_before_minutes(),
							tag.getMl_forecast_term(),
							tag.getMl_forecast_predict_count(),
							tag.getMl_forecast_timestamp_fit(),
							
							tag.getUse_aggregation(),
							tag.getAggregation_1_minutes(),
							tag.getAggregation_5_minutes(),
							tag.getAggregation_10_minutes(),
							tag.getAggregation_30_minutes(),
							tag.getAggregation_1_hours(),
							
							tag.getPublish_mqtt(),
							tag.getPublish_stomp(),
							tag.getPublish_kafka(),
							
							tag.getBool_true(),
							tag.getBool_true_priority(),
							tag.getBool_true_message(),
							tag.getBool_false(),
							tag.getBool_false_priority(),
							tag.getBool_false_message(),
							
							tag.getTag_id() });
			
			super.getDomainChangeEventBus().onTagChanged(tag.getTag_id());

		} catch (Exception ex) {
			log.error("\n TagDAO.updateTag Exception : " + ex.getMessage(), ex);
			throw ex;
		} finally {
			super.closeConnection(connection);
		}
	}

	
	public void deleteTag(Tag tag) throws Exception {
		//
		Connection connection = null;
		//
		try {
			// 태그 ID
			String tag_id = tag.getTag_id();
			StringBuffer query = new StringBuffer();
			query.append("DELETE FROM MM_TAGS    \n");
			query.append(" WHERE TAG_ID = ?    \n");
			
			log.debug("\n query.toString() : {\n" + query.toString() + "}");
			log.debug("\n Key : [" +tag_id + "]");
			//
			QueryRunner run = new QueryRunner();
			connection = super.getConnection();
			run.update(connection, query.toString(), new Object[] { tag_id });

			super.getDomainChangeEventBus().onTagChanged(tag_id);
			
			//
		} catch (Exception ex) {
			log.error(ex);
			throw ex;
		} finally {
			super.closeConnection(connection);
		}
	};
	
	
	public String insertVituralTag(Tag tag) throws Exception {
		//
		String opc_id = "";
		Connection connection = null;

		//
		try {
			connection = super.getConnection();
			connection.setAutoCommit(false);

			//
			StringBuffer query = new StringBuffer();
			query.append("INSERT INTO MM_TAGS    \n");
			query.append("       ( \n");
			query.append("         TAG_ID,    \n");
			query.append("         TAG_NAME,    \n");
			query.append("         TAG_SOURCE,    \n");
			query.append("         OPC_ID,    \n");
			query.append("         INSERT_DATE,    \n");
			query.append("         JAVA_TYPE,    \n");
			query.append("         ALIAS_NAME,    \n");
			query.append("         UNIT,    \n");
			query.append("         DESCRIPTION,    \n");
			query.append("         LINKED_ASSET_ID,    \n");
			query.append("         INTERVAL    \n");
			query.append("       )    \n");
			query.append("VALUES    \n");
			query.append("       (    \n");
			query.append("         ?,    \n");
			query.append("         ?,    \n");
			query.append("         'OPC',    \n");
			query.append("         ?,    \n");
			query.append("         SYSDATE,    \n");
			query.append("         ?,    \n");
			query.append("         ?,    \n");
			query.append("         ?,    \n");
			query.append("         ?,    \n");
			query.append("         ?,    \n");
			query.append("         1000    \n");
			query.append("       )    \n");

			log.debug("\n query.toString() : {\n" + query.toString() + "}");
			log.debug("\n Parameters : [" + tag.toString() + "]");

			QueryRunner run = new QueryRunner();
			run.update(connection, query.toString(),
					new Object[] { 
							tag.getTag_id(), 
							tag.getTag_name(), 
							tag.getOpc_id(), 
							tag.getJava_type(),
							(StringUtils.isNotEmpty(tag.getAlias_name())) ? tag.getAlias_name() : "", 
							tag.getUnit(),
							(StringUtils.isNotEmpty(tag.getDescription())) ? tag.getDescription() : "" ,
							(StringUtils.isNotEmpty(tag.getLinked_asset_id())) ? tag.getLinked_asset_id() : "" 
					}
			);

			connection.commit();
			//
			
			super.getDomainChangeEventBus().onTagChanged(tag.getTag_id());
			
		} catch (Exception ex) {
			connection.rollback();
			log.error(ex.getMessage(), ex);
			throw ex;
		} finally {
			connection.setAutoCommit(true);
			super.closeConnection(connection);
		}

		return opc_id;
	}

	public String deleteVituralTag(Tag tag) throws Exception {
		//
		String opc_id = "";
		Connection connection = null;

		//
		try {
			connection = super.getConnection();
			connection.setAutoCommit(false);

			//
			StringBuffer query = new StringBuffer();
			query.append("DELETE FROM MM_TAGS WHERE TAG_ID =  ?  \n");

			log.debug("\n query.toString() : {\n" + query.toString() + "}");
			log.debug("\n Parameters : [" + tag.toString() + "]");

			QueryRunner run = new QueryRunner();
			run.update(connection, query.toString(), new Object[] { tag.getTag_id() });

			connection.commit();
			//
			
			super.getDomainChangeEventBus().onTagChanged(tag.getTag_id());
			
		} catch (Exception ex) {
			connection.rollback();
			log.error(ex);
			throw ex;
		} finally {
			connection.setAutoCommit(true);
			super.closeConnection(connection);
		}

		return opc_id;
	};
	
	
	public void updateTagAsset(String tagID, String linkedAssetID) throws Exception {
		Connection connection = null;
		//
		try {

			StringBuffer query = new StringBuffer();
			query.append("UPDATE MM_TAGS    \n");
			query.append("   SET LINKED_ASSET_ID = ?,    \n");
			query.append("       UPDATE_DATE = SYSDATE    \n");
			query.append(" WHERE TAG_ID = ?      \n");

			log.debug("\n TagDAO.updateTagAsset query : {\n" + query.toString() + "}");
			log.debug("\n Parameters : [tagID : " + tagID + " linkedAssetID : " + linkedAssetID + "]");

			QueryRunner run = new QueryRunner();
			connection = super.getConnection();
			run.update(connection, query.toString(), new Object[] { linkedAssetID, tagID });
			//
			
			super.getDomainChangeEventBus().onTagChanged(tagID);
			
		} catch (Exception ex) {
			log.error(ex);
			throw ex;
		} finally {
			super.closeConnection(connection);
		}
	}

	public void deleteTagMappingInfo(Tag tag) throws Exception {
		//
		Connection connection = null;
		//
		try {
			// Point Mapping �젙蹂� �궘�젣
			StringBuffer query = new StringBuffer();
			query.append("UPDATE MM_TAGS    \n");
			query.append("   SET LINKED_ASSET_ID = null    \n");
			query.append(" WHERE TAG_ID = ?    \n");
			query.append("   AND LINKED_ASSET_ID = ?    \n");

			log.debug("\n query.toString() : {\n" + query.toString() + "}");
			//
			QueryRunner run = new QueryRunner();
			connection = super.getConnection();
			run.update(connection, query.toString(), new Object[] { tag.getTag_id(), tag.getLinked_asset_id() });

			connection.commit();
			//
			
			super.getDomainChangeEventBus().onTagChanged(tag.getTag_id());
			
		} catch (Exception ex) {
			log.error(ex);
			throw ex;
		} finally {
			super.closeConnection(connection);
		}
	}

	public void deleteTagMappingInfoByTagID(String tagID) throws Exception {
		//
		Connection connection = null;
		//
		try {
			// Point Mapping �젙蹂� �궘�젣
			StringBuffer query = new StringBuffer();
			query.append("UPDATE MM_TAGS    			\n");
			query.append("   SET LINKED_ASSET_ID = null \n");
			query.append(" WHERE TAG_ID = ?    			\n");

			log.debug("\n query.toString() : {\n" + query.toString() + "}");
			//
			QueryRunner run = new QueryRunner();
			connection = super.getConnection();
			run.update(connection, query.toString(), new Object[] { tagID });

			connection.commit();
			//
			
			super.getDomainChangeEventBus().onTagChanged(tagID);
			
		} catch (Exception ex) {
			log.error(ex);
			throw ex;
		} finally {
			super.closeConnection(connection);
		}
	}

}
