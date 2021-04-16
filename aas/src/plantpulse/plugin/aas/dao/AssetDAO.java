package plantpulse.plugin.aas.dao;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import plantpulse.domain.Asset;
import plantpulse.domain.Metadata;
import plantpulse.domain.Site;
import plantpulse.domain.Tag;
import plantpulse.json.JSONObject;
import plantpulse.plugin.aas.datasouce.HSQLDBDataSource;

/**
 * AssetDAO
 * 
 * @author leesa
 *
 */
public class AssetDAO {

	private static final Log log = LogFactory.getLog(AssetDAO.class);

	/**
	 * selectTagList
	 * @return
	 * @throws Exception
	 */
	public List<Site> selectSiteList() throws Exception {
		List<Site> list = new ArrayList<>();
		
		Connection conn = null ;
		Statement stmt = null;
		ResultSet rs = null;
		try{

			conn = HSQLDBDataSource.getInstance().getConnection();
			stmt = conn.createStatement();
			
			StringBuffer sql = new StringBuffer();
			sql.append(" SELECT * FROM mm_sites ");
			
			rs = stmt.executeQuery(sql.toString());

			while (rs.next()) {
				JSONObject json = new JSONObject();
				json.put("site_id",     rs.getString("site_id"));
				json.put("lat",     rs.getString("lat"));
				json.put("lng",     rs.getString("lng"));
				json.put("site_name",   rs.getString("site_name"));
				json.put("description", rs.getString("description"));
				list.add((Site)JSONObject.toBean(json, Site.class));
			}

		}catch(Exception e){
			log.error(e,e);
		}finally {
			if(rs != null) rs.close();
			if(stmt != null) stmt.close();
			if(conn != null)conn.close();
		}
		return list;
	}
	
	
	/**
	 * selectTagList
	 * @return
	 * @throws Exception
	 */
	public List<Asset> selectAssetList(String site_id) throws Exception {
		List<Asset> list = new ArrayList<>();
		Connection conn = null ;
		Statement stmt = null;
		ResultSet rs = null;
		try{

			conn = HSQLDBDataSource.getInstance().getConnection();
			stmt = conn.createStatement();
			
			StringBuffer sql = new StringBuffer();
			sql.append(" SELECT * FROM mm_asset_tree WHERE site_id = '" + site_id + "' AND asset_type = 'M' ");
			
			rs = stmt.executeQuery(sql.toString());

			while (rs.next()) {
				JSONObject json = new JSONObject();
				json.put("site_id",     rs.getString("site_id"));
				json.put("asset_id",    rs.getString("asset_id"));
				json.put("asset_name",  rs.getString("asset_name"));
				json.put("asset_type",  rs.getString("asset_type"));
				json.put("asset_svg_img",  rs.getString("asset_svg_img"));
				json.put("table_type",  rs.getString("table_type"));
				json.put("description", rs.getString("description"));
				json.put("insert_date", rs.getString("insert_date"));
				json.put("update_date", rs.getString("update_date"));
				list.add((Asset)JSONObject.toBean(json, Asset.class));
			}
		}catch(Exception e){
			log.error(e,e);
		}finally {
			if(rs != null) rs.close();
			if(stmt != null) stmt.close();
			if(conn != null)conn.close();
		}
		return list;
	}

	
	
	
	
	/**
	 * selectTagList
	 * @return
	 * @throws Exception
	 */
	public List<Tag> selectTagList(String linked_asset_id) throws Exception {
		List<Tag> list = new ArrayList<>();
		JSONObject json = null;
		Connection conn = null ;
		Statement stmt = null;
		ResultSet rs = null;
		try{

			conn = HSQLDBDataSource.getInstance().getConnection();
			stmt = conn.createStatement();
			
			StringBuffer sql = new StringBuffer();
			sql.append(" SELECT ");
			sql.append(" A.*, ");
			sql.append(" A.DESCRIPTION AS TAG_DESC, ");
			sql.append(" B.OPC_NAME, ");
			sql.append(" B.DESCRIPTION AS OPC_DESC, ");
			sql.append(" C.SITE_ID, ");
			sql.append(" C.SITE_NAME, ");
			sql.append(" C.DESCRIPTION AS SITE_DESC ");
			sql.append(" FROM ");
			sql.append(" MM_TAGS A, ");
			sql.append(" MM_OPCS B, ");
			sql.append(" MM_SITES C ");
			sql.append(" WHERE ");
			sql.append(" A.OPC_ID = B.OPC_ID ");
			sql.append(" AND B.SITE_ID = C.SITE_ID ");
			sql.append(" AND A.LINKED_ASSET_ID = '" + linked_asset_id + "' ");
			
			rs = stmt.executeQuery(sql.toString());

			while (rs.next()) {
				//
				json = new JSONObject();
				json.put("site_id",   rs.getString("site_id"));
				json.put("site_name", rs.getString("site_name"));
				json.put("opc_id",    rs.getString("opc_id"));
				json.put("opc_name",  rs.getString("opc_name"));
				json.put("tag_id",    rs.getString("tag_id"));
				json.put("tag_name",  rs.getString("tag_name"));
				json.put("description",  rs.getString("tag_desc"));
				json.put("java_type",    rs.getString("java_type"));
				json.put("alias_name",   rs.getString("alias_name"));
				json.put("linked_asset_id",   rs.getString("linked_asset_id"));
				list.add((Tag)JSONObject.toBean(json, Tag.class));
			}

	
		}catch(Exception e){
			log.error(e,e);
		}finally {
			if(rs != null) rs.close();
			if(stmt != null) stmt.close();
			if(conn != null)conn.close();
		}
		return list;
	}
	
	

	/**
	 * selectMetadataList
	 * @return
	 * @throws Exception
	 */
	public List<Metadata> selectMetadataList(String asset_id) throws Exception {
		List<Metadata> list = new ArrayList<>();
		Connection conn = null ;
		Statement stmt = null;
		ResultSet rs = null;
		try{

			conn = HSQLDBDataSource.getInstance().getConnection();
			stmt = conn.createStatement();
			
			StringBuffer sql = new StringBuffer();
			sql.append(" SELECT * FROM mm_metadata WHERE object_id = '" + asset_id + "'  ");
			
			rs = stmt.executeQuery(sql.toString());

			while (rs.next()) {
				JSONObject json = new JSONObject();
				json.put("object_id",   rs.getString("object_id"));
				json.put("key",         rs.getString("key"));
				json.put("value",       rs.getString("value"));
				json.put("description", rs.getString("description"));
				list.add((Metadata)JSONObject.toBean(json, Metadata.class));
			}
		}catch(Exception e){
			log.error(e,e);
		}finally {
			if(rs != null) rs.close();
			if(stmt != null) stmt.close();
			if(conn != null)conn.close();
		}
		return list;
	}

}
