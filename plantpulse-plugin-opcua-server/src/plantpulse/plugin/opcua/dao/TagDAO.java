package plantpulse.plugin.opcua.dao;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.Statement;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import plantpulse.json.JSONArray;
import plantpulse.json.JSONObject;
import plantpulse.plugin.opcua.datasouce.HSQLDBDataSource;
import plantpulse.plugin.opcua.utils.ConstantsJSON;

public class TagDAO{

	private static final Log log = LogFactory.getLog(TagDAO.class);

	/**
	 * selectTagList
	 * @return
	 * @throws Exception
	 */
	public JSONArray selectTagList() throws Exception {
		JSONArray list = new JSONArray();
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
			sql.append(" AND A.OPC_ID IS NOT NULL ");
			sql.append(" AND A.LINKED_ASSET_ID IS NOT NULL ");
			sql.append(" AND A.PUBLISH_KAFKA = 'Y' ");
			
			rs = stmt.executeQuery(sql.toString());

			while (rs.next()) {
				json = new JSONObject();
				
				json.put("site_id",   rs.getString("site_id"));
				json.put("site_name", rs.getString("site_name"));
				json.put("site_desc", rs.getString("site_desc"));
				json.put("opc_id",    rs.getString("opc_id"));
				json.put("opc_name",  rs.getString("opc_name"));
				json.put("opc_desc",  rs.getString("opc_desc"));
				json.put("tag_id",    rs.getString("tag_id"));
				json.put("tag_name",  rs.getString("tag_name"));
				json.put("tag_desc",  rs.getString("tag_desc"));
				json.put("type",      rs.getString("java_type"));
				list.add(json);
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
