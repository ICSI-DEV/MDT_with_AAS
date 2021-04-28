package plantpulse.cep.dao;

import java.sql.Connection;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.apache.commons.lang.StringUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.springframework.stereotype.Repository;

import plantpulse.dbutils.QueryRunner;
import plantpulse.dbutils.handlers.MapListHandler;


@Repository
public class ControlDAO extends CommonDAO {

	private static final Log log = LogFactory.getLog(ControlDAO.class);

	public List<Map<String, Object>> searchControlList(Map<String,String> params) throws Exception {
		Connection connection = null;
		List<Map<String, Object>> list = null;
		//
		try {
			
			List<Object> param_list = new ArrayList<Object>();
			
			//
			StringBuffer query = new StringBuffer();

			query.append("SELECT S.*   \n");
			query.append("  FROM MM_CONTROL S    \n");
			query.append("       WHERE S.CONTROL_DATE BETWEEN CAST ('" + params.get("search_date_from") + ":00" + "' AS TIMESTAMP)  and CAST ('" + params.get("search_date_to") + ":59" + "' AS TIMESTAMP)    \n");
			//
			if (StringUtils.isNotEmpty(params.get("site_name"))) {
				query.append("   AND UPPER(S.SITE_NAME) LIKE '%' ||  UPPER(?) || '%'    \n");
				param_list.add(params.get("site_name"));
			};
			
			if (StringUtils.isNotEmpty(params.get("opc_name"))) {
				query.append("   AND UPPER(S.opc_name) LIKE '%' ||  UPPER(?) || '%'    \n");
				param_list.add(params.get("opc_name"));
			};
			
			if (StringUtils.isNotEmpty(params.get("asset_name"))) {
				query.append("   AND UPPER(S.ASSET_NAME) LIKE '%' ||  UPPER(?) || '%'    \n");
				param_list.add(params.get("asset_name"));
			};
			
			if (StringUtils.isNotEmpty(params.get("tag_id"))) {
				query.append("   AND UPPER(S.tag_id) LIKE '%' ||  UPPER(?) || '%'    \n");
				param_list.add(params.get("tag_id"));
			};
			if (StringUtils.isNotEmpty(params.get("tag_name"))) {
				query.append("   AND UPPER(S.tag_name) LIKE '%' ||  UPPER(?) || '%'    \n");
				param_list.add(params.get("tag_name"));
			};
			
			if (StringUtils.isNotEmpty(params.get("result"))) {
				query.append("   AND UPPER(S.result) LIKE '%' ||  UPPER(?) || '%'    \n");
				param_list.add(params.get("result"));
			};
			
			if (StringUtils.isNotEmpty(params.get("wrtie_value"))) {
				query.append("   AND UPPER(S.wrtie_value) LIKE '%' ||  UPPER(?) || '%'    \n");
				param_list.add(params.get("wrtie_value"));
			};
			
			//
			query.append(" ORDER BY S.CONTROL_DATE DESC, S.SITE_NAME ASC, S.ASSET_NAME ASC, S.TAG_NAME ASC   \n");

			log.debug("\n query.toString() : {\n" + query.toString() + "}");

			QueryRunner run = new QueryRunner();
			connection = super.getConnection();
			list = run.query(connection, query.toString(), new MapListHandler(), param_list.toArray() );

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
	
};
