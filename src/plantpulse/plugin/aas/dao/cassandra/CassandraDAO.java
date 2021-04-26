package plantpulse.plugin.aas.dao.cassandra;

import java.util.List;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.json.JSONObject;

import com.datastax.driver.core.BoundStatement;
import com.datastax.driver.core.ConsistencyLevel;
import com.datastax.driver.core.PreparedStatement;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.ResultSetFuture;
import com.datastax.driver.core.Row;
import com.datastax.driver.core.Session;

import plantpulse.json.JSONArray;

/**
 * CassandraDAO
 * 
 * @author leesa
 *
 */
public class CassandraDAO {
	
	
	private static final Log log = LogFactory.getLog(CassandraDAO.class);
	
	/**
	 * 알람 최신 목록을 반환한다.
	 * @param asset_id
	 * @return
	 */
	public JSONArray getAlarmList(String asset_id) throws Exception {
			//
		    JSONArray result = new JSONArray();

			//
			try {
				//
				Session session = CassandraSessionFixer.getSession();

				//
				StringBuffer cql = new StringBuffer();
				cql.append(" SELECT JSON * FROM pp.tm_asset_alarm_mv_by_asset_id  WHERE asset_id =  ? LIMIT 10 ");
				PreparedStatement statement =  PreparedStatementCacheFactory.getInstance().get(session, cql.toString(), ConsistencyLevel.ONE);
				
				BoundStatement query = new BoundStatement(statement);
				query.setString(0, asset_id);

				ResultSetFuture rsf = session.executeAsync(query);
				ResultSet rs = rsf.getUninterruptibly();
				List<Row> list  = rs.all();

				for (int i = 0; list != null && i < list.size(); i++) {
					Row row = list.get(i);
					String json_str = row.getString(0);
					json_str = json_str.replaceAll("NaN", "null"); //
					JSONObject json = JSONObject.fromObject(json_str);
					result.add(json);
				};
				
				return result;
			} catch (Exception ex) {
				log.error(ex.getMessage(), ex);
				throw ex;
			} finally {

			}
	}
	
	/**
	 * 타임라인 최식 목록을 반환한다.
	 * @param asset_id
	 * @return
	 * @throws Exception
	 */
	public JSONArray getTimelineList(String asset_id) throws Exception {
		//
	    JSONArray result = new JSONArray();

		//
		try {
			//
			Session session = CassandraSessionFixer.getSession();

			//
			StringBuffer cql = new StringBuffer();
			cql.append(" SELECT JSON * FROM pp.tm_asset_timeline  WHERE asset_id =  ? LIMIT 10 ");
			PreparedStatement statement =  PreparedStatementCacheFactory.getInstance().get(session, cql.toString(), ConsistencyLevel.ONE);
			
			BoundStatement query = new BoundStatement(statement);
			query.setString(0, asset_id);

			ResultSetFuture rsf = session.executeAsync(query);
			ResultSet rs = rsf.getUninterruptibly();
			List<Row> list  = rs.all();

			for (int i = 0; list != null && i < list.size(); i++) {
				Row row = list.get(i);
				String json_str = row.getString(0);
				json_str = json_str.replaceAll("NaN", "null"); //
				JSONObject json = JSONObject.fromObject(json_str);
				result.add(json);
			};
			
			return result;
		} catch (Exception ex) {
			log.error(ex.getMessage(), ex);
			throw ex;
		} finally {

		}
	}

}
