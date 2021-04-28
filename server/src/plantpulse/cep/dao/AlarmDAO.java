package plantpulse.cep.dao;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.Timestamp;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.commons.lang.StringUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.json.JSONArray;
import org.json.JSONObject;

import plantpulse.cep.domain.Alarm;
import plantpulse.cep.engine.utils.DateUtils;
import plantpulse.cep.service.support.tag.TagLocation;
import plantpulse.server.mvc.util.ParamUtils;
import plantpulse.server.web.jsp.tag.tag.LocationTag;

public class AlarmDAO extends CommonDAO {

	private static final Log log = LogFactory.getLog(AlarmDAO.class);


	/**
	 * Get Alarm list
	 * 
	 * @return
	 * @throws Exception
	 */
	public List<Alarm> getAlarmList(String insert_user_id) throws Exception {
		Connection con = null;
		PreparedStatement pstmt = null;
		ResultSet rs = null;

		List<Alarm> list = new ArrayList<Alarm>();
		//
		try {
			//
			con = super.getConnection();
			StringBuffer query = new StringBuffer();
			query.append("\n");
			query.append("SELECT alarm_config_id,      \n");
			query.append("       alarm_seq,            \n");
			query.append("       priority,             \n");
			query.append("       alarm_date,           \n");
			query.append("       description,          \n");
			query.append("       insert_user_id,       \n");
			query.append("       insert_date,           \n");
			query.append("       is_read,              \n");
			query.append("       read_date             \n");
			query.append("  FROM MM_alarm      \n");
			query.append("  WHERE insert_user_id = '" + insert_user_id + "'      \n");
			query.append(" ORDER BY alarm_seq desc     \n");
			query.append(";\n");
			//
			pstmt = con.prepareStatement(query.toString());
			log.debug("[getalarm_list query] : " + query.toString());
			rs = pstmt.executeQuery();
			//
			while (rs.next()) {
				Alarm alarm = new Alarm();
				alarm.setAlarm_config_id(rs.getString("alarm_config_id"));
				alarm.setAlarm_seq(rs.getInt("alarm_seq"));
				alarm.setPriority(rs.getString("priority"));
				alarm.setAlarm_date(new Date(rs.getTimestamp("alarm_date").getTime()));
				alarm.setDescription(rs.getString("description"));
				alarm.setInsert_user_id(rs.getString("insert_user_id"));
				alarm.setInsert_date(new Date(rs.getTimestamp("insert_date").getTime()));
				alarm.setIs_read(rs.getString("is_read"));
				if (rs.getTimestamp("read_date") != null)
					alarm.setRead_date(new Date(rs.getTimestamp("read_date").getTime()));
				list.add(alarm);
			}
		} catch (Exception ex) {
			log.error("[AlarmDAO.getalarm_list] " + ex.getMessage(), ex);
		} finally {
			if (rs != null)
				rs.close();
			if (pstmt != null)
				pstmt.close();
			closeConnection(con);
		}
		return list;
	}

	public Alarm getAlarm(long alarm_seq, String insert_user_id) throws Exception {
		Connection con = null;
		PreparedStatement pstmt = null;
		ResultSet rs = null;

		Alarm alarm = null;
		//
		try {
			//
			con = super.getConnection();
			StringBuffer query = new StringBuffer();
			query.append("\n");
			query.append("SELECT alarm_config_id,      \n");
			query.append("       alarm_seq,            \n");
			query.append("       priority,             \n");
			query.append("       alarm_date,           \n");
			query.append("       description,          \n");
			query.append("       insert_user_id,       \n");
			query.append("       insert_date,           \n");
			query.append("       is_read,              \n");
			query.append("       read_date             \n");
			query.append("  FROM MM_alarm      \n");
			query.append("  WHERE alarm_seq = " + alarm_seq + "      \n");
			query.append("    AND insert_user_id = '" + insert_user_id + "'     \n");
			query.append(" ORDER BY alarm_seq desc     \n");
			query.append(";\n");
			//
			pstmt = con.prepareStatement(query.toString());
			log.debug("[getalarm_list query] : " + query.toString());
			rs = pstmt.executeQuery();
			//
			if (rs.next()) {
				alarm = new Alarm();
				alarm.setAlarm_config_id(rs.getString("alarm_config_id"));
				alarm.setAlarm_seq(rs.getInt("alarm_seq"));
				alarm.setPriority(rs.getString("priority"));
				alarm.setAlarm_date(new Date(rs.getTimestamp("alarm_date").getTime()));
				alarm.setDescription(rs.getString("description"));
				alarm.setInsert_user_id(rs.getString("insert_user_id"));
				alarm.setInsert_date(new Date(rs.getTimestamp("insert_date").getTime()));
				alarm.setIs_read(rs.getString("is_read"));
				if (rs.getTimestamp("read_date") != null)
					alarm.setRead_date(new Date(rs.getTimestamp("read_date").getTime()));
			}
		} catch (Exception ex) {
			log.error("[AlarmDAO.getalarm_list] " + ex.getMessage(), ex);
		} finally {
			if (rs != null)
				rs.close();
			if (pstmt != null)
				pstmt.close();
			super.closeConnection(con);
		}
		return alarm;
	}

	public JSONObject getAlarmTimestampListByTagId(String alarm_date_from, String alarm_date_to, String tag_id) throws Exception {
		Connection con = null;
		PreparedStatement pstmt = null;
		ResultSet rs = null;

		JSONObject json = null;
		//
		try {
			//
			con = super.getConnection();
			StringBuffer query = new StringBuffer();
			query.append("     SELECT                                                                     \n");
			query.append("            a.alarm_date                                                        \n");
			query.append("       FROM MM_alarm a,                                                  \n");
			query.append("            MM_ALARM_CONFIG c,                                            \n");
			query.append("            MM_tags          d,                                                  \n");
			query.append("            MM_opcs          e,                                                  \n");
			query.append("            MM_sites         f                                                  \n");
			query.append("       WHERE alarm_date between CAST ('" + alarm_date_from + ":00" + "' AS TIMESTAMP)  and CAST ('" + alarm_date_to + ":59" + "' AS TIMESTAMP)    \n");
			query.append("        AND a.alarm_config_id = 	c.alarm_config_id		   \n");
			query.append("        AND c.tag_id    = 	d.tag_id		   \n");
			query.append("        AND d.opc_id    = 	e.opc_id		   \n");
			query.append("        AND e.site_id    = 	f.site_id		   \n");
			query.append("        AND c.tag_id    = 	'" + tag_id + "'		   \n");
			query.append(";\n");
			//
			pstmt = con.prepareStatement(query.toString());
			log.debug("[getAlarmCountByTimestamp query] : " + query.toString());
			rs = pstmt.executeQuery();
			//
			json = new JSONObject();
			while (rs.next()) {

				long timestamp = rs.getTimestamp("alarm_date").getTime() / 1000;
				json.put(timestamp + "", 1);
			}
		} catch (Exception ex) {
			log.error("[AlarmDAO.getAlarmTimestampListByTagId] " + ex.getMessage(), ex);
		} finally {
			if (rs != null)
				rs.close();
			if (pstmt != null)
				pstmt.close();
			super.closeConnection(con);
		}
		return json;
	}

	public JSONArray getAlarmEventDrops(String alarm_date_from, String alarm_date_to, String tag_id) throws Exception {
		Connection con = null;
		PreparedStatement pstmt = null;
		ResultSet rs = null;

		JSONArray array = null;
		//
		try {
			//
			con = super.getConnection();
			StringBuffer query = new StringBuffer();
			query.append("     SELECT                                                                     \n");
			query.append("            a.alarm_date                                                        \n");
			query.append("       FROM MM_alarm a,                                                  \n");
			query.append("            MM_ALARM_CONFIG c,                                            \n");
			query.append("            MM_tags          d,                                                  \n");
			query.append("            MM_opcs          e,                                                  \n");
			query.append("            MM_sites         f                                                  \n");
			query.append("       WHERE alarm_date between CAST ('" + alarm_date_from + ":00" + "' AS TIMESTAMP)  and CAST ('" + alarm_date_to + ":59" + "' AS TIMESTAMP)    \n");
			query.append("        AND a.alarm_config_id = 	c.alarm_config_id		   \n");
			query.append("        AND c.tag_id    = 	d.tag_id		   \n");
			query.append("        AND d.opc_id    = 	e.opc_id		   \n");
			query.append("        AND e.site_id    = 	f.site_id		   \n");
			query.append("        AND c.tag_id    = 	'" + tag_id + "'		   \n");
			query.append(";\n");
			//
			pstmt = con.prepareStatement(query.toString());
			log.debug("[getAlarmEventDrops query] : " + query.toString());
			rs = pstmt.executeQuery();
			//
			array = new JSONArray();
			while (rs.next()) {
				long timestamp = rs.getTimestamp("alarm_date").getTime() / 1000;
				array.add(timestamp);
			}
		} catch (Exception ex) {
			log.error("[AlarmDAO.getAlarmEventDrops] " + ex.getMessage(), ex);
		} finally {
			if (rs != null)
				rs.close();
			if (pstmt != null)
				pstmt.close();
			super.closeConnection(con);
		}
		return array;
	}

	public List<Map<String, Object>> getAlarmPage(Map<String, Object> params, String limit, boolean use_location) throws Exception {
		Connection con = null;
		PreparedStatement pstmt = null;
		ResultSet rs = null;

		List<Map<String, Object>> list = new ArrayList<Map<String, Object>>();

		//
		try {

			String alarm_date_from = (String) params.get("alarm_date_from");
			String alarm_date_to = (String) params.get("alarm_date_to");
			String insert_user_id = (String) params.get("insert_user_id");
			String priority = (String) params.get("priority");
			String description = (String) params.get("description");
			String tag_id = (String) params.get("tag_id");
			String tag_ids = (String) params.get("tag_ids");

			//
			con = super.getConnection();
			StringBuffer query = new StringBuffer();
			query.append("\n");

			query.append("     SELECT a.alarm_seq,                                                           \n");
			query.append("            a.priority,                                                            \n");
			query.append("            a.alarm_date,                                                          \n");
			query.append("            a.description,                                                         \n");
			query.append("            a.insert_user_id,                                                      \n");
			query.append("            a.insert_date,                                                         \n");
			query.append("            a.is_read,              											   \n");
			query.append("            a.read_date,                                                           \n");
			query.append("            d.tag_id,                                                             \n");
			query.append("            d.tag_name,                                                           \n");
			query.append("            e.opc_id,                                                             \n");
			query.append("            e.opc_name,                                                           \n");
			query.append("            f.site_id,                                                             \n");
			query.append("            f.site_name,                                                           \n");
			query.append("            d.description as tag_description,                                      \n");
			query.append("            b.cnt                                                                  \n");
			query.append("       FROM MM_alarm a,                                                  \n");
			query.append("            MM_ALARM_CONFIG c,                                            \n");
			query.append("            MM_tags          d,                                                  \n");
			query.append("            MM_opcs          e,                                                  \n");
			query.append("            MM_sites         f,                                                  \n");
			query.append("            (select count(*) as cnt from MM_alarm) b                     \n");
			query.append("       WHERE alarm_date between CAST ('" + alarm_date_from + ":00" + "' AS TIMESTAMP)  and CAST ('" + alarm_date_to + ":59" + "' AS TIMESTAMP)    \n");
			query.append("        AND a.alarm_config_id = 	c.alarm_config_id		   \n");
			query.append("        AND c.tag_id    = 	d.tag_id		   \n");
			query.append("        AND d.opc_id    = 	e.opc_id		   \n");
			query.append("        AND e.site_id    = 	f.site_id		   \n");
			query.append("        AND a.insert_user_id = '" + insert_user_id + "'   						   \n");
			if (priority != null) {
				query.append("        AND a.priority LIKE '%" + priority + "%'   						   \n");
			}
			if (tag_id != null) {
				query.append("        AND c.tag_id = '" + tag_id + "'   						   \n");
			}
			if (description != null) {
				query.append("        AND a.description LIKE '%" + description + "%'   						   \n");
			}
			if (tag_ids != null) {
				query.append("   AND c.tag_id IN (  " + ParamUtils.commaStringToSQLInString(tag_ids) + " ) \n");
			}
			query.append("      ORDER by a.alarm_seq desc                                                    \n");
			query.append("      LIMIT " + limit + "   \n");

			query.append(";\n");
			//
			pstmt = con.prepareStatement(query.toString());
			log.debug("[getAlarmPage query] : " + query.toString());
			rs = pstmt.executeQuery();
			//
			LocationTag location_tag = new LocationTag();
			//
			while (rs.next()) {
				Map<String, Object> map = new HashMap<String, Object>();
				map.put("alarm_seq", rs.getInt("alarm_seq"));
				map.put("priority", rs.getString("priority"));
				//TODO 타임스탬프로 변경
				map.put("alarm_date", new Date(rs.getTimestamp("alarm_date").getTime()));
				map.put("alarm_timestamp", rs.getTimestamp("alarm_date").getTime());
				map.put("description", rs.getString("description"));
				map.put("tag_id", rs.getString("tag_id"));
				map.put("tag_name", rs.getString("tag_name"));
				map.put("opc_id", rs.getString("opc_id"));
				map.put("opc_name", rs.getString("opc_name"));
				map.put("site_id", rs.getString("site_id"));
				map.put("site_name", rs.getString("site_name"));
				map.put("tag_description", rs.getString("tag_description"));
				map.put("insert_user_id", rs.getString("insert_user_id"));
				map.put("insert_date", rs.getTimestamp("insert_date"));
				map.put("is_read", rs.getString("is_read"));
				if (rs.getTimestamp("read_date") != null) {
					map.put("read_date", new Date(rs.getTimestamp("read_date").getTime()));
				} else {
					map.put("read_date", null);
				}
				//

				if(use_location){
					TagLocation location = new TagLocation();
					String tag_location = location.getLocation(rs.getString("tag_id"), false);
					map.put("tag_location", tag_location);
				}
				//
				list.add(map);
			}
		} catch (Exception ex) {
			log.error("[AlarmDAO.getAlarmPage] " + ex.getMessage(), ex);
		} finally {
			if (rs != null)
				rs.close();
			if (pstmt != null)
				pstmt.close();
			super.closeConnection(con);
		}
		return list;
	};
	
	public List<Map<String, Object>> getAlarmFlag(Map<String, Object> params, String limit) throws Exception {
		Connection con = null;
		PreparedStatement pstmt = null;
		ResultSet rs = null;

		List<Map<String, Object>> list = new ArrayList<Map<String, Object>>();

		//
		try {

			String alarm_date_from = (String) params.get("alarm_date_from");
			String alarm_date_to = (String) params.get("alarm_date_to");
			String insert_user_id = (String) params.get("insert_user_id");
			String priority = (String) params.get("priority");
			String description = (String) params.get("description");
			String tag_id = (String) params.get("tag_id");
			String tag_ids = (String) params.get("tag_ids");

			//
			con = super.getConnection();
			StringBuffer query = new StringBuffer();
			query.append("\n");

			query.append("     SELECT a.alarm_seq,                                                           \n");
			query.append("            a.priority,                                                            \n");
			query.append("            a.alarm_date,                                                          \n");
			query.append("            a.description,                                                         \n");
			query.append("            a.insert_user_id,                                                      \n");
			query.append("            a.insert_date,                                                         \n");
			query.append("            a.is_read,              											   \n");
			query.append("            a.read_date,                                                           \n");
			query.append("            d.tag_id,                                                             \n");
			query.append("            d.tag_name,                                                           \n");
			query.append("            e.opc_id,                                                             \n");
			query.append("            e.opc_name,                                                           \n");
			query.append("            f.site_id,                                                             \n");
			query.append("            f.site_name,                                                           \n");
			query.append("            d.description as tag_description,                                      \n");
			query.append("            b.cnt                                                                  \n");
			query.append("       FROM MM_alarm a,                                                  \n");
			query.append("            MM_ALARM_CONFIG c,                                            \n");
			query.append("            MM_tags          d,                                                  \n");
			query.append("            MM_opcs          e,                                                  \n");
			query.append("            MM_sites         f,                                                  \n");
			query.append("            (select count(*) as cnt from MM_alarm) b                     \n");
			query.append("       WHERE alarm_date between CAST ('" + alarm_date_from + ":00" + "' AS TIMESTAMP)  and CAST ('" + alarm_date_to + ":59" + "' AS TIMESTAMP)    \n");
			query.append("        AND a.alarm_config_id = 	c.alarm_config_id		   \n");
			query.append("        AND c.tag_id    = 	d.tag_id		   \n");
			query.append("        AND d.opc_id    = 	e.opc_id		   \n");
			query.append("        AND e.site_id    = 	f.site_id		   \n");
			query.append("        AND a.insert_user_id = '" + insert_user_id + "'   						   \n");
			if (priority != null) {
				query.append("        AND a.priority LIKE '%" + priority + "%'   						   \n");
			}
			if (tag_id != null) {
				query.append("        AND c.tag_id = '" + tag_id + "'   						   \n");
			}
			if (description != null) {
				query.append("        AND a.description LIKE '%" + description + "%'   						   \n");
			}
			if (tag_ids != null) {
				query.append("   AND c.tag_id IN (  " + ParamUtils.commaStringToSQLInString(tag_ids) + " ) \n");
			}
			query.append("      ORDER by a.alarm_date desc                                                    \n");
			query.append("      LIMIT " + limit + "   \n");

			query.append(";\n");
			//
			pstmt = con.prepareStatement(query.toString());
			log.debug("[getAlarmPage query] : " + query.toString());
			rs = pstmt.executeQuery();
			//
			LocationTag location_tag = new LocationTag();
			//
			while (rs.next()) {
				Map<String, Object> map = new HashMap<String, Object>();
				map.put("alarm_seq", rs.getInt("alarm_seq"));
				map.put("priority", rs.getString("priority"));
				//TODO 타임스탬프로 변경
				map.put("alarm_date", new Date(rs.getTimestamp("alarm_date").getTime()));
				map.put("alarm_timestamp", rs.getTimestamp("alarm_date").getTime());
				map.put("description", rs.getString("description"));
				map.put("tag_id", rs.getString("tag_id"));
				map.put("tag_name", rs.getString("tag_name"));
				map.put("opc_id", rs.getString("opc_id"));
				map.put("opc_name", rs.getString("opc_name"));
				map.put("site_id", rs.getString("site_id"));
				map.put("site_name", rs.getString("site_name"));
				map.put("tag_description", rs.getString("tag_description"));
				map.put("insert_user_id", rs.getString("insert_user_id"));
				map.put("insert_date", rs.getTimestamp("insert_date"));
				map.put("is_read", rs.getString("is_read"));
				if (rs.getTimestamp("read_date") != null) {
					map.put("read_date", new Date(rs.getTimestamp("read_date").getTime()));
				} else {
					map.put("read_date", null);
				};
				
				//
				TagLocation location = new TagLocation();
				String tag_location = location.getLocation(rs.getString("tag_id"), false);
				map.put("tag_location", tag_location);
				//
				list.add(map);
			}
		} catch (Exception ex) {
			log.error("[AlarmDAO.getAlarmPage] " + ex.getMessage(), ex);
		} finally {
			if (rs != null)
				rs.close();
			if (pstmt != null)
				pstmt.close();
			super.closeConnection(con);
		}
		return list;
	};
	
	

	public long getAlarmCount(Map<String, Object> params) throws Exception {
		Connection con = null;
		PreparedStatement pstmt = null;
		ResultSet rs = null;

		long count = 0;

		//
		try {

			String alarm_date_from = (String) params.get("alarm_date_from");
			String alarm_date_to = (String) params.get("alarm_date_to");
			String insert_user_id = (String) params.get("insert_user_id");

			String priority = (String) params.get("priority");
			String description = (String) params.get("description");
			String tag_id = (String) params.get("tag_id");
			String tag_ids = (String) params.get("tag_ids");

			//
			con = super.getConnection();
			StringBuffer query = new StringBuffer();
			query.append("\n");
			query.append("     SELECT count(*) as cnt                                                    \n");
			query.append("       FROM MM_alarm a,                                                  \n");
			query.append("            MM_ALARM_CONFIG c,                                            \n");
			query.append("            MM_tags          d,                                                  \n");
			query.append("            MM_opcs          e,                                                  \n");
			query.append("            MM_sites         f,                                                  \n");
			query.append("            (select count(*) as cnt from MM_alarm) b                     \n");
			query.append("       WHERE alarm_date between CAST ('" + alarm_date_from + ":00" + "' AS TIMESTAMP)  and CAST ('" + alarm_date_to + ":59" + "' AS TIMESTAMP)    \n");
			query.append("        AND a.alarm_config_id = 	c.alarm_config_id		   \n");
			query.append("        AND c.tag_id    = 	d.tag_id		   \n");
			query.append("        AND d.opc_id    = 	e.opc_id		   \n");
			query.append("        AND e.site_id    = 	f.site_id		   \n");
			query.append("        AND a.insert_user_id = '" + insert_user_id + "'   						   \n");
			if (priority != null) {
				query.append("        AND a.priority LIKE '%" + priority + "%'   						   \n");
			}
			if (tag_id != null) {
				query.append("        AND c.tag_id = '" + tag_id + "'   						   \n");
			}
			if (description != null) {
				query.append("        AND a.description LIKE '%" + description + "%'   						   \n");
			}
			if (tag_ids != null) {
				query.append("   AND c.tag_id IN (  " + ParamUtils.commaStringToSQLInString(tag_ids) + " ) \n");
			}
			query.append(";\n");
			//
			pstmt = con.prepareStatement(query.toString());
			log.debug("[getAlarmCount query] : " + query.toString());
			rs = pstmt.executeQuery();
			//
			if (rs.next()) {
				count = rs.getInt("cnt");
			}
		} catch (Exception ex) {
			log.error("[AlarmDAO.getalarm_count] " + ex.getMessage(), ex);
		} finally {
			if (rs != null)
				rs.close();
			if (pstmt != null)
				pstmt.close();
			super.closeConnection(con);
		}
		return count;
	};

	public List<Map<String, Object>> getAlarmCountPriorityByDate(Map<String, Object> params) throws Exception {

		List<Map<String, Object>> list = new ArrayList<Map<String, Object>>();

		Connection con = null;
		PreparedStatement pstmt = null;
		ResultSet rs = null;

		//
		try {

			String alarm_date_from = (String) params.get("alarm_date_from");
			String alarm_date_to = (String) params.get("alarm_date_to");
			String insert_user_id = (String) params.get("insert_user_id");

			String priority = (String) params.get("priority");
			String description = (String) params.get("description");
			String tag_id = (String) params.get("tag_id");
			String tag_ids = (String) params.get("tag_ids");

			con = super.getConnection();

			//
			DateFormat format = new SimpleDateFormat("yyyy-MM-dd HH:mm");
			Date startdate = format.parse(alarm_date_from);
			Date enddate = format.parse(alarm_date_to);

			List<Date> date_list = DateUtils.getBetweenDates(startdate, enddate);
			for (int i = 0; date_list != null && i < date_list.size(); i++) {
				Date date = date_list.get(i);
				//
				DateFormat dformat = new SimpleDateFormat("yyyy-MM-dd");
				//
				StringBuffer query = new StringBuffer();
				query.append("\n");
				query.append("     SELECT priority, count(*) AS cnt                                          \n");
				query.append("       FROM MM_alarm a,                                                  \n");
				query.append("            MM_ALARM_CONFIG c,                                            \n");
				query.append("            MM_tags          d,                                                  \n");
				query.append("            MM_opcs          e,                                                  \n");
				query.append("            MM_sites         f,                                                  \n");
				query.append("            (select count(*) as cnt from MM_alarm) b                     \n");
				query.append(
						"       WHERE a.alarm_date BETWEEN CAST ('" + dformat.format(date) + " 00:00:00" + "' AS TIMESTAMP)  and CAST ('" + dformat.format(date) + " 23:59:59" + "' AS TIMESTAMP) ");
				query.append("        AND a.alarm_config_id = 	c.alarm_config_id		   \n");
				query.append("        AND c.tag_id    = 	d.tag_id		   \n");
				query.append("        AND d.opc_id    = 	e.opc_id		   \n");
				query.append("        AND e.site_id    = 	f.site_id		   \n");
				query.append("        AND a.insert_user_id = '" + insert_user_id + "'   						   \n");
				if (priority != null) {
					query.append("        AND a.priority LIKE '%" + priority + "%'   						   \n");
				}
				if (tag_id != null) {
					query.append("        AND c.tag_id = '" + tag_id + "'   						   \n");
				}
				if (description != null) {
					query.append("        AND a.description LIKE '%" + description + "%'   						   \n");
				}
				if (tag_ids != null) {
					query.append("   AND c.tag_id IN (  " + ParamUtils.commaStringToSQLInString(tag_ids) + " ) \n");
				}
				query.append("   GROUP by priority ");
				query.append(";\n");

				//
				pstmt = con.prepareStatement(query.toString());
				rs = pstmt.executeQuery();
				//
				Map<String, Object> map = new HashMap<String, Object>();
				DateFormat df = new SimpleDateFormat("yyyy-MM-dd (E)");
				map.put("timestamp", df.format(date));
				map.put("INFO", 0);
				map.put("WARN", 0);
				map.put("ERROR", 0);
				while (rs.next()) {
					map.put(rs.getString("priority"), rs.getInt("cnt"));
				}
				list.add(map);

				if (rs != null) {
					rs.close();
				}
				if (pstmt != null)
					pstmt.close();
			}

		} catch (Exception ex) {
			log.error("[AlarmDAO.getAlarmCountPriorityByHour] " + ex.getMessage(), ex);
		} finally {
			super.closeConnection(con);
		}
		return list;
	};

	public int getAlarmCountByPriority(Map<String, Object> params, String priority) throws Exception {

		Connection con = null;
		PreparedStatement pstmt = null;
		ResultSet rs = null;

		int cnt = 0;

		//
		try {

			String alarm_date_from = (String) params.get("alarm_date_from");
			String alarm_date_to = (String) params.get("alarm_date_to");
			String insert_user_id = (String) params.get("insert_user_id");

			// String priority = (String) params.get("priority");
			String description = (String) params.get("description");
			String tag_id = (String) params.get("tag_id");
			String tag_ids = (String) params.get("tag_ids");

			//
			con = super.getConnection();
			StringBuffer query = new StringBuffer();
			query.append("\n");
			query.append("     SELECT count(*) as cnt                                                    \n");
			query.append("       FROM MM_alarm a,                                                  \n");
			query.append("            MM_ALARM_CONFIG c,                                            \n");
			query.append("            MM_tags          d,                                                  \n");
			query.append("            MM_opcs          e,                                                  \n");
			query.append("            MM_sites         f,                                                  \n");
			query.append("            (select count(*) as cnt from MM_alarm) b                     \n");
			query.append("       WHERE alarm_date between CAST ('" + alarm_date_from + ":00" + "' AS TIMESTAMP)  and CAST ('" + alarm_date_to + ":59" + "' AS TIMESTAMP)    \n");
			query.append("        AND a.alarm_config_id = 	c.alarm_config_id		   \n");
			query.append("        AND c.tag_id    = 	d.tag_id		   \n");
			query.append("        AND d.opc_id    = 	e.opc_id		   \n");
			query.append("        AND e.site_id    = 	f.site_id		   \n");
			query.append("        AND a.insert_user_id = '" + insert_user_id + "'   						   \n");
			if (priority != null) {
				query.append("        AND a.priority LIKE '%" + priority + "%'   						   \n");
			}
			if (tag_id != null) {
				query.append("        AND c.tag_id = '" + tag_id + "'   						   \n");
			}
			if (description != null) {
				query.append("        AND a.description LIKE '%" + description + "%'   						   \n");
			}
			if (tag_ids != null) {
				query.append("   AND c.tag_id IN (  " + ParamUtils.commaStringToSQLInString(tag_ids) + " ) \n");
			}
			query.append(";\n");
			//
			pstmt = con.prepareStatement(query.toString());
			log.debug("[getalarm_count query] : " + query.toString());
			rs = pstmt.executeQuery();
			//
			if (rs.next()) {
				cnt = rs.getInt("cnt");
			}
		} catch (Exception ex) {
			log.error("[AlarmDAO.getAlarmCountByPriority] " + ex.getMessage(), ex);
		} finally {
			if (rs != null)
				rs.close();
			if (pstmt != null)
				pstmt.close();
			super.closeConnection(con);
		}
		return cnt;
	};

	public List<Map<String, Object>> getAlarmRank(Map<String, Object> params) throws Exception {
		Connection con = null;
		PreparedStatement pstmt = null;
		ResultSet rs = null;

		List<Map<String, Object>> list = new ArrayList<Map<String, Object>>();
		String alarm_date_from = (String) params.get("alarm_date_from");
		String alarm_date_to = (String) params.get("alarm_date_to");
		String insert_user_id = (String) params.get("insert_user_id");
		// String priority = (String) params.get("priority");
		// String description = (String) params.get("description");
		//
		try {
			//
			con = super.getConnection();
			StringBuffer query = new StringBuffer();
			query.append("     SELECT    * FROM (                                                                    ");
			query.append("     SELECT                                                                        ");
			query.append("            c.alarm_config_id,                                                    \n");
			query.append("            c.alarm_config_name,                                                    \n");
			query.append("            c.alarm_config_priority,                                               \n");
			query.append("            c.message,                                                            \n");
			query.append("            c.tag_id,                                                             \n");
			query.append("            d.tag_name,                                                           \n");
			query.append("            e.opc_id,                                                             \n");
			query.append("            e.opc_name,                                                           \n");
			query.append("            f.site_id,                                                             \n");
			query.append("            f.site_name,                                                           \n");
			query.append("            count(*) as alarm_count                                               \n");
			query.append("      FROM  MM_alarm a,                                                  \n");
			query.append("            MM_ALARM_CONFIG c,                                            \n");
			query.append("            MM_tags          d,                                                  \n");
			query.append("            MM_opcs          e,                                                  \n");
			query.append("            MM_sites         f                                                   \n");
			query.append("       WHERE a.alarm_date between CAST ('" + alarm_date_from + ":00" + "' AS TIMESTAMP)  and CAST ('" + alarm_date_to + ":59" + "' AS TIMESTAMP)    \n");
			query.append("        AND a.alarm_config_id = 	c.alarm_config_id		   \n");
			query.append("        AND c.tag_id    = 	d.tag_id		   \n");
			query.append("        AND d.opc_id    = 	e.opc_id		   \n");
			query.append("        AND e.site_id   = 	f.site_id		   \n");
			query.append("        AND a.insert_user_id = '" + insert_user_id + "'   						   \n");
			query.append("      GROUP by c.alarm_config_id, c.alarm_config_name, c.alarm_config_priority, c.message, c.tag_id, d.tag_name, e.opc_id, e.opc_name, f.site_id, f.site_name  \n");
			query.append("  ) ORDER BY alarm_count DESC \n ");
			query.append("    LIMIT 10                  \n ");
			query.append(";\n");
			//
			pstmt = con.prepareStatement(query.toString());
			log.debug("[getAlarmRank query] : " + query.toString());
			rs = pstmt.executeQuery();
			//
			while (rs.next()) {
				Map<String, Object> map = new HashMap<String, Object>();
				map.put("alarm_config_id", rs.getString("alarm_config_id"));
				map.put("alarm_config_name", rs.getString("alarm_config_name"));
				map.put("alarm_config_priority", rs.getString("alarm_config_priority"));
				map.put("message", rs.getString("message"));
				map.put("alarm_count", rs.getInt("alarm_count"));

				map.put("tag_id", rs.getString("tag_id"));
				map.put("tag_name", rs.getString("tag_name"));
				map.put("opc_id", rs.getString("opc_id"));
				map.put("opc_name", rs.getString("opc_name"));
				map.put("site_id", rs.getString("site_id"));
				map.put("site_name", rs.getString("site_name"));

				list.add(map);
			}
		} catch (Exception ex) {
			log.error("[AlarmDAO.getAlarmRank] " + ex.getMessage(), ex);
		} finally {
			if (rs != null)
				rs.close();
			if (pstmt != null)
				pstmt.close();
			super.closeConnection(con);
		}
		return list;
	};

	
	
	public List<Map<String, Object>> getAlarmRankByOPC(Map<String, Object> params) throws Exception {
		Connection con = null;
		PreparedStatement pstmt = null;
		ResultSet rs = null;

		List<Map<String, Object>> list = new ArrayList<Map<String, Object>>();
		String alarm_date_from = (String) params.get("alarm_date_from");
		String alarm_date_to = (String) params.get("alarm_date_to");
		String insert_user_id = (String) params.get("insert_user_id");
		// String priority = (String) params.get("priority");
		// String description = (String) params.get("description");
		//
		try {
			//
			con = super.getConnection();
			StringBuffer query = new StringBuffer();
			query.append("     SELECT * FROM ( ");
			query.append("     	     SELECT    ");                                                      
			query.append("     	             f.site_name,");
			query.append("     	             e.opc_id,");
			query.append("     	             e.opc_name,");
			query.append("     	             e.description,");
			 query.append("     	            count(*) as alarm_count ");                                             
			 query.append("     	      FROM  MM_alarm a,          ");                                       
			 query.append("     	            MM_ALARM_CONFIG c,     ");                                      
            query.append("     	            MM_tags          d,     ");                                            
            query.append("     	            MM_opcs          e,    ");                                             
            query.append("     	            MM_sites         f    ");
            query.append("     	       WHERE a.alarm_date between CAST ('" + alarm_date_from + ":00" + "' AS TIMESTAMP)  and CAST ('" + alarm_date_to + ":59" + "' AS TIMESTAMP)    ");
            query.append("     	        AND a.alarm_config_id = 	c.alarm_config_id		   ");
            query.append("     	        AND c.tag_id    = 	d.tag_id	 ");	  
            query.append("     	        AND d.opc_id    = 	e.opc_id		");  
            query.append("     	        AND e.site_id   = 	f.site_id	");
    		query.append("     	        AND a.insert_user_id = '" + insert_user_id + "'   			 ");			  
    		query.append("     	      GROUP by f.site_name, e.opc_id, e.opc_name, e.description ");
    		query.append("     	  ) ORDER BY alarm_count DESC  ");

			query.append(";\n");
			//
			pstmt = con.prepareStatement(query.toString());
			log.debug("[getAlarmRankByOPC query] : " + query.toString());
			rs = pstmt.executeQuery();
			//
			while (rs.next()) {
				Map<String, Object> map = new HashMap<String, Object>();
				map.put("site_name", rs.getString("site_name"));
				map.put("opc_id", rs.getString("opc_id"));
				map.put("opc_name", rs.getString("opc_name"));
				map.put("description", rs.getString("description"));
				map.put("alarm_count", rs.getInt("alarm_count"));

			   list.add(map);
			}
		} catch (Exception ex) {
			log.error("[AlarmDAO.getAlarmRankByOPC] " + ex.getMessage(), ex);
		} finally {
			if (rs != null)
				rs.close();
			if (pstmt != null)
				pstmt.close();
			super.closeConnection(con);
		}
		return list;
	};
	
	
	public List<Map<String, Object>> getAlarmRankByAsset(Map<String, Object> params) throws Exception {
		Connection con = null;
		PreparedStatement pstmt = null;
		ResultSet rs = null;

		List<Map<String, Object>> list = new ArrayList<Map<String, Object>>();
		String alarm_date_from = (String) params.get("alarm_date_from");
		String alarm_date_to = (String) params.get("alarm_date_to");
		String insert_user_id = (String) params.get("insert_user_id");
		// String priority = (String) params.get("priority");
		// String description = (String) params.get("description");
		//
		try {
			//
			con = super.getConnection();
			StringBuffer query = new StringBuffer();
			query.append("     SELECT * FROM ( ");
			query.append("     	     SELECT    ");                                                      
			query.append("     	             f.site_name,");
			query.append("     	             d.linked_asset_id as asset_id,");
			query.append("     	             g.asset_name,");
			query.append("     	             g.description,");
			 query.append("     	            count(*) as alarm_count ");                                             
			 query.append("     	      FROM  MM_alarm a,          ");                                       
			 query.append("     	            MM_ALARM_CONFIG c,     ");                                      
            query.append("     	            MM_tags          d,     ");                                            
            query.append("     	            MM_opcs          e,    ");                                             
            query.append("     	            MM_sites         f ,   ");
            query.append("     	            mm_asset_tree    g     ");
            query.append("     	       WHERE a.alarm_date between CAST ('" + alarm_date_from + ":00" + "' AS TIMESTAMP)  and CAST ('" + alarm_date_to + ":59" + "' AS TIMESTAMP)    ");
            query.append("     	        AND a.alarm_config_id = 	c.alarm_config_id		   ");
            query.append("     	        AND c.tag_id    = 	d.tag_id	 ");	  
            query.append("     	        AND d.opc_id    = 	e.opc_id		");  
            query.append("     	        AND e.site_id   = 	f.site_id	");
    		query.append("     	        AND d.linked_asset_id = g.asset_id  ");
    		query.append("     	        AND a.insert_user_id = '" + insert_user_id + "'   			 ");			  
    		query.append("     	      GROUP by f.site_name, d.linked_asset_id, g.asset_name, g.description  ");
    		query.append("     	  ) ORDER BY alarm_count DESC  ");

			query.append(";\n");
			//
			pstmt = con.prepareStatement(query.toString());
			log.debug("[getAlarmRankByAsset query] : " + query.toString());
			rs = pstmt.executeQuery();
			//
			while (rs.next()) {
				Map<String, Object> map = new HashMap<String, Object>();
				map.put("site_name", rs.getString("site_name"));
				map.put("asset_id", rs.getString("asset_id"));
				map.put("asset_name", rs.getString("asset_name"));
				map.put("description", rs.getString("description"));
				map.put("alarm_count", rs.getInt("alarm_count"));

			   list.add(map);
			}
		} catch (Exception ex) {
			log.error("[AlarmDAO.getAlarmRankByAsset] " + ex.getMessage(), ex);
		} finally {
			if (rs != null)
				rs.close();
			if (pstmt != null)
				pstmt.close();
			super.closeConnection(con);
		}
		return list;
	};

	
	
	
	public long getAlarmCountByInsertUserId(String insert_user_id) throws Exception {
		Connection con = null;
		PreparedStatement pstmt = null;
		ResultSet rs = null;

		long count = 0;

		//
		try {
			//
			con = super.getConnection();
			StringBuffer query = new StringBuffer();
			query.append("\n");
			query.append("     SELECT count(*) as cnt                                                      \n");
			query.append("       FROM MM_alarm                                                  \n");
			query.append("       WHERE insert_user_id = '" + insert_user_id + "'   						   \n");
			query.append(";\n");
			//
			pstmt = con.prepareStatement(query.toString());
			log.debug("[getAlarmCountByInsertUserId query] : " + query.toString());
			rs = pstmt.executeQuery();
			//
			if (rs.next()) {
				count = rs.getInt("cnt");
			}
		} catch (Exception ex) {
			log.error("[AlarmDAO.getAlarmCountByInsertUserId] " + ex.getMessage(), ex);
		} finally {
			if (rs != null)
				rs.close();
			if (pstmt != null)
				pstmt.close();
			super.closeConnection(con);
		}
		return count;
	}

	public List<Map<String, Object>> getRecentAlarmData(String insert_user_id) throws Exception {
		Connection con = null;
		PreparedStatement pstmt = null;
		ResultSet rs = null;

		List<Map<String, Object>> list = new ArrayList<Map<String, Object>>();
		//
		try {
			//
			con = super.getConnection();
			StringBuffer query = new StringBuffer();
			query.append("\n");
			query.append("SELECT t.*,                        \n");
			query.append("       rownum() as rnum            \n");
			query.append("  FROM (                           \n");
			query.append("         select *                  \n");
			query.append("           FROM MM_alarm   \n");
			query.append("          WHERE insert_user_id = '" + insert_user_id + "'  \n");
			query.append("          ORDER BY alarm_seq desc  \n");
			query.append("       ) t                         \n");
			query.append(" WHERE rownum() <= 5               \n");
			query.append(";\n");
			//
			pstmt = con.prepareStatement(query.toString());
			log.debug("[getAlarmPage query] : " + query.toString());
			rs = pstmt.executeQuery();
			//
			while (rs.next()) {
				Map<String, Object> map = new HashMap<String, Object>();
				map.put("alarm_seq", rs.getInt("alarm_seq"));
				map.put("priority", rs.getString("priority"));
				map.put("alarm_date", new Date(rs.getTimestamp("alarm_date").getTime()));
				map.put("description", rs.getString("description"));
				map.put("insert_user_id", rs.getString("insert_user_id"));
				map.put("insert_date", rs.getDate("insert_date"));
				map.put("rnum", rs.getInt("rnum"));
				list.add(map);

				//

			}
		} catch (Exception ex) {
			log.error("[AlarmDAO.getRecentAlarmData] " + ex.getMessage(), ex);
		} finally {
			if (rs != null)
				rs.close();
			if (pstmt != null)
				pstmt.close();
			super.closeConnection(con);
		}
		return list;
	}

	public List<Map<String, Object>> getTodayAlarmData(String insert_user_id) throws Exception {
		Connection con = null;
		PreparedStatement pstmt = null;
		ResultSet rs = null;

		List<Map<String, Object>> list = new ArrayList<Map<String, Object>>();
		//
		try {

			SimpleDateFormat format = new SimpleDateFormat("yyyy-MM-dd");
			String to_date = format.format(new Date()); //

			//
			con = super.getConnection();
			StringBuffer query = new StringBuffer();
			query.append("\n");
			query.append("SELECT T.*,                        \n");
			query.append("       ROWNUM() AS RNUM            \n");
			query.append("  FROM (                           \n");
			query.append("         SELECT A.*,                  \n");
			query.append("                AC.TAG_ID,           \n");
			query.append("                B.TAG_NAME,           \n");
			query.append("                C.OPC_ID,              \n");
			query.append("                C.OPC_NAME,            \n");
			query.append("                D.SITE_ID,             \n");
			query.append("                D.SITE_NAME           \n");
			query.append("           FROM MM_ALARM A,  \n");
			query.append("                 MM_ALARM_CONFIG AC,  \n");
			query.append("                MM_TAGS B,  \n");
			query.append("                MM_OPCS C,  \n");
			query.append("                MM_SITES D  \n");
			query.append("          WHERE A.ALARM_CONFIG_ID = AC.ALARM_CONFIG_ID \n");
			query.append("            AND AC.TAG_ID = B.TAG_ID \n");
			query.append("            AND B.OPC_ID = C.OPC_ID \n");
			query.append("            AND C.SITE_ID = D.SITE_ID \n");
			query.append("            AND A.INSERT_USER_ID = '" + insert_user_id + "'  \n");
			query.append("            AND A.ALARM_DATE BETWEEN CAST ('" + to_date + " 00:00:00" + "' AS TIMESTAMP)  AND CAST ('" + to_date + " 23:59:59" + "' AS TIMESTAMP)    \n");
			query.append("          ORDER BY A.ALARM_SEQ DESC  \n");
			query.append("       ) T                         \n");
			query.append(" WHERE ROWNUM() <= 100               \n");
			query.append(";\n");
			//
			pstmt = con.prepareStatement(query.toString());
			log.debug("[getTodayAlarmData query] : " + query.toString());
			rs = pstmt.executeQuery();
			//
			while (rs.next()) {
				Map<String, Object> map = new HashMap<String, Object>();
				map.put("alarm_seq", rs.getInt("alarm_seq"));
				map.put("priority", rs.getString("priority"));
				map.put("alarm_date", new Date(rs.getTimestamp("alarm_date").getTime()));
				map.put("description", rs.getString("description"));
				map.put("insert_user_id", rs.getString("insert_user_id"));
				map.put("insert_date", rs.getDate("insert_date"));
				map.put("rnum", rs.getInt("rnum"));

				map.put("tag_id", rs.getString("tag_id"));
				map.put("tag_name", rs.getString("tag_name"));
				map.put("opc_id", rs.getString("opc_id"));
				map.put("opc_name", rs.getString("opc_name"));
				map.put("site_id", rs.getString("site_id"));
				map.put("site_name", rs.getString("site_name"));
				list.add(map);

				//

			}
		} catch (Exception ex) {
			log.error("[AlarmDAO.getTodayAlarmData] " + ex.getMessage(), ex);
		} finally {
			if (rs != null)
				rs.close();
			if (pstmt != null)
				pstmt.close();
			super.closeConnection(con);
		}
		return list;
	}

	public int getTodayAlarmTotalCount(String insert_user_id) throws Exception {
		Connection con = null;
		PreparedStatement pstmt = null;
		ResultSet rs = null;

		int count = 0;
		//
		try {

			SimpleDateFormat format = new SimpleDateFormat("yyyy-MM-dd");
			String to_date = format.format(new Date()); //

			//
			con = super.getConnection();
			StringBuffer query = new StringBuffer();
			query.append("\n");

			query.append("         select count(*) as total_count  \n");
			query.append("           FROM MM_alarm   \n");
			query.append("          WHERE insert_user_id = '" + insert_user_id + "'  \n");
			query.append("            AND alarm_date between CAST ('" + to_date + " 00:00:00" + "' AS TIMESTAMP)  and CAST ('" + to_date + " 23:59:59" + "' AS TIMESTAMP)    \n");

			pstmt = con.prepareStatement(query.toString());
			log.debug("[getAlarmPage query] : " + query.toString());
			rs = pstmt.executeQuery();
			//
			if (rs.next()) {
				count = rs.getInt("total_count");

			}
		} catch (Exception ex) {
			log.error("[AlarmDAO.getRecentAlarmData] " + ex.getMessage(), ex);
		} finally {
			if (rs != null)
				rs.close();
			if (pstmt != null)
				pstmt.close();
			super.closeConnection(con);
		}
		return count;
	}

	public int getAlarmCount(String insert_user_id) throws Exception {
		Connection con = null;
		PreparedStatement pstmt = null;
		ResultSet rs = null;
		int cnt = 0;
		//
		try {
			//
			con = super.getConnection();
			StringBuffer query = new StringBuffer();
			query.append("\n");
			query.append("SELECT count(*) AS cnt    \n");
			query.append("  FROM MM_alarm   \n");
			query.append("   WHERE insert_user_id = '" + insert_user_id + "'  \n");
			query.append(";\n");
			//
			pstmt = con.prepareStatement(query.toString());
			log.debug("[getalarm_count query] : " + query.toString());
			rs = pstmt.executeQuery();
			//
			if (rs.next()) {
				cnt = rs.getInt("cnt");
			}
		} catch (Exception ex) {
			log.error("[AlarmDAO.getalarm_count] " + ex.getMessage(), ex);
		} finally {
			if (rs != null)
				rs.close();
			if (pstmt != null)
				pstmt.close();
			super.closeConnection(con);
		}
		return cnt;
	}

	public List<Map<String, Object>> getAlarmCountByHour(Map<String, Object> params) throws Exception {

		List<Map<String, Object>> list = new ArrayList<Map<String, Object>>();

		Connection con = null;
		PreparedStatement pstmt = null;
		ResultSet rs = null;

		//
		try {

			String alarm_date_from = (String) params.get("alarm_date_from");
			String alarm_date_to = (String) params.get("alarm_date_to");
			String insert_user_id = (String) params.get("insert_user_id");

			//
			DateFormat format = new SimpleDateFormat("yyyy-MM-dd HH:mm");
			Date startdate = format.parse(alarm_date_from);
			Date enddate = format.parse(alarm_date_to);

			con = super.getConnection();

			List<Date> date_list = DateUtils.getBetweenDates(startdate, enddate);
			for (int i = 0; date_list != null && i < date_list.size(); i++) {
				Date date = date_list.get(i);
				//
				DateFormat dformat = new SimpleDateFormat("yyyy-MM-dd");
				//
				for (int j = 0; j < 23; j++) {
					StringBuffer query = new StringBuffer();
					query.append("SELECT count(*) AS cnt    ");
					query.append("  FROM MM_alarm   ");
					query.append("   WHERE insert_user_id = ? ");
					query.append("   AND alarm_date BETWEEN CAST ('" + dformat.format(date) + " " + formatHH(j) + ":00:00" + "' AS TIMESTAMP)  and CAST ('" + dformat.format(date) + " "
							+ formatHH(j + 1) + ":59:59" + "' AS TIMESTAMP) ");
					//
					pstmt = con.prepareStatement(query.toString());
					pstmt.setString(1, insert_user_id);
					rs = pstmt.executeQuery();
					//
					Map<String, Object> map = new HashMap<String, Object>();
					map.put("date", date);
					map.put("count", 0);
					if (rs.next()) {
						map.put("count", rs.getInt("cnt"));
						list.add(map);
					}
				}
			}

		} catch (Exception ex) {
			log.error("[AlarmDAO.getAlarmCountByHour] " + ex.getMessage(), ex);
		} finally {
			if (rs != null)
				rs.close();
			if (pstmt != null)
				pstmt.close();
			super.closeConnection(con);
		}
		return list;
	}

	public List<Map<String, Object>> getAlarmCountPriorityByHour(Map<String, Object> params) throws Exception {

		List<Map<String, Object>> list = new ArrayList<Map<String, Object>>();

		Connection con = null;
		PreparedStatement pstmt = null;
		ResultSet rs = null;

		//
		try {

			con = super.getConnection();

			String alarm_date_from = (String) params.get("alarm_date_from");
			String alarm_date_to = (String) params.get("alarm_date_to");
			String insert_user_id = (String) params.get("insert_user_id");

			//
			DateFormat format = new SimpleDateFormat("yyyy-MM-dd HH:mm");
			Date startdate = format.parse(alarm_date_from);
			Date enddate = format.parse(alarm_date_to);

			List<Date> date_list = DateUtils.getBetweenDates(startdate, enddate);
			for (int i = 0; date_list != null && i < date_list.size(); i++) {
				Date date = date_list.get(i);
				//
				DateFormat dformat = new SimpleDateFormat("yyyy-MM-dd");
				//
				StringBuffer query = new StringBuffer();
				query.append("SELECT priority, count(*) AS cnt    ");
				query.append("  FROM MM_alarm   ");
				query.append("   WHERE insert_user_id = ? ");
				query.append("   AND alarm_date BETWEEN CAST ('" + dformat.format(date) + " 00:00:00" + "' AS TIMESTAMP)  and CAST ('" + dformat.format(date) + " 23:59:59" + "' AS TIMESTAMP) ");
				query.append("   GROUP by priority ");
				//
				pstmt = con.prepareStatement(query.toString());
				pstmt.setString(1, insert_user_id);
				rs = pstmt.executeQuery();
				//
				Map<String, Object> map = new HashMap<String, Object>();
				DateFormat df = new SimpleDateFormat("yyyy-MM-dd (E)");
				// log.info(df.format(date));
				map.put("timestamp", df.format(date));
				map.put("INFO", 0);
				map.put("WARN", 0);
				map.put("ERROR", 0);
				while (rs.next()) {
					map.put(rs.getString("priority"), rs.getInt("cnt"));
				}
				list.add(map);

				if (rs != null) {
					rs.close();
				}
				if (pstmt != null)
					pstmt.close();
			}

		} catch (Exception ex) {
			log.error("[AlarmDAO.getAlarmCountPriorityByHour] " + ex.getMessage(), ex);
		} finally {
			super.closeConnection(con);
		}
		return list;
	}

	public int getAlarmCountByUnread(String insert_user_id) throws Exception {
		Connection con = null;
		PreparedStatement pstmt = null;
		ResultSet rs = null;
		int cnt = 0;
		//
		try {
			//
			con = super.getConnection();
			StringBuffer query = new StringBuffer();
			query.append("\n");
			query.append("SELECT count(*) AS cnt    \n");
			query.append("  FROM MM_alarm   \n");
			query.append("   WHERE insert_user_id = '" + insert_user_id + "'  \n");
			query.append("     AND is_read = 'N' \n");
			query.append(";\n");
			//
			pstmt = con.prepareStatement(query.toString());
			log.debug("[getAlarmCountByUnread query] : " + query.toString());
			rs = pstmt.executeQuery();
			//
			if (rs.next()) {
				cnt = rs.getInt("cnt");
			}

		} catch (Exception ex) {
			log.error("[AlarmDAO.getAlarmCountByUnread] " + ex.getMessage(), ex);
		} finally {
			if (rs != null)
				rs.close();
			if (pstmt != null)
				pstmt.close();
			super.closeConnection(con);
		}
		return cnt;
	}

	public int getAlarmCountByPriority(String alarm_date_from, String alarm_date_to, String insert_user_id, String priority) throws Exception {
		Connection con = null;
		PreparedStatement pstmt = null;
		ResultSet rs = null;
		int cnt = 0;
		//
		try {
			//
			con = super.getConnection();
			StringBuffer query = new StringBuffer();
			query.append("\n");
			query.append("SELECT count(*) AS cnt    \n");
			query.append("  FROM MM_alarm   \n");
			query.append("   WHERE insert_user_id = '" + insert_user_id + "'  \n");
			query.append("    AND alarm_date between CAST ('" + alarm_date_from + ":00" + "' AS TIMESTAMP)  and CAST ('" + alarm_date_to + ":59" + "' AS TIMESTAMP)    \n");
			query.append("    AND  priority = '" + priority + "'  \n");
			query.append(";\n");
			//
			pstmt = con.prepareStatement(query.toString());
			log.debug("[getalarm_count query] : " + query.toString());
			rs = pstmt.executeQuery();
			//
			if (rs.next()) {
				cnt = rs.getInt("cnt");
			}
		} catch (Exception ex) {
			log.error("[AlarmDAO.getAlarmCountByPriority] " + ex.getMessage(), ex);
		} finally {
			if (rs != null)
				rs.close();
			if (pstmt != null)
				pstmt.close();
			super.closeConnection(con);
		}
		return cnt;
	}

	public int getAlarmCountByPriority(String insert_user_id, String priority) throws Exception {
		Connection con = null;
		PreparedStatement pstmt = null;
		ResultSet rs = null;
		int cnt = 0;
		//
		try {
			//
			con = super.getConnection();
			StringBuffer query = new StringBuffer();
			query.append("\n");
			query.append("SELECT count(*) AS cnt    \n");
			query.append("  FROM MM_alarm   \n");
			query.append("   WHERE insert_user_id = '" + insert_user_id + "'  \n");
			query.append("    AND  priority = '" + priority + "'  \n");
			query.append(";\n");
			//
			pstmt = con.prepareStatement(query.toString());
			log.debug("[getalarm_count query] : " + query.toString());
			rs = pstmt.executeQuery();
			//
			if (rs.next()) {
				cnt = rs.getInt("cnt");
			}
		} catch (Exception ex) {
			log.error("[AlarmDAO.getAlarmCountByPriority] " + ex.getMessage(), ex);
		} finally {
			if (rs != null)
				rs.close();
			if (pstmt != null)
				pstmt.close();
			super.closeConnection(con);
		}
		return cnt;
	}

	public int getAlarmConfigCountByDate(String alarm_config_id, String insert_user_id, Date before_date, String priority) throws Exception {
		Connection con = null;
		PreparedStatement pstmt = null;
		ResultSet rs = null;
		int cnt = 0;
		//
		try {
			//
			con = super.getConnection();
			StringBuffer query = new StringBuffer();
			query.append("\n");
			query.append("SELECT COUNT(*) AS cnt    \n");
			query.append("  FROM MM_alarm   \n");
			query.append("   WHERE alarm_config_id = '" + alarm_config_id + "'  \n");
			query.append("    AND alarm_date between CAST ('" + DateUtils.format(before_date) + "' AS TIMESTAMP)  and NOW()    \n");
			query.append("    AND insert_user_id = '" + insert_user_id + "'  \n");
			query.append("    AND priority = '" + priority + "'  \n");
			query.append(";\n");
			//
			pstmt = con.prepareStatement(query.toString());
			log.debug("[getAlarmConfigCountByDate query] : " + query.toString());
			rs = pstmt.executeQuery();
			//
			if (rs.next()) {
				cnt = rs.getInt("cnt");
			}
		} catch (Exception ex) {
			log.error("[AlarmDAO.getAlarmCountByDate] " + ex.getMessage(), ex);
		} finally {
			if (rs != null)
				rs.close();
			if (pstmt != null)
				pstmt.close();
			super.closeConnection(con);
		}
		return cnt;
	}
	
	
	public Map<String, Object> getTagAlarmAggregation(String insert_user_id, String tag_id) throws Exception {
		Connection con = null;
		PreparedStatement pstmt = null;
		ResultSet rs = null;

		Map<String, Object> map = new HashMap<>();
		//
		try {
			//
			con = super.getConnection();
			StringBuffer query = new StringBuffer();
			query.append("SELECT COUNT(*) AS ALARM_COUNT_TOTAL, ");
			query.append("       NVL(SUM(ERROR_CNT), 0) AS ALARM_COUNT_ERROR, ");
			query.append("       NVL(SUM(WARN_CNT), 0) AS ALARM_COUNT_WARN, ");
			query.append("       NVL(SUM(INFO_CNT), 0) AS ALARM_COUNT_INFO ");
			query.append("  FROM ( ");
			query.append("         SELECT CASE WHEN a.PRIORITY = 'ERROR' THEN 1 ELSE 0 END AS ERROR_CNT, ");
			query.append("		          CASE WHEN a.PRIORITY = 'WARN' THEN 1 ELSE 0 END AS WARN_CNT, ");
			query.append("		          CASE WHEN a.PRIORITY = 'INFO' THEN 1 ELSE 0 END AS INFO_CNT ");
			query.append("		     FROM MM_ALARM a, ");
			query.append("		          MM_ALARM_CONFIG b ");
			query.append("		    WHERE a.ALARM_CONFIG_ID = b.ALARM_CONFIG_ID ");
			query.append("		      AND a.INSERT_USER_ID = ? ");
			query.append("		      AND b.TAG_ID = ?   ");
			query.append("       ) ");
			//
			pstmt = con.prepareStatement(query.toString());
			pstmt.setString(1, insert_user_id);
			pstmt.setString(2, tag_id);
			//log.info("[SQL] : " + query.toString());
			//log.info("[PARAMETERS] : " + insert_user_id + ", " + tag_id);
			rs = pstmt.executeQuery();
			//
			while (rs.next()) {
				map.put("alarm_count_total", rs.getLong("alarm_count_total"));
				map.put("alarm_count_error", rs.getLong("alarm_count_error"));
				map.put("alarm_count_warn", rs.getLong("alarm_count_warn"));
				map.put("alarm_count_info", rs.getLong("alarm_count_info"));
				//
			}
		} catch (Exception ex) {
			log.error("Tag Alarm Aggregation select failed. [MESSAGE]=" + ex.getMessage(), ex);
		} finally {
			if (rs != null)
				rs.close();
			if (pstmt != null)
				pstmt.close();
			super.closeConnection(con);
		}
		return map;
	}
	
	public Map<String, Object> getTagAlarmStatistics(String insert_user_id, String tag_id, String conditionQuery) throws Exception {
		Connection con = null;
		PreparedStatement pstmt = null;
		ResultSet rs = null;
		Map<String, Object> map = new HashMap<>();
		//
		try {
			//
			con = super.getConnection();
			StringBuffer query = new StringBuffer();
			query.append("SELECT COUNT(*) AS ALARM_TOTAL_COUNT, ");
			query.append("       NVL(SUM(TAG_CNT), 0) AS ALARM_TAG_COUNT, ");
			query.append("       ( ");
			query.append("         SELECT MAX(a2.insert_date) ");
			query.append("		     FROM MM_ALARM a2, ");
			query.append("		          MM_ALARM_CONFIG b2 ");
			query.append("		    WHERE a2.ALARM_CONFIG_ID = b2.ALARM_CONFIG_ID ");
			query.append("		      AND b2.TAG_ID = ? ");
			query.append("		      AND a2.INSERT_USER_ID = ? ");
			query.append("	     ) AS LAST_ALARM_DT ");
			query.append("  FROM ( ");
			query.append("         SELECT CASE WHEN b.TAG_ID = ? THEN 1 ELSE 0 END AS TAG_CNT ");
			query.append("		     FROM MM_ALARM a, ");
			query.append("		          MM_ALARM_CONFIG b ");
			query.append("		    WHERE a.ALARM_CONFIG_ID = b.ALARM_CONFIG_ID ");
			query.append("		      AND a.INSERT_USER_ID = ? ");
			if (StringUtils.isNotEmpty(conditionQuery)) {
				query.append(conditionQuery);
			}
			query.append("       ) ");
			//
			pstmt = con.prepareStatement(query.toString());
			pstmt.setString(1, tag_id);
			pstmt.setString(2, insert_user_id);
			pstmt.setString(3, tag_id);
			pstmt.setString(4, insert_user_id);
			//log.info("[SQL] : " + query.toString());
			//log.info("[PARAMETERS] : " + tag_id + ", " + insert_user_id);
			rs = pstmt.executeQuery();
			//
			while (rs.next()) {
				map.put("alarm_total_count", rs.getLong("alarm_total_count"));
				map.put("alarm_tag_count", rs.getLong("alarm_tag_count"));
				map.put("last_alarm_dt", rs.getString("last_alarm_dt"));
				//
			}
		} catch (Exception ex) {
			log.error("Tag Alarm info select failed. [MESSAGE]=" + ex.getMessage(), ex);
		} finally {
			if (rs != null)
				rs.close();
			if (pstmt != null)
				pstmt.close();
			super.closeConnection(con);
		}
		return map;
	}

	/**
	 * getNextAlarmSeq()
	 * @return
	 * @throws Exception
	 */
	public synchronized long getNextAlarmSeq() throws Exception { 
		
		long alarm_seq = 0;
		
		Connection con = null;
		PreparedStatement pstmt = null;
		ResultSet rs = null;

		try {
			//
			con = super.getConnection();
			StringBuffer query = new StringBuffer();
			query.append("\n");
			query.append("SELECT NEXT VALUE FOR ALARM_SEQ FROM DUAL             \n");
			query.append(";\n");
			//
			log.debug("[getNextAlarmSeq query] : " + query.toString());
			
			pstmt = con.prepareStatement(query.toString());
			rs = pstmt.executeQuery();
			if(rs.next()){
				alarm_seq = rs.getLong(1);
			}
			
		} catch (Exception ex) {
			log.error("[AlarmDAO.getNextAlarmSeq] " + ex.getMessage(), ex);
		} finally {
			if (rs != null)
				rs.close();
			if (pstmt != null)
				pstmt.close();
			super.closeConnection(con);
		} 
		return alarm_seq;
		
	} 
	
	/**
	 * Save alarm
	 * 
	 * @param alarm
	 * @throws Exception
	 */
	public void saveAlarm(Alarm alarm) throws Exception {
		Connection con = null;
		PreparedStatement pstmt = null;
		ResultSet rs = null;

		try {
			//
			con = super.getConnection();
			StringBuffer query = new StringBuffer();
			query.append("\n");
			query.append("INSERT INTO MM_ALARM               \n");
			query.append("       (                                   \n");
			query.append("         alarm_seq,                        \n");
			query.append("         alarm_config_id,                  \n");
			query.append("         priority,                         \n");
			query.append("         alarm_date,                       \n");
			query.append("         description,                      \n");
			query.append("         insert_user_id,                   \n");
			query.append("         insert_date,                      \n");
			query.append("         is_read                           \n");
			query.append("       ) values (                          \n");
			query.append("         ?,   \n");
			query.append("         ?,   \n");
			query.append("         ?,   \n");
			query.append("         ?,   \n");
			query.append("         ?,   \n");
			query.append("         ?,   \n");
			query.append("         SYSDATE,                               \n");
			query.append("         'N'                                   \n");
			query.append("       )                                        \n");
			query.append(";\n");
			//
			log.debug("[saveAlarm query] : " + query.toString());
			pstmt = con.prepareStatement(query.toString());
			pstmt.setLong(1, alarm.getAlarm_seq());
			pstmt.setString(2, alarm.getAlarm_config_id());
			pstmt.setString(3, alarm.getPriority());
			pstmt.setTimestamp(4, new Timestamp(alarm.getAlarm_date().getTime()));
			pstmt.setString(5,  alarm.getDescription());
			pstmt.setString(6,  alarm.getInsert_user_id());
			pstmt.execute();

		} catch (Exception ex) {
			log.error("[AlarmDAO.saveAlarm] " + ex.getMessage(), ex);
		} finally {
			if (rs != null)
				rs.close();
			if (pstmt != null)
				pstmt.close();
			super.closeConnection(con);
		}
	}

	/**
	 * Save alarm
	 * 
	 * @param alarm
	 * @throws Exception
	 */
	public void updateReadAllAlarm(String insert_user_id) throws Exception {
		Connection con = null;
		PreparedStatement pstmt = null;
		ResultSet rs = null;

		try {
			//
			con = super.getConnection();
			StringBuffer query = new StringBuffer();
			query.append("UPDATE MM_ALARM \n");
			query.append("   SET IS_READ = 'Y', \n ");
			query.append("       READ_DATE = SYSDATE \n ");
			query.append(" WHERE INSERT_USER_ID = '" + insert_user_id + "'    \n");
			query.append("   AND IS_READ = 'N'    \n");
			//
			log.debug("[updateReadAlarm query] : " + query.toString());
			pstmt = con.prepareStatement(query.toString());
			pstmt.execute();

		} catch (Exception ex) {
			log.error("[AlarmDAO.updateReadAllAlarm] " + ex.getMessage(), ex);
		} finally {
			if (rs != null)
				rs.close();
			if (pstmt != null)
				pstmt.close();
			super.closeConnection(con);
		}
	}
	
	public void updateReadAlarm(long alarm_seq, String insert_user_id) throws Exception {
		Connection con = null;
		PreparedStatement pstmt = null;
		ResultSet rs = null;

		try {
			//
			con = super.getConnection();
			StringBuffer query = new StringBuffer();
			query.append("UPDATE MM_ALARM \n");
			query.append("   SET IS_READ = 'Y', \n ");
			query.append("       READ_DATE = SYSDATE \n ");
			query.append(" WHERE INSERT_USER_ID = '" + insert_user_id + "'    \n");
			query.append("   AND ALARM_SEQ = '" + alarm_seq + "'    \n");
			query.append("   AND IS_READ = 'N'    \n");
			//
			log.debug("[updateReadAlarm query] : " + query.toString());
			pstmt = con.prepareStatement(query.toString());
			pstmt.execute();

		} catch (Exception ex) {
			log.error("[AlarmDAO.updateReadAllAlarm] " + ex.getMessage(), ex);
		} finally {
			if (rs != null)
				rs.close();
			if (pstmt != null)
				pstmt.close();
			super.closeConnection(con);
		}
	}

	/**
	 * Save alarm
	 * 
	 * @param alarm
	 * @throws Exception
	 */
	public void deleteAlarm(long alarm_seq, String insert_user_id) throws Exception {
		Connection con = null;
		PreparedStatement pstmt = null;
		ResultSet rs = null;

		try {
			//
			con = super.getConnection();
			StringBuffer query = new StringBuffer();
			query.append("\n");
			query.append("DELETE FROM MM_ALARM               \n");
			query.append("  WHERE ALARM_SEQ = " + alarm_seq + "     \n");
			query.append("    AND INSERT_USER_ID = '" + insert_user_id + "'     \n");
			//
			log.debug("[deleteAlarm query] : " + query.toString());
			pstmt = con.prepareStatement(query.toString());
			pstmt.execute();

		} catch (Exception ex) {
			log.error("[AlarmDAO.deleteAlarm] " + ex.getMessage(), ex);
		} finally {
			if (rs != null)
				rs.close();
			if (pstmt != null)
				pstmt.close();
			super.closeConnection(con);
		}
	}

	/**
	 * Save alarm
	 * 
	 * @param alarm
	 * @throws Exception
	 */
	public void deleteAllAlarm(String insert_user_id) throws Exception {
		Connection con = null;
		PreparedStatement pstmt = null;
		ResultSet rs = null;

		try {
			//
			con = super.getConnection();
			StringBuffer query = new StringBuffer();
			query.append("\n");
			query.append("DELETE FROM MM_ALARM               \n");
			query.append("  WHERE INSERT_USER_ID = '" + insert_user_id + "'     \n");
			//
			log.debug("[deleteAllAlarm query] : " + query.toString());
			pstmt = con.prepareStatement(query.toString());
			pstmt.execute();

		} catch (Exception ex) {
			log.error("[AlarmDAO.deleteAlarm] " + ex.getMessage(), ex);
		} finally {
			if (rs != null)
				rs.close();
			if (pstmt != null)
				pstmt.close();
			super.closeConnection(con);
		}
	}

	public void clear() throws Exception {
		Connection con = null;
		PreparedStatement pstmt = null;
		ResultSet rs = null;

		try {
			//
			con = super.getConnection();
			StringBuffer query = new StringBuffer();
			query.append("\n");
			query.append("DELETE FROM MM_ALARM               \n");
			//
			log.info("[Alarm table clear] : " + query.toString());
			pstmt = con.prepareStatement(query.toString());
			pstmt.execute();

		} catch (Exception ex) {
			log.error("[AlarmDAO.clear] " + ex.getMessage(), ex);
		} finally {
			if (rs != null)
				rs.close();
			if (pstmt != null)
				pstmt.close();
			super.closeConnection(con);
		}
	}

	private String formatHH(int hh) {
		if (hh < 10) {
			return "0" + hh;
		}
		return "" + hh;
	}

}
