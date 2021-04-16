package plantpulse.cep.dao;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.List;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import plantpulse.dbutils.QueryRunner;
import plantpulse.dbutils.handlers.BeanHandler;
import plantpulse.dbutils.handlers.BeanListHandler;
import plantpulse.dbutils.handlers.ScalarHandler;
import plantpulse.domain.AlarmConfig;

public class AlarmConfigDAO extends CommonDAO {

	private static final Log log = LogFactory.getLog(AlarmConfigDAO.class);

	public List<AlarmConfig> getAlarmConfigListAll() throws Exception {
		Connection connection = null;
		List<AlarmConfig> list = null;
		//
		try {
			//
			StringBuffer query = new StringBuffer();
			query.append("SELECT  * ");
			query.append("  FROM MM_ALARM_CONFIG                       ");
			query.append(" ORDER BY NVL(LAST_UPDATE_DATE, INSERT_DATE) DESC ");

			QueryRunner run = new QueryRunner();
			connection = super.getConnection();
			list = (List<AlarmConfig>) run.query(connection, query.toString(), new BeanListHandler<AlarmConfig>(AlarmConfig.class), new Object[] {});

			//log.debug(list.toString());

			//
		} catch (Exception ex) {
			log.error(ex);
			throw ex;
		} finally {
			super.closeConnection(connection);
		}
		return list;
	}
	
	

	public List<AlarmConfig> getAlarmConfigListByTagId(String tag_id) throws Exception {
		Connection connection = null;
		List<AlarmConfig> list = null;
		//
		try {
			//
			StringBuffer query = new StringBuffer();
			query.append("SELECT  * ");
			query.append("  FROM MM_ALARM_CONFIG                       ");
			query.append(" WHERE TAG_ID = ? ");

			QueryRunner run = new QueryRunner();
			connection = super.getConnection();
			list = (List<AlarmConfig>) run.query(connection, query.toString(), new BeanListHandler<AlarmConfig>(AlarmConfig.class), new Object[] { tag_id });

			//log.debug(list.toString());

			//
		} catch (Exception ex) {
			log.error(ex);
			throw ex;
		} finally {
			super.closeConnection(connection);
		}
		return list;
	};

	

	public List<AlarmConfig> getAlarmConfigListByInsertUserId(String insert_user_id) throws Exception {
		Connection connection = null;
		List<AlarmConfig> list = null;
		//
		try {
			//
			StringBuffer query = new StringBuffer();
			query.append("SELECT  * ");
			query.append("  FROM MM_ALARM_CONFIG                       ");
			query.append("  WHERE INSERT_USER_ID = ?                       ");
			query.append("    AND ALARM_TYPE = 'DEFAULT'                     ");
			query.append(" ORDER BY NVL(LAST_UPDATE_DATE, INSERT_DATE) DESC ");

			QueryRunner run = new QueryRunner();
			connection = super.getConnection();
			list = (List<AlarmConfig>) run.query(connection, query.toString(), new BeanListHandler<AlarmConfig>(AlarmConfig.class), new Object[] { insert_user_id });

			//log.debug(list.toString());

			//
		} catch (Exception ex) {
			log.error(ex);
			throw ex;
		} finally {
			super.closeConnection(connection);
		}
		return list;
	};
	
	
	

	public AlarmConfig getAlarmConfig(String alarm_config_id) throws Exception {
		Connection connection = null;
		AlarmConfig one = null;
		//
		try {

			StringBuffer query = new StringBuffer();
			query.append("SELECT  * ");
			query.append("  FROM MM_ALARM_CONFIG                       ");
			query.append(" WHERE ALARM_CONFIG_ID = ?       \n");
			query.append(" ORDER BY NVL(LAST_UPDATE_DATE, INSERT_DATE) DESC ");

			QueryRunner run = new QueryRunner();
			connection = super.getConnection();
			one = (AlarmConfig) run.query(connection, query.toString(), new BeanHandler<AlarmConfig>(AlarmConfig.class), new Object[] { alarm_config_id });

			//
		} catch (Exception ex) {
			log.error(ex);
			throw ex;
		} finally {
			super.closeConnection(connection);
		}
		return one;
	}

	public void saveAlarmConfig(AlarmConfig alarm_config) throws Exception {
		Connection connection = null;
		//
		try {

			connection = super.getConnection();
			connection.setAutoCommit(false);

			QueryRunner run = new QueryRunner();

			// 1. 마스터 저장
			StringBuffer query = new StringBuffer();
			query.append("\n");
			query.append("INSERT INTO MM_ALARM_CONFIG                   \n");
			query.append("       (                                            \n");
			query.append("         ALARM_CONFIG_ID,                            \n");
			query.append("         ALARM_CONFIG_NAME,                            \n");
			query.append("         ALARM_CONFIG_PRIORITY,                            \n");
			query.append("         ALARM_CONFIG_DESC,                            \n");
			query.append("         TAG_ID,                                     \n");
			query.append("         EPL,                                         \n");
			query.append("         CONDITION,                                 \n");
			query.append("         MESSAGE,                                 \n");
			query.append("         SEND_EMAIL,                                \n");
			query.append("         INSERT_USER_ID,                            \n");
			query.append("         INSERT_DATE,                                \n");
			query.append("         ALARM_TYPE,                                \n");
			query.append("         RECIEVE_ME,                                \n");
			query.append("         RECIEVE_OTHERS,                                \n");
			query.append("         DUPLICATE_CHECK,                                \n");
			query.append("         DUPLICATE_CHECK_TIME                                \n");
			query.append("       ) values (                                   \n");
			query.append("         ?,        \n");
			query.append("         ?,        \n");
			query.append("         ?,        \n");
			query.append("         ?,        \n");
			query.append("         ?,        \n");
			query.append("         ?,        \n");
			query.append("         ?,        \n");
			query.append("         ?,        \n");
			query.append("         ?,        \n");
			query.append("         ?,        \n");
			query.append("         SYSDATE,  \n");
			query.append("         ?,        \n");
			query.append("         ?,        \n");
			query.append("         ?,        \n");
			query.append("         ?,        \n");
			query.append("         ?         \n");
			query.append("       )           \n");
			query.append(";\n");

			run.update(connection, query.toString(),
					new Object[] { alarm_config.getAlarm_config_id(), alarm_config.getAlarm_config_name(), alarm_config.getAlarm_config_priority(), alarm_config.getAlarm_config_desc(),
							alarm_config.getTag_id(), alarm_config.getEpl(), alarm_config.getCondition(), alarm_config.getMessage(), alarm_config.isSend_email(), alarm_config.getInsert_user_id(),
							alarm_config.getAlarm_type(), alarm_config.isRecieve_me(), alarm_config.getRecieve_others(), alarm_config.isDuplicate_check(), alarm_config.getDuplicate_check_time() });

			//log.debug("query.toString() : " + query.toString());
			//log.debug("alarm_config : " + alarm_config);

			connection.commit();
			
			super.getDomainChangeEventBus().onAlarmConfigChanged(alarm_config.getAlarm_config_id());
			
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

	public void updateAlarmConfig(AlarmConfig alarm_config) throws Exception {
		Connection connection = null;
		try {
			connection = super.getConnection();
			connection.setAutoCommit(false);
			//
			StringBuffer query = new StringBuffer();
			query.append("UPDATE MM_ALARM_CONFIG                  \n");
			query.append("   SET ALARM_CONFIG_NAME = ? ,                 \n");
			query.append("       ALARM_CONFIG_PRIORITY = ?  ,                  \n");
			query.append("       ALARM_CONFIG_DESC = ?  ,                  \n");
			query.append("       TAG_ID = ?  ,                  \n");
			query.append("       EPL = ?  ,                                         \n");
			query.append("       CONDITION = ?  ,                                \n");
			query.append("       MESSAGE = ?  ,                                \n");
			query.append("       SEND_EMAIL = ?  ,                              \n");
			query.append("       SEND_SMS = ?  ,                              \n");
			query.append("       RECIEVE_ME = ?  ,                              \n");
			query.append("       RECIEVE_OTHERS = ?  ,                              \n");
			query.append("       DUPLICATE_CHECK = ?  ,                              \n");
			query.append("       DUPLICATE_CHECK_TIME = ?  ,                              \n");
			query.append("       LAST_UPDATE_DATE = SYSDATE        \n");
			query.append(" WHERE ALARM_CONFIG_ID = ?   \n");
			//
			QueryRunner run = new QueryRunner();
			run.update(connection, query.toString(),
					new Object[] { alarm_config.getAlarm_config_name(), alarm_config.getAlarm_config_priority(), alarm_config.getAlarm_config_desc(), alarm_config.getTag_id(), alarm_config.getEpl(),
							alarm_config.getCondition(), alarm_config.getMessage(), alarm_config.isSend_email(), alarm_config.isSend_sms(), alarm_config.isRecieve_me(),
							alarm_config.getRecieve_others(), alarm_config.isDuplicate_check(), alarm_config.getDuplicate_check_time(), alarm_config.getAlarm_config_id() });

			//log.debug("query.toString() : " + query.toString());
			//log.debug("alarm_config : " + alarm_config.toString());

			connection.commit();
			
			super.getDomainChangeEventBus().onAlarmConfigChanged(alarm_config.getAlarm_config_id());
			
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

	public void deleteAlarmConfig(String alarm_config_id) throws Exception {
		Connection connection = null;
		try {
			connection = super.getConnection();
			connection.setAutoCommit(false);

			//
			StringBuffer query = new StringBuffer();
			query.append("DELETE FROM MM_ALARM_CONFIG WHERE ALARM_CONFIG_ID = ? \n");
			//
			QueryRunner run = new QueryRunner();
			run.update(connection, query.toString(), new Object[] { alarm_config_id });

			connection.commit();
			
			super.getDomainChangeEventBus().onAlarmConfigChanged(alarm_config_id);
			
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

	public String selectAlarmConfigIdById(String alarm_config_id) {
		String alarmConfigId = "";
		Connection connection = null;
		try {
			//
			StringBuffer query = new StringBuffer();
			query.append("SELECT MAX(ALARM_CONFIG_ID) AS ALARM_CONFIG_ID FROM MM_ALARM_CONFIG WHERE ALARM_CONFIG_ID = ? \n");
			//
			QueryRunner run = new QueryRunner();
			connection = super.getConnection();
			alarmConfigId = run.query(connection, query.toString(), new ScalarHandler<String>("alarm_config_id"), new Object[] { alarm_config_id });
			//
			return alarmConfigId;
		} catch (Exception ex) {
			log.error(ex);
			return alarmConfigId;
		} finally {
			try {
				super.closeConnection(connection);
			} catch (SQLException e) {
				log.error(e);
			}
		}
	}

}
