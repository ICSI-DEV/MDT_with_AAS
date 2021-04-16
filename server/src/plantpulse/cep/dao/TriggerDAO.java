package plantpulse.cep.dao;

import java.sql.Connection;
import java.util.List;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import plantpulse.cep.domain.Trigger;
import plantpulse.cep.domain.TriggerAttribute;
import plantpulse.dbutils.QueryRunner;
import plantpulse.dbutils.handlers.BeanHandler;
import plantpulse.dbutils.handlers.BeanListHandler;
import plantpulse.dbutils.handlers.ScalarHandler;

public class TriggerDAO extends CommonDAO {

	private static final Log log = LogFactory.getLog(TriggerDAO.class);

	// TRIGGER
	public List<Trigger> getTriggerList() throws Exception {

		Connection connection = null;
		List<Trigger> list = null;
		//
		try {
			//
			StringBuffer query = new StringBuffer();
			query.append("SELECT  * ");
			query.append("  FROM MM_TRIGGER                       ");
			query.append(" ORDER BY NVL(LAST_UPDATE_DATE, INSERT_DATE) DESC ");

			QueryRunner run = new QueryRunner();
			connection = super.getConnection();
			list = (List<Trigger>) run.query(connection, query.toString(), new BeanListHandler(Trigger.class), new Object[] {});

			// append attribute
			for (int i = 0; list != null && i < list.size(); i++) {
				Trigger trigger = list.get(i);
				query = new StringBuffer();
				query.append("SELECT  * ");
				query.append("  FROM MM_TRIGGER_ATTRIBUTES                       ");
				query.append("  WHERE TRIGGER_ID = ?                       ");
				query.append(" ORDER BY SORT_ORDER ASC ");

				run = new QueryRunner();
				List<TriggerAttribute> attr_list = (List<TriggerAttribute>) run.query(connection, query.toString(), new BeanListHandler(TriggerAttribute.class),
						new Object[] { trigger.getTrigger_id() });

				TriggerAttribute[] trigger_attributes = attr_list.toArray(new TriggerAttribute[attr_list.size()]);
				list.get(i).setTrigger_attributes(trigger_attributes);
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

	public Trigger getTrigger(String trigger_id) throws Exception {

		Connection connection = null;
		Trigger one = null;
		//
		try {

			StringBuffer query = new StringBuffer();
			query.append("SELECT  * ");
			query.append("  FROM MM_TRIGGER                       ");
			query.append(" WHERE TRIGGER_ID = ?       \n");
			query.append(" ORDER BY NVL(LAST_UPDATE_DATE, INSERT_DATE) DESC ");

			QueryRunner run = new QueryRunner();

			connection = super.getConnection();

			one = (Trigger) run.query(connection, query.toString(), new BeanHandler(Trigger.class), new Object[] { trigger_id });

			// apend attributes
			query = new StringBuffer();
			query.append("SELECT  * ");
			query.append("  FROM MM_TRIGGER_ATTRIBUTES                       ");
			query.append("  WHERE TRIGGER_ID = ?                       ");
			query.append(" ORDER BY SORT_ORDER ASC ");

			run = new QueryRunner();
			List<TriggerAttribute> attr_list = (List<TriggerAttribute>) run.query(connection, query.toString(), new BeanListHandler(TriggerAttribute.class), new Object[] { trigger_id });

			TriggerAttribute[] trigger_attributes = attr_list.toArray(new TriggerAttribute[attr_list.size()]);
			one.setTrigger_attributes(trigger_attributes);

			//

			//
		} catch (Exception ex) {
			log.error(ex);
			throw ex;
		} finally {
			super.closeConnection(connection);
		}
		return one;
	}

	public void saveTrigger(Trigger trigger) throws Exception {
		Connection connection = null;

		//
		try {
			connection = super.getConnection();
			connection.setAutoCommit(false);

			QueryRunner run = new QueryRunner();

			// 1. 마스터 저장
			// Get MAX TRIGGER_ID
			StringBuffer query = new StringBuffer();
			query.append("SELECT 'TRIGGER_' || LPAD(NEXT VALUE FOR TRIGGER_SEQ, 5, '0') AS TRIGGER_ID FROM DUAL   \n");
			String trigger_id = run.query(connection, query.toString(), new ScalarHandler<String>(), new Object[] {});
			trigger.setTrigger_id(trigger_id);

			//
			query = new StringBuffer();
			query.append("\n");
			query.append("INSERT INTO MM_TRIGGER                             \n");
			query.append("       (                                            \n");
			query.append("         TRIGGER_ID,                                \n");
			query.append("         TRIGGER_NAME,                              \n");
			query.append("         TRIGGER_DESC,                              \n");
			query.append("         EPL,                                       \n");
			query.append("         STATUS,                                    \n");
			query.append("         USE_MQ,                                    \n");
			query.append("         MQ_PROTOCOL,                            \n");
			query.append("         MQ_DESTINATION,                            \n");
			query.append("         USE_STORAGE,                               \n");
			query.append("         STORAGE_TABLE,                             \n");
			query.append("         STORAGE_TABLE_PRIMARY_KEYS,                \n");
			query.append("         STORAGE_TABLE_CLUSTERING_ORDER_BY_KEYS,     \n");
			query.append("         STORAGE_TABLE_IDX_KEYS,                     \n");
			query.append("         INSERT_DATE                                \n");
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
			query.append("         ?,        \n");
			query.append("         ?,        \n");
			query.append("         ?,        \n");
			query.append("         SYSDATE   \n");
			query.append("       )           \n");
			query.append(";\n");

			run.update(connection, query.toString(), new Object[] { trigger.getTrigger_id(), trigger.getTrigger_name(), trigger.getTrigger_desc(), trigger.getEpl(), trigger.getStatus(),
					trigger.isUse_mq(), trigger.getMq_protocol(), trigger.getMq_destination(), trigger.isUse_storage(), trigger.getStorage_table(), trigger.getStorage_table_primary_keys(), trigger.getStorage_table_clustering_order_by_keys(),
					trigger.getStorage_table_idx_keys()});

			// 2. 아트리뷰트 저장
			for (int i = 0; trigger.getTrigger_attributes() != null && i < trigger.getTrigger_attributes().length; i++) {
				TriggerAttribute attribute = trigger.getTrigger_attributes()[i];
				query = new StringBuffer();
				query.append("\n");
				query.append("INSERT INTO MM_TRIGGER_ATTRIBUTES                    \n");
				query.append("       (                                            \n");
				query.append("         TRIGGER_ID,                            \n");
				query.append("         FIELD_NAME,                            \n");
				query.append("         FIELD_DESC,                            \n");
				query.append("         JAVA_TYPE,                                \n");
				query.append("         SORT_ORDER                                 \n");
				query.append("       ) values (                                   \n");
				query.append("         ?,        \n");
				query.append("         ?,        \n");
				query.append("         ?,        \n");
				query.append("         ?,        \n");
				query.append("         ?        \n");
				query.append("       )           \n");
				query.append(";\n");

				run.update(connection, query.toString(),
						new Object[] { trigger.getTrigger_id(), attribute.getField_name(), attribute.getField_desc(), attribute.getJava_type(), attribute.getSort_order() });

			}
			;
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

	public void updateTrigger(Trigger trigger) throws Exception {

		Connection connection = null;

		try {

			connection = super.getConnection();

			//
			StringBuffer query = new StringBuffer();
			query.append("UPDATE MM_TRIGGER                  \n");
			query.append("   SET TRIGGER_NAME = ? ,                 \n");
			query.append("       TRIGGER_DESC = ?  ,                  \n");
			query.append("       EPL = ?  ,                  \n");
			query.append("       STATUS = ?  ,                  \n");
			query.append("       USE_MQ = ?  ,                  \n");
			query.append("       MQ_PROTOCOL = ?  ,                  \n");
			query.append("       MQ_DESTINATION = ?  ,                  \n");
			query.append("       USE_STORAGE = ?  ,                  \n");
			query.append("       STORAGE_TABLE = ?  ,                  \n");
			query.append("       STORAGE_TABLE_PRIMARY_KEYS = ?  ,                  \n");
			query.append("       STORAGE_TABLE_CLUSTERING_ORDER_BY_KEYS = ?,     \n");
			query.append("       STORAGE_TABLE_IDX_KEYS = ?,                     \n");
			query.append("       LAST_UPDATE_DATE = SYSDATE        \n");
			query.append(" WHERE TRIGGER_ID = ?   \n");
			//
			QueryRunner run = new QueryRunner();
			run.update(connection, query.toString(), new Object[] { trigger.getTrigger_name(), trigger.getTrigger_desc(), trigger.getEpl(), trigger.getStatus(), trigger.isUse_mq(),
					trigger.getMq_protocol(), trigger.getMq_destination(), 
					trigger.isUse_storage(), 
					trigger.getStorage_table(), trigger.getStorage_table_primary_keys(), trigger.getStorage_table_clustering_order_by_keys(), trigger.getStorage_table_idx_keys(), trigger.getTrigger_id() });

			// 2. 아트리뷰트 저장
			query = new StringBuffer();
			query.append("DELETE FROM MM_TRIGGER_ATTRIBUTES WHERE TRIGGER_ID = ?  \n");
			run.update(connection, query.toString(), new Object[] { trigger.getTrigger_id() });

			for (int i = 0; trigger.getTrigger_attributes() != null && i < trigger.getTrigger_attributes().length; i++) {
				TriggerAttribute attribute = trigger.getTrigger_attributes()[i];
				query = new StringBuffer();
				query.append("\n");
				query.append("INSERT INTO MM_TRIGGER_ATTRIBUTES                    \n");
				query.append("       (                                            \n");
				query.append("         TRIGGER_ID,                            \n");
				query.append("         FIELD_NAME,                            \n");
				query.append("         FIELD_DESC,                            \n");
				query.append("         JAVA_TYPE,                                \n");
				query.append("         SORT_ORDER                                 \n");
				query.append("       ) values (                                   \n");
				query.append("         ?,        \n");
				query.append("         ?,        \n");
				query.append("         ?,        \n");
				query.append("         ?,        \n");
				query.append("         ?        \n");
				query.append("       )           \n");
				query.append(";\n");

				run.update(connection, query.toString(),
						new Object[] { trigger.getTrigger_id(), attribute.getField_name(), attribute.getField_desc(), attribute.getJava_type(), attribute.getSort_order() });

			}
			;

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

	public void deleteTrigger(String trigger_id) throws Exception {

		Connection connection = null;

		try {

			connection = super.getConnection();
			connection.setAutoCommit(false);

			//
			StringBuffer query = new StringBuffer();
			query.append("DELETE FROM MM_TRIGGER WHERE TRIGGER_ID = ? \n");
			//
			QueryRunner run = new QueryRunner();
			run.update(connection, query.toString(), new Object[] { trigger_id });

			// 2. 아트리뷰트 저장
			query = new StringBuffer();
			query.append("DELETE FROM MM_TRIGGER_ATTRIBUTES WHERE TRIGGER_ID = ?  \n");
			run.update(connection, query.toString(), new Object[] { trigger_id });

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