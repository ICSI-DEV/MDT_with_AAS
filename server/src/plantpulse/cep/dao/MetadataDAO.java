package plantpulse.cep.dao;

import java.sql.Connection;
import java.util.List;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.springframework.stereotype.Repository;

import plantpulse.dbutils.QueryRunner;
import plantpulse.dbutils.handlers.BeanHandler;
import plantpulse.dbutils.handlers.BeanListHandler;
import plantpulse.domain.Metadata;

@Repository
public class MetadataDAO  extends CommonDAO {

	private static final Log log = LogFactory.getLog(MetadataDAO.class);
	

	public List<Metadata> selectMetadataList() throws Exception {
		Connection connection = null;
		List<Metadata> list = null;
		//
		try {
			//
			StringBuffer query = new StringBuffer();

			query.append("SELECT *   \n");
			query.append("  FROM MM_METADATA    \n");
			query.append(" ORDER BY OBJECT_ID   \n");

			log.debug("\n query.toString() : {\n" + query.toString() + "}");

			QueryRunner run = new QueryRunner();
			connection = super.getConnection();
			list = run.query(connection, query.toString(), new BeanListHandler<Metadata>(Metadata.class), new Object[] {  });

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

	public List<Metadata> selectMetadataList(String object_id) throws Exception {
		Connection connection = null;
		List<Metadata> list = null;
		//
		try {
			//
			StringBuffer query = new StringBuffer();

			query.append("SELECT *   \n");
			query.append("  FROM MM_METADATA    \n");
			query.append("  WHERE  OBJECT_ID = ?    \n");
			query.append(" ORDER BY KEY   \n");

			log.debug("\n query.toString() : {\n" + query.toString() + "}");

			QueryRunner run = new QueryRunner();
			connection = super.getConnection();
			list = run.query(connection, query.toString(), new BeanListHandler<Metadata>(Metadata.class), new Object[] { object_id });

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

	public Metadata selectMetadata(String object_id, String key) throws Exception {
		Connection connection = null;
		Metadata metadata = new Metadata();
		//
		try {
			//
			StringBuffer query = new StringBuffer();
			query.append("SELECT *    \n");
			query.append("  FROM MM_METADATA   \n");
			query.append(" WHERE OBJECT_ID = ?   \n");
			query.append("   AND KEY = ?   \n");

			log.debug("\n query.toString() : {\n" + query.toString() + "}");

			QueryRunner run = new QueryRunner();
			connection = super.getConnection();
			metadata = run.query(connection, query.toString(), new BeanHandler<Metadata>(Metadata.class), new Object[] { object_id, key });

			log.debug(metadata.toString());
			//
		} catch (Exception ex) {
			log.error("Select metadata Exception : " + ex.getMessage());
			throw ex;
		} finally {
			super.closeConnection(connection);
		}

		return metadata;
	}

	

	public void insertMetadataList(List<Metadata> metadata_list) throws Exception {
		//
		Connection connection = null;
		//
		try {
			connection = super.getConnection();
			
			if(metadata_list == null || metadata_list.size() == 0 ) {
				return;
			}
	
			for(int i=0; i < metadata_list.size(); i++) {
				//
				Metadata metadata = metadata_list.get(i);
				//if metadata.getKey(), metadata.getValue(), is not null
				//
				StringBuffer query = new StringBuffer();
				query.append("INSERT INTO MM_METADATA    \n");
				query.append("	     (                \n");
				query.append("	       OBJECT_ID,       \n");
				query.append("	       KEY,     \n");
				query.append("	       VALUE,    \n");
				query.append("	       DESCRIPTION,    \n");
				query.append("	       INSERT_DATE    \n");
				query.append("	     )    \n");
				query.append("VALUES     \n");
				query.append("	     (    \n");
				query.append("	       ?,    \n");
				query.append("	       ?,     \n");
				query.append("	       ?,     \n");
				query.append("	       ?,     \n");
				query.append("	       SYSDATE    \n");
				query.append("	     )    \n");
	
				log.debug("\n query.toString() : {\n" + query.toString() + "}");
				//
				QueryRunner run = new QueryRunner();
				run.update(connection, query.toString(),
						new Object[] { metadata.getObject_id(), metadata.getKey(), metadata.getValue(), metadata.getDescription() });
				
			};
			
			//
			super.getDomainChangeEventBus().onSiteChanged(metadata_list.get(0).getObject_id());

		} catch (Exception ex) {
			log.error("Metadata Insert Exception : " + ex.getMessage());
			throw ex;
		} finally {
			super.closeConnection(connection);
		}
	}

	public void updateMetadata(Metadata metadata) throws Exception {
		//
		Connection connection = null;
		//
		try {
			//
			StringBuffer query = new StringBuffer();
			query.append("UPDATE MM_METADATA    \n");
			query.append("   SET KEY = ?,    \n");
			query.append("       VALUE = ?,    \n");
			query.append("       DESCRIPTION = ?,    \n");;
			query.append("       UPDATE_DATE = SYSDATE   \n");
			query.append(" WHERE OBJECT_ID = ?    \n");

			log.debug("\n query.toString() : {\n" + query.toString() + "}");
			//
			QueryRunner run = new QueryRunner();
			connection = super.getConnection();
			run.update(connection, query.toString(),
					new Object[] { metadata.getKey(), metadata.getValue(), metadata.getDescription(), metadata.getObject_id() });
			
			//
			super.getDomainChangeEventBus().onSiteChanged(metadata.getObject_id() );
			
		} catch (Exception ex) {
			log.error("Metadata Update Exception : " + ex.getMessage());
			throw ex;
		} finally {
			super.closeConnection(connection);
		}
	}

	public void deleteMetadataList(String object_id) throws Exception {
		//
		Connection connection = null;
		//
		try {
			//
			StringBuffer query = new StringBuffer();
			query.append("DELETE FROM MM_METADATA    \n");
			query.append(" WHERE OBJECT_ID = ?  \n");

			log.debug("\n query.toString() : {\n" + query.toString() + "}");
			//
			QueryRunner run = new QueryRunner();
			connection = super.getConnection();
			run.update(connection, query.toString(), new Object[] { object_id });
			
			//
			super.getDomainChangeEventBus().onSiteChanged(object_id);

		} catch (Exception ex) {
			log.error("Metadata Delete Exception : " + ex.getMessage());
			throw ex;
		} finally {
			super.closeConnection(connection);
		}
	}


}
