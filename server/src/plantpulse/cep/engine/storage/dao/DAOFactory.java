package plantpulse.cep.engine.storage.dao;

import plantpulse.cep.engine.config.ConfigurationManager;
import plantpulse.cep.engine.storage.StorageProductType;
import plantpulse.cep.engine.storage.insert.StorageInsertDAO;
import plantpulse.cep.engine.storage.insert.cassandra.CassandraBatchStatementBuilder;
import plantpulse.cep.engine.storage.insert.cassandra.CassandraInsertDAO;
import plantpulse.cep.engine.storage.insert.scylladb.ScyllaDBInsertDAO;
import plantpulse.cep.engine.storage.select.StorageSelectDAO;
import plantpulse.cep.engine.storage.select.cassandra.CassandraSelectDAO;
import plantpulse.cep.engine.storage.select.scylladb.ScyllaDBSelectDAO;


/**
 * DAOFactory
 * 
 * @author leesa
 *
 */
public class DAOFactory {

	
	private  String storage_product_type;
	
	public DAOFactory() {
		this.storage_product_type = ConfigurationManager.getInstance().getServer_configuration().getStorage_db_type();
	}
	
	public StorageInsertDAO getInsertDAO() throws Exception {
		if (storage_product_type.equals(StorageProductType.SCYLLADB)) {
			return new ScyllaDBInsertDAO();
		} else if (storage_product_type.equals(StorageProductType.CASSANDRA)) {
			return new CassandraInsertDAO();
		} else {
			throw new Exception("Unsupport storage database type = [" + storage_product_type + "]");
		}
	}

	public StorageSelectDAO getSelectDAO() throws Exception {
		if (storage_product_type.equals(StorageProductType.SCYLLADB)) {
			return new ScyllaDBSelectDAO();
		} else if (storage_product_type.equals(StorageProductType.CASSANDRA)) {
			return new CassandraSelectDAO();
		} else {
			throw new Exception("Unsupport storage database type = [" + storage_product_type + "]");
		}
	}
	

	public void buildPreparedStatement() throws Exception{
		if (storage_product_type.equals(StorageProductType.SCYLLADB)) {
			CassandraBatchStatementBuilder.getInstance().init();
		} else if (storage_product_type.equals(StorageProductType.CASSANDRA)) {
			CassandraBatchStatementBuilder.getInstance().init();
		} else {
			throw new Exception("Unsupport storage database type = [" + storage_product_type + "]");
		}
		
	}

}
