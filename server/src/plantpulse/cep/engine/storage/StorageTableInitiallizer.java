package plantpulse.cep.engine.storage;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import plantpulse.cep.service.client.StorageClient;

/**
 * StorageTableInitiallizer
 * @author lsb
 *
 */
public class StorageTableInitiallizer {

	private static final Log log = LogFactory.getLog(StorageTableInitiallizer.class);

	public void init() throws Exception {
		try {
			
			//
			StorageClient client = new StorageClient();
			//
			client.forInsert().createKeyspace();
			client.forInsert().createFunctionAndAggregate();
			//
			client.forInsert().createTableForMetadata();
			client.forInsert().createTableForPointAndAlarm();
			client.forInsert().createTableForBLOB();
			client.forInsert().createTableForSampling();
			client.forInsert().createTableForAggregation();
			client.forInsert().createTableForSnapshot();
			client.forInsert().createTableForArchive();
			//
			client.forInsert().createTableForAssetTimeline();
			client.forInsert().createTableForAssetData();
			client.forInsert().createTableForAssetAlarm();
			client.forInsert().createTableForAssetEvent();
			client.forInsert().createTableForAssetContext();
			client.forInsert().createTableForAssetAggregation();
			client.forInsert().createTableForAssetHealthStatus();
			client.forInsert().createTableForAssetConnectionStatus();
			
			client.forInsert().createTableForDomainChangeHistory();
			
			client.forInsert().createTableForOptions();
			
			//
			client.forInsert().createTableForSystemHealthStatus();
			
			//
			client.forInsert().setupTableCompactionStrategy();
			client.forInsert().setupTableComression();
			client.forInsert().setupTableCacheAndTTL();
			
			//
			client.forInsert().createMaterializedView();
			
			//
			client.buildPreparedStatement();
			
			//
			log.info("Storage initiallized.");
			//
		} catch (Exception e) {
			log.error("Storage initiallize failed : " + e.getMessage(), e);
			throw e;
		}
	}

}
