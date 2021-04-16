package plantpulse.cep.engine.recovery;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.redisson.api.RMap;

import plantpulse.cep.engine.inmemory.redis.RedisInMemoryClient;
import plantpulse.cep.engine.snapshot.PointMapFactory;
import plantpulse.event.opc.Point;


/**
 * PointMapRecovery
 * 
 * @author lsb
 *
 */
public class PointMapRecovery {
	
	private static final Log log = LogFactory.getLog(PointMapRecovery.class);
	
	public void recovery() throws Exception {
		
		try{
			//
			RMap<String, Point> point_map = RedisInMemoryClient.getInstance().getPointMap();
			if(point_map != null && point_map.size() > 0){
			   		long start = System.currentTimeMillis();
					PointMapFactory.getInstance().setPointMap(point_map);
					//
					long process_time = System.currentTimeMillis() - start;
					log.info("Last tag point cache recovery completed : count=[" + point_map.size() +"], process_time=[" + process_time + "]ms, ");
			}else{
				log.info("Point map is not exist in in-memory.");
			}
			//
		}catch(Exception ex){
			log.error("Point map recovery failed : " + ex.getMessage(), ex);
		}
		
	
		
	}

}
