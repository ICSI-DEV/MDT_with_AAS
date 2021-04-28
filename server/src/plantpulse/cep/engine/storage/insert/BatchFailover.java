package plantpulse.cep.engine.storage.insert;

import java.io.File;
import java.util.List;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.json.JSONArray;
import org.realdisplay.framework.util.FileUtils;

import plantpulse.buffer.db.DBDataPoint;
import plantpulse.cep.context.EngineContext;

/**
 * BatchFailover
 * @author leesa
 *
 */
public class BatchFailover {
	
	private static Log log = LogFactory.getLog(BatchFailover.class);

	private String backup_path = "";

	/**
	 * 페일오버 초기화
	 * 배치 실패시, 저장할 디렉토리 생성
	 */
	public void init() {
		try {
			String root = (String) EngineContext.getInstance().getProps().get("_ROOT");
			backup_path = root + "/../../backup/cassandra/batcherror/";
			FileUtils.forceMkdir(new File(backup_path));
		} catch (Exception ex) {
			log.error("DB Batch failover init failed :" + ex.getMessage(), ex);
		} finally {
			//
		}
	}

	/**
	 * 배치 저장에 실패한 포인트 저장 (JSON 파일로 저장)
	 * @param batch_error_point_list
	 */
	public void backup(List<DBDataPoint> batch_error_point_list) {
		try {
			String file_name = "" + System.nanoTime() + ".json";
			File file = new File(backup_path + file_name);
			JSONArray array = JSONArray.fromObject(batch_error_point_list);
			FileUtils.writeStringToFile(file, array.toString(), "UTF-8");
		} catch (Exception ex) {
			log.error("DB Batch fail point save error :" + ex.getMessage(), ex);
		} finally {
			//
		}
	};

}
