package plantpulse.cep.version;

import java.util.ArrayList;
import java.util.List;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import plantpulse.Metadata;
import plantpulse.cep.dao.VersionDAO;
import plantpulse.cep.engine.logging.EngineLogger;
import plantpulse.cep.version.update.VersionList;

/**
 * VersionManager
 * 
 * @author leesa
 *
 */
public class VersionManager {

	private static final Log log = LogFactory.getLog(VersionManager.class);

	private VersionDAO version_dao;

	public VersionManager() {
		version_dao = new VersionDAO();
	}

	public double getMetadataVersion() {
		return Metadata.SOURCE_VERISON;
	}

	public void update() {
		try {
		    
			log.info("Platform version update check starting ... ");
			//
			version_dao.createVersionHistoryTable();
			//
			List<VersionUpdate> version_list = VersionList.getList();
			List<Long> patched_list = new ArrayList<>();
			//
			for (int i = 0; i < version_list.size(); i++) {
				VersionUpdate up = version_list.get(i);
					try {
						boolean is_aleady_pathed = version_dao.checkPatchedVersion(up.getVersion());
						if(!is_aleady_pathed) {
							up.upgrade();
							patched_list.add(up.getVersion());
							version_dao.insertVersion(up.getVersion());
							log.info("[" + up.getVersion() + "] Version patched.");
							EngineLogger.info("플랫폼 업데이트 패치 적용 완료 : " + up.getVersion());
						}else {
							//
						}
					} catch (Exception ex) {
						log.error("[" + up.getVersion() + "] Version patch failed.");
						EngineLogger.error("플랫폼 업데이트 패치 적용 실패 : " + up.getVersion());
						throw ex;
					}
			};
			
			if(patched_list.size() > 0) {
				log.info("Platform version update completed : patch_count=["  + patched_list.size() + "], patched_list=["  + patched_list.toString() + "]");
				log.info("Platform restart is required after the upgrade is complete. Restart the platform now.");
				EngineLogger.info("플랫폼 업데이트가 완료되었습니다. 플랫폼을 재시작하십시오. 패치목록=[" + patched_list.toString() + "]");
				EngineLogger.info("플랫폼 종료 완료 (재시작)");
				//
				System.exit(0);
				
			}else {
				log.info("Platform does not require an version update.");
			}
		} catch (Exception e) {
			log.error("Platform version update failed : " + e.getMessage(), e);
		}

	};

}
