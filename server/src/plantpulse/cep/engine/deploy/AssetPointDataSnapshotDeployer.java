package plantpulse.cep.engine.deploy;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import plantpulse.cep.engine.logging.EngineLogger;
import plantpulse.cep.engine.properties.PropertiesLoader;
import plantpulse.cep.engine.scheduling.JobDateUtils;
import plantpulse.cep.engine.scheduling.JobManager;
import plantpulse.cep.engine.scheduling.job.AssetPointDataJob;
import plantpulse.cep.engine.scheduling.job.AssetPointDataSamplingJob;
import plantpulse.cep.engine.scheduling.job.AssetTableLayoutJob;


/**
 * AssetPointDataSnapshotDeployer
 * 
 * @author leesa
 *
 */
public class AssetPointDataSnapshotDeployer implements Deployer {

	private static final Log log = LogFactory.getLog(AssetPointDataSnapshotDeployer.class);

	public void deploy() {
		try {
			
			//
			String SNAPSHOT_INTEVAL = PropertiesLoader.getStorage_properties().getProperty("storage.asset.data.snapshot.interval", "10 SECONDS");
			
			//----------------------------------------------------------------------------------------------------------------------------------
			//1. 먼저 에셋 테이블 레이아웃 캐싱 잡을 실행
			(new AssetTableLayoutJob()).load(); //처음 부팅시, 레이아웃 먼저 로드 후, 시작
			//
			AssetTableLayoutJob layout_job = new AssetTableLayoutJob(); 
			JobManager.getInstance().startJob("ASSET_TABLE_LAYOUT_JOB",  JobDateUtils.getCronPattern("10 MINUTES"), layout_job, "10 MINUTES");
			//----------------------------------------------------------------------------------------------------------------------------------
			
			//2. 캐시된 에셋 테이블 레이아웃으로 포인트 데이터를 저장
			AssetPointDataJob data_job = new AssetPointDataJob();
			JobManager.getInstance().startJob("ASSET_DATA_JOB", JobDateUtils.getCronPattern(SNAPSHOT_INTEVAL), data_job, SNAPSHOT_INTEVAL);
			
			//4. 추가적인 에셋 데이터 샘플링 
			JobManager.getInstance().startJob("ASSET_DATA_SAMPLING_JOB_BY_1_MINUTES",  JobDateUtils.getCronPattern("1 MINUTES"),  new AssetPointDataSamplingJob(), "1 MINUTES");
			JobManager.getInstance().startJob("ASSET_DATA_SAMPLING_JOB_BY_1_HOURS",    JobDateUtils.getCronPattern("1 HOURS"),    new AssetPointDataSamplingJob(), "1 HOURS");
			
			log.info("Asset point data snapshot job deployed.");

		} catch (Exception ex) {
			EngineLogger.error("에셋 포인트 데이터 스냅샷 처리기를 배치하는 도중 오류가 발생하였습니다 : 에러=" + ex.getMessage());
			log.error("Asset point data snapshot job deploy error : " + ex.getMessage(), ex);
		}
	}

}
