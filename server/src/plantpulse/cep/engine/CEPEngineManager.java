package plantpulse.cep.engine;

import java.sql.Driver;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.Collections;
import java.util.Date;
import java.util.Enumeration;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.log4j.Appender;
import org.apache.log4j.Level;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;
import org.fusesource.hawtdispatch.Dispatch;

import com.datastax.driver.core.Session;
import com.espertech.esper.client.Configuration;
import com.espertech.esper.client.ConfigurationDBRef;
import com.espertech.esper.client.EPServiceProvider;
import com.espertech.esper.client.EPServiceProviderManager;

import plantpulse.cep.Metadata;
import plantpulse.cep.db.HSQLDBServer;
import plantpulse.cep.db.datasource.HSQLDBDataSource;
import plantpulse.cep.engine.async.AsynchronousExecutorPool;
import plantpulse.cep.engine.config.CEPServiceConfigurator;
import plantpulse.cep.engine.config.ConfigurationManager;
import plantpulse.cep.engine.config.ServerConfiguration;
import plantpulse.cep.engine.dds.DataDistributionFactory;
import plantpulse.cep.engine.dds.DataDistributionService;
import plantpulse.cep.engine.deploy.AssetCacheDeployer;
import plantpulse.cep.engine.deploy.AssetConnectionStatusDeployer;
import plantpulse.cep.engine.deploy.AssetHealthStatusDeployer;
import plantpulse.cep.engine.deploy.AssetPointDataSnapshotDeployer;
import plantpulse.cep.engine.deploy.AssetStatementDeployer;
import plantpulse.cep.engine.deploy.EventDeployer;
import plantpulse.cep.engine.deploy.GraphDeployer;
import plantpulse.cep.engine.deploy.MetastoreBackupDeployer;
import plantpulse.cep.engine.deploy.ModuleDeployer;
import plantpulse.cep.engine.deploy.PointAggregationDeployer;
import plantpulse.cep.engine.deploy.PointArchiveDeployer;
import plantpulse.cep.engine.deploy.PointCalculationDeployer;
import plantpulse.cep.engine.deploy.PointForecastDeployer;
import plantpulse.cep.engine.deploy.PointMapBinaryDeployer;
import plantpulse.cep.engine.deploy.PointMapDeployer;
import plantpulse.cep.engine.deploy.PointMatrixDeployer;
import plantpulse.cep.engine.deploy.PointSamplingDeployer;
import plantpulse.cep.engine.deploy.PointSnapshotDeployer;
import plantpulse.cep.engine.deploy.PointTimedDeployer;
import plantpulse.cep.engine.deploy.SiteArchitectureCacheDeployer;
import plantpulse.cep.engine.deploy.SiteTagCountDeployer;
import plantpulse.cep.engine.deploy.StatementDeployer;
import plantpulse.cep.engine.deploy.SystemHealthStatusDeployer;
import plantpulse.cep.engine.deploy.TagAlarmConfigDeployer;
import plantpulse.cep.engine.deploy.TagCacheDeployer;
import plantpulse.cep.engine.deploy.TokenDeployer;
import plantpulse.cep.engine.deploy.TriggerDeployer;
import plantpulse.cep.engine.eventbus.DomainChangeSyncTimer;
import plantpulse.cep.engine.inmemory.redis.RedisInMemoryClient;
import plantpulse.cep.engine.logging.EngineLogger;
import plantpulse.cep.engine.memory.JavaGCTimer;
import plantpulse.cep.engine.messaging.broker.MessageServerBroker;
import plantpulse.cep.engine.messaging.broker.MultiProtocolMessageServerBroker;
import plantpulse.cep.engine.model.SiteMetaModelUpdater;
import plantpulse.cep.engine.monitoring.jmx.JMXAgent;
import plantpulse.cep.engine.monitoring.jmx.JMXToTimeseriesDBSyncTimer;
import plantpulse.cep.engine.monitoring.metric.PipelineMetricRegistry;
import plantpulse.cep.engine.monitoring.timer.CEPServiceMonitorTimer;
import plantpulse.cep.engine.monitoring.timer.CassandraHealthCheckTimer;
import plantpulse.cep.engine.monitoring.timer.DDSMonitorTimer;
import plantpulse.cep.engine.monitoring.timer.DataFlowErrorTimer;
import plantpulse.cep.engine.monitoring.timer.HDDMonitoringTimer;
import plantpulse.cep.engine.monitoring.timer.MQandDBHealthCheckTimer;
import plantpulse.cep.engine.monitoring.timer.MetastoreCheckpointTimer;
import plantpulse.cep.engine.monitoring.timer.NetworkLatencyTimer;
import plantpulse.cep.engine.monitoring.timer.OSPerformanceAlarmTimer;
import plantpulse.cep.engine.monitoring.timer.PointStreamCountMonitorTimer;
import plantpulse.cep.engine.monitoring.timer.PointStreamTimeoutMonitorTimer;
import plantpulse.cep.engine.monitoring.timer.StorageSessionMonitorTimer;
import plantpulse.cep.engine.monitoring.timer.SystemRunDurationTimer;
import plantpulse.cep.engine.monitoring.timer.TempDirDeleteTimer;
import plantpulse.cep.engine.ntp.NTPDateSyncTimer;
import plantpulse.cep.engine.os.OSEnv;
import plantpulse.cep.engine.pipeline.DataFlowPipe;
import plantpulse.cep.engine.pipeline.dataflow.BaseDataFlowPipe;
import plantpulse.cep.engine.pipeline.dataflow.ParallelDataFlowPipe;
import plantpulse.cep.engine.pipeline.dataflow.adjuster.DataFlowAdjuster;
import plantpulse.cep.engine.properties.PropertiesLoader;
import plantpulse.cep.engine.recovery.OPCAgentRecoveryTimer;
import plantpulse.cep.engine.recovery.PointMapRecovery;
import plantpulse.cep.engine.scheduling.JobSchedulerFactory;
import plantpulse.cep.engine.storage.StorageProcessor;
import plantpulse.cep.engine.storage.StorageProcessorFactory;
import plantpulse.cep.engine.storage.buffer.DBDataPointBufferFactory;
import plantpulse.cep.engine.storage.timeseries.TimeSeriesDatabase;
import plantpulse.cep.engine.storage.timeseries.TimeSeriesDatabaseFactory;
import plantpulse.cep.engine.stream.StreamProcessor;
import plantpulse.cep.engine.stream.StreamProcessorFactory;
import plantpulse.cep.engine.test.TestEventTimer;
import plantpulse.cep.engine.timer.TimerExecuterPool;
import plantpulse.cep.query.QueryExpireTimer;
import plantpulse.cep.version.VersionManager;
import plantpulse.dbutils.cassandra.SessionCacheFactory;

/**
 * <B> CEP 엔진 매니저 </B> <B> 스트리밍 처리를 위한 핵심 엔진의 매니저입니다. 전체 CEP기반의 처리의 라이프사이클을
 * 관리합니다.</B>
 * 
 * @author leesangboo
 * 
 */
public class CEPEngineManager {

	private static class CEPEngineManagerHolder {
		static CEPEngineManager instance = new CEPEngineManager();
	};

	public static CEPEngineManager getInstance() {
		return CEPEngineManagerHolder.instance;
	};

	private static final Log log = LogFactory.getLog(CEPEngineManager.class);

	public static final String META_DATABASE_ALIAS_NAME = "metabase";

	public static final String TIMESERIES_DATABASE_ALIAS_NAME = "timebase";

	//
	private EPServiceProvider provider;

	//
	private final AtomicBoolean started = new AtomicBoolean(false);

	//
	private Date start_date = null;
	private Date started_date = null;

	private boolean is_shutdown = false;

	//
	private MessageServerBroker message_broker = null;

	//
	private StreamProcessor stream_processor = null;
	
	//
	private StorageProcessor storage_processor = null;
	
	//
	private DataFlowPipe data_flow_pipe = null;

	//
	private PipelineMetricRegistry pipeline_metric_registry = null;
	
	//
	private CEPServiceMonitorTimer cep_service_monitor = null;
	
	//
	private NetworkLatencyTimer network_latency_timer = null;
	
	//
	private OSPerformanceAlarmTimer performance_alarm_timer = null;

	//
	private DataFlowErrorTimer data_flow_error_timer = null;
	
	//
	private PointStreamCountMonitorTimer point_stream_count_timer = null;
	
	//
	private PointStreamTimeoutMonitorTimer point_stream_timeout_timer = null;

	//
	private MQandDBHealthCheckTimer mq_and_db_health_timer = null;

	//
	private CassandraHealthCheckTimer cassandra_health_timer = null;

	//
	private StorageSessionMonitorTimer storage_session_monitoring_timer = null;
	
	//
	private DDSMonitorTimer dds_minotoring_timer = null;

	//
	private HDDMonitoringTimer hdd_monitoring_timer = null;

	//
	private QueryExpireTimer query_expire_timer = null;

	//
	private TestEventTimer test_event_timer = null;

	//
	private OPCAgentRecoveryTimer opc_agent_recovery_timer;;

	//
	private DomainChangeSyncTimer domain_change_sync_timer = null;

	//
	private DataFlowAdjuster data_flow_adjuster = null;

	//
	private SiteMetaModelUpdater site_meta_model_updater = null;

	//
	private JavaGCTimer java_gc_timer = null;

	//
	private TempDirDeleteTimer temp_del_timer = null;

	//
	private MetastoreCheckpointTimer meta_checkpoint_timer = null;

	//
	private SystemRunDurationTimer system_run_duration_timer = null;

	//
	private TimeSeriesDatabase timeseries_database = null;
	
	//
	private DataDistributionService data_distribution_service = null;

	//
	private JMXAgent jmx = null;

	//
	private JMXToTimeseriesDBSyncTimer jmx_tdb_sync_timer = null;

	//
	private NTPDateSyncTimer ntpdate_sync_timer = null;

	//
	private VersionManager vesion_manager = null;
	
	

	/**
	 * 엔진 매니저를 시작한다.
	 * 
	 * @param root
	 */
	public void start(String root, boolean is_restart) {
		try {

			start_date = new Date();

			//
			EngineLogger.info("O/S 환경정보 설정 중 ...");
			OSEnv.getInstance().configure();
			EngineLogger.info("O/S 환경정보 설정 완료");

			//

			log.info("CEPEngine starting... : serviceId=[" + Metadata.CEP_SERVICE_ID + "]");

			EngineLogger.reset();

			EngineLogger.info("CEP 엔진을 시작중입니다...");

			//
			// 전체 프로퍼티 로딩
			//
			PropertiesLoader.load();

			// 비동기 쓰레드 풀 시작
			AsynchronousExecutorPool.getInstance().start();
			TimerExecuterPool.getInstance().start();
			EngineLogger.info("서버 스레드 및 타이머 풀링 시스템을 시작하였습니다.");

			//
			ntpdate_sync_timer = new NTPDateSyncTimer();
			ntpdate_sync_timer.start();

			//
			Properties prop = PropertiesLoader.getEngine_properties();

			// 서버 홈 설정
			String server_home = root + "../../";
			ConfigurationManager.getInstance().getServer_configuration().setServer_home(server_home);

			// 엔진 설정 생성
			Configuration cep_config = CEPServiceConfigurator.createConfiguration(root);

			// 서버 설정 정보 로드
			ServerConfiguration server_config = ConfigurationManager.getInstance().getServer_configuration();
			EngineLogger.info("서버 설정 정보 = " + server_config.toString());

			ConfigurationDBRef metabase_config = new ConfigurationDBRef();
			metabase_config.setDriverManagerConnection(server_config.getMetastore_driver(),
					server_config.getMetastore_url(), server_config.getMetastore_user(),
					server_config.getMetastore_password());
			cep_config.addDatabaseReference(META_DATABASE_ALIAS_NAME, metabase_config);

			ConfigurationDBRef timebase_config = new ConfigurationDBRef();
			timebase_config.setDriverManagerConnection("plantpulse.cassandra.jdbc.CassandraDriver",
					"jdbc:cassandra://" + server_config.getStorage_host() + ":" + server_config.getStorage_port() + "/"
							+ server_config.getStorage_keyspace(),
					server_config.getStorage_user(), server_config.getStorage_password());
			cep_config.addDatabaseReference(TIMESERIES_DATABASE_ALIAS_NAME, timebase_config);

			EngineLogger.info("CEP 데이터베이스 연동 설정이 완료되었습니다...");

			// 인-메모리 캐시 연결
			RedisInMemoryClient.getInstance().connect();
			EngineLogger.info("인-메모리 캐쉬에 연결되었습니다...");

			// 설정을 통한 프로바이더 획득
			EngineLogger.info("CEP 서비스를 시작중 입니다...");
			provider = EPServiceProviderManager.getProvider(Metadata.CEP_SERVICE_ID, cep_config);//
			
			// 파이프라인 매트릭 레지스트리 초기화
			pipeline_metric_registry = new PipelineMetricRegistry();
			pipeline_metric_registry.init();
			
			// 데이터 배포 서비싀 초기화
			DataDistributionFactory.getInstance().init();
			data_distribution_service = DataDistributionFactory.getInstance().getDataDistributionService();
			EngineLogger.info("데이터 배포 팩토리 및 서비스를 초기화하였습니다.");

			// 잡스케쥴러 시작
			EngineLogger.info("잡 스케쥴러 팩토리를 초기화중 입니다...");
			JobSchedulerFactory.getInstance().init();

			//데이터 플로우 조정기 초기화
			data_flow_adjuster = (DataFlowAdjuster) Class.forName(prop.getProperty("engine.dataflow.adjuster.class")).newInstance();
			EngineLogger.info("데이터 플로우 조정기 인스턴스를 생성하였습니다.");
			
            // 사이트 모델 업데이터 초기화
			site_meta_model_updater = (SiteMetaModelUpdater) Class.forName(prop.getProperty("engine.site.model.updater.class")).newInstance();
			EngineLogger.info("사이트 메타 모델 업데이터 인스턴스를 생성하였습니다.");

			if (!is_restart) {

				// 내장 DB 서버 시작
				HSQLDBServer.getInstance().start(server_home);
				EngineLogger.info("데이터베이스 서버를 시작하였습니다.");

				// 버전 매니저 업데이트 체크
				EngineLogger.info("버전 매니저가 업데이트 체크를 시작합니다...");
				vesion_manager = new VersionManager();
				vesion_manager.update();

				// 시계열 데이터베이스 연결
				timeseries_database = (new TimeSeriesDatabaseFactory()).getTimeSeriesDatabase();
				timeseries_database.connect();
				EngineLogger.info("시계열 데이터베이스와 연결되었습니다.");

				// 스트리밍 프로세서 시작
				StreamProcessorFactory.getInstance().init();
				stream_processor = StreamProcessorFactory.getInstance().getStreamProcessor();
				stream_processor.start();
				EngineLogger.info("스트리밍 프로세서를 시작하였습니다.");

				// 스토리지 프로세서 시작
				StorageProcessorFactory.getInstance().init();
				storage_processor = StorageProcessorFactory.getInstance().getStorageProcessor();
				EngineLogger.info("스토리지 프로세서를 시작하였습니다.");
				
				data_flow_pipe = new BaseDataFlowPipe(data_flow_adjuster, stream_processor, storage_processor, data_distribution_service);
				data_flow_pipe.init();
				EngineLogger.info("기본 데이터 흐름 파이프를 초기화하였습니다.");

				// MQTT 서버 브로커시작
				message_broker = MultiProtocolMessageServerBroker.getInstance();
				message_broker.start();
				message_broker.initPublisher();
				message_broker.startListener(started);
			}
			;

			// 모니터 시작
			cep_service_monitor =  new CEPServiceMonitorTimer(Metadata.CEP_SERVICE_ID, provider);
			cep_service_monitor.start();
			EngineLogger.info("CEP 성능 모니터링 서비스를 실행하였습니다.");
			
			// 네트워크 지연 모니터
			network_latency_timer = new NetworkLatencyTimer();
			network_latency_timer.start();
			EngineLogger.info("네트워크 지연 모니터링 서비스를 실행하였습니다.");

			// 포인트 스트림 카운트 타이머
			point_stream_count_timer = new PointStreamCountMonitorTimer();
			point_stream_count_timer.start();
			EngineLogger.info("포인트 스트림 카운트 모니터링 서비스를 실행하였습니다.");
			
			// 포인트 맵 복구
			PointMapRecovery point_map_recovery = new PointMapRecovery();
			point_map_recovery.recovery();
			EngineLogger.info("마지막 태그 포인트를 복원하였습니다.");

			// 이벤트 로딩
			EventDeployer event_deployer = new EventDeployer();
			event_deployer.deploy();
			EngineLogger.info("이벤트 목록을 배치하였습니다.");

			// 모듈 로딩
			ModuleDeployer module_deployer = new ModuleDeployer(provider, root);
			module_deployer.deploy();
			EngineLogger.info("모듈을 배치하였습니다.");

			// 싱글 스테이트먼트 로딩
			StatementDeployer statement_deployer = new StatementDeployer(provider);
			statement_deployer.deploy();
			EngineLogger.info("스테이트먼트 목록을 배치하였습니다.");

			// 오늘 집계 처리 디플로이
			SiteTagCountDeployer today_aggregation_deployer = new SiteTagCountDeployer();
			today_aggregation_deployer.deploy();
			EngineLogger.info("사이트 카운트 집계 스테이트먼트를 배치하였습니다.");

			// 기준 시간 태그 인벤트 생성기
			PointTimedDeployer tag_timed_deployer = new PointTimedDeployer();
			tag_timed_deployer.deploy();
			EngineLogger.info("기준 시간 태그 이벤트 생성기를 배치하였습니다.");

			// 에셋 로케이션 캐싱 처리
			AssetCacheDeployer asset_cache_deployer = new AssetCacheDeployer();
			asset_cache_deployer.deploy();
			EngineLogger.info("에셋 캐싱 처리기를 배치하였습니다.");

			// 태그 로케이션 캐싱 처리
			TagCacheDeployer tag_cache_deployer = new TagCacheDeployer();
			tag_cache_deployer.deploy();
			EngineLogger.info("태그 캐싱 처리기를 배치하였습니다.");

			// 태그 알람 설정을 배치
			TagAlarmConfigDeployer alarm_config_deployer = new TagAlarmConfigDeployer();
			alarm_config_deployer.deploy();
			EngineLogger.info("태그 알람 설정을 배치하였습니다.");

			// 태그 포인트 샘플링 처리 디플로이
			PointSamplingDeployer tag_sampling_deployer = new PointSamplingDeployer();
			tag_sampling_deployer.deploy();
			EngineLogger.info("태그 샘플링 스테이트먼트를 배치하였습니다.");

			// 태그 포인트 집계 처리 디플로이
			PointAggregationDeployer tag_aggregation_deployer = new PointAggregationDeployer();
			tag_aggregation_deployer.deploy();
			EngineLogger.info("태그 포인트 집계 스테이트먼트를 배치하였습니다.");

			// 태그 계산 디플로이
			PointCalculationDeployer tag_config_deployer = new PointCalculationDeployer();
			tag_config_deployer.deploy();
			EngineLogger.info("태그 포인트 계산 스테이트먼트를 배치하였습니다.");

			// 태그 포인트 메트릭스 처리 디플로이
			PointMatrixDeployer tag_matrix_deployer = new PointMatrixDeployer();
			tag_matrix_deployer.deploy();
			EngineLogger.info("태그 포인트 메트릭스 저장 처리기를 배치하였습니다.");

			// 태그 포인트 스냅샵 처리 디플로이
			PointSnapshotDeployer tag_snapshot_deployer = new PointSnapshotDeployer();
			tag_snapshot_deployer.deploy();
			EngineLogger.info("태그 포인트 스냅샷 저장 처리기를 배치하였습니다.");

			// 태그 포인트 아카이브 처리 디플로이
			if (prop.getOrDefault("engine.data.tag.point.archive.enabled", "false").equals("true")) {
				PointArchiveDeployer tag_archive_deployer = new PointArchiveDeployer();
				tag_archive_deployer.deploy();
				EngineLogger.info("태그 포인트 아카이브 저장 처리기를 배치하였습니다.");
			} else {
				EngineLogger.info("태그 포인트 아카이브 저장 처리기는 비활성화 되었습니다.");
			}

			// 태그 포인트 맵 처리기 디플로이
			PointMapDeployer point_map_deployer = new PointMapDeployer();
			point_map_deployer.deploy();
			EngineLogger.info("태그 포인트 맵 처리기를 배치하였습니다.");

			// 태그 포인트 맵 바이너리 처리기 디플로이
			PointMapBinaryDeployer point_map_bin_deployer = new PointMapBinaryDeployer();
			point_map_bin_deployer.deploy();
			EngineLogger.info("태그 포인트 맵 바이너리 처리기를 배치하였습니다.");

			// 태그 포인트 로우 처리기 디플로이
			PointForecastDeployer point_forecast_deployer = new PointForecastDeployer();
			point_forecast_deployer.deploy();
			EngineLogger.info("태그 포인트 예측 처리기를 배치하였습니다.");

			// 태그 포인트 에셋 처리기 디플로이
			AssetPointDataSnapshotDeployer point_asset_deployer = new AssetPointDataSnapshotDeployer();
			point_asset_deployer.deploy();
			EngineLogger.info("에셋 포인트 데이터 스냅샷 처리기를 배치하였습니다.");

			// 에셋 스테이트먼트 배치기
			AssetStatementDeployer asset_statement_deployer = new AssetStatementDeployer();
			asset_statement_deployer.deploy();
			EngineLogger.info("에셋 스테이트먼트 처리기를 배치하였습니다.");

			// 메타스토어 백업 배치기
			MetastoreBackupDeployer metastore_backup_deployer = new MetastoreBackupDeployer();
			metastore_backup_deployer.deploy();
			EngineLogger.info("메타스토어 백업 처리기를 배치하였습니다.");

			// 에셋 모델 투 이벤트 디플로이
			// AssetTemplateToEventDeployer model_deployer = new
			// AssetTemplateToEventDeployer();
			// model_deployer.deploy();
			// EngineLogger.info("에셋 모델 이벤트 생성기를 배치하였습니다.");

			// AssetTemplateToTableDeployer atable_deployer = new
			// AssetTemplateToTableDeployer();
			// atable_deployer.deploy();
			// EngineLogger.info("에셋 모델 테이블 생성기를 배치하였습니다.");

			AssetHealthStatusDeployer asset_health_deployer = new AssetHealthStatusDeployer();
			asset_health_deployer.deploy();
			EngineLogger.info("에셋 헬스 상태 처리기를 배치하였습니다.");

			AssetConnectionStatusDeployer asset_connection_deployer = new AssetConnectionStatusDeployer();
			asset_connection_deployer.deploy();
			EngineLogger.info("에셋 연결 상태 처리기를 배치하였습니다.");

			// 애플리케이션 시작 (그래프, 알람, 트리거)
			GraphDeployer graph_deployer = new GraphDeployer();
			graph_deployer.deploy();
			EngineLogger.info("그래프를 배치하였습니다.");

			TriggerDeployer trigger_deployer = new TriggerDeployer();
			trigger_deployer.deploy();
			EngineLogger.info("트리거를 배치하였습니다.");

			SystemHealthStatusDeployer system_health_deployer = new SystemHealthStatusDeployer();
			system_health_deployer.deploy();
			EngineLogger.info("시스템 헬스 상태 처리기를 배치하였습니다.");

			site_meta_model_updater.update();
			EngineLogger.info("사이트 메타 모델을 업데이트하였습니다.");

			SiteArchitectureCacheDeployer site_archi_cache_deployer = new SiteArchitectureCacheDeployer();
			site_archi_cache_deployer.deploy();
			EngineLogger.info("사이트 아키텍처 JSON을 캐쉬하였습니다.");

			domain_change_sync_timer = new DomainChangeSyncTimer();
			domain_change_sync_timer.start();
			EngineLogger.info("도매인 변경 동기화 타이머를 시작하였습니다.");

			meta_checkpoint_timer = new MetastoreCheckpointTimer();
			meta_checkpoint_timer.start();
			EngineLogger.info("메타스토어 체크포인트 타이머를 시작하였습니다.");

			TokenDeployer token_deployer = new TokenDeployer();
			token_deployer.deploy();
			EngineLogger.info("API 인증 토큰을 배포하였습니다.");

			JobSchedulerFactory.getInstance().start();
			EngineLogger.info("잡 스케쥴러 팩토리에서 스케쥴러를 시작하였습니다.");

	
			jmx = JMXAgent.getInstance();
			jmx.start();
			EngineLogger.info("JMX 에이전트를 시작하였습니다.");

			//
			jmx_tdb_sync_timer = new JMXToTimeseriesDBSyncTimer();
			jmx_tdb_sync_timer.start();
			EngineLogger.info("JMX 메트릭 시계열 데이터베이스 동기화 타이머를 시작하였습니다.");

			//
			String server_time = LocalDateTime.now().format(DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss"));
			EngineLogger.info("현재 서버시간은 [" + server_time + "] 입니다.");

			//
			EngineLogger.info("CEP 엔진이 시작되었습니다.");
			log.info("CEPEngineManager started.");

			started.set(true);
			started_date = new Date();

			// connection_watcher.start();

			//
			performance_alarm_timer = new OSPerformanceAlarmTimer();
			performance_alarm_timer.start();
			
			point_stream_timeout_timer = new PointStreamTimeoutMonitorTimer();
			point_stream_timeout_timer.start();

			data_flow_error_timer = new DataFlowErrorTimer();
			data_flow_error_timer.start();

			mq_and_db_health_timer = new MQandDBHealthCheckTimer();
			mq_and_db_health_timer.start();

			cassandra_health_timer = new CassandraHealthCheckTimer();
			cassandra_health_timer.start();

			storage_session_monitoring_timer = new StorageSessionMonitorTimer();
			storage_session_monitoring_timer.start();
			
			dds_minotoring_timer = new DDSMonitorTimer();
			dds_minotoring_timer.start();

			hdd_monitoring_timer = new HDDMonitoringTimer();
			hdd_monitoring_timer.start();

			query_expire_timer = new QueryExpireTimer();
			query_expire_timer.start();

			temp_del_timer = new TempDirDeleteTimer();
			temp_del_timer.start();

			test_event_timer = new TestEventTimer();
			test_event_timer.start();

			opc_agent_recovery_timer = new OPCAgentRecoveryTimer();
			opc_agent_recovery_timer.start();

			system_run_duration_timer = new SystemRunDurationTimer();
			system_run_duration_timer.start();

			EngineLogger.info("시스템 모니터링 서비스가 시작되었습니다.");

			// GC 타이머
			java_gc_timer = new JavaGCTimer();
			if (prop.getProperty("engine.javagc.enabled").equals("true")) {
				java_gc_timer.start();
			}

			EngineLogger.info("플랜트펄스 플랫폼의 모든 서비스가 정상적으로 시작되었습니다.");

			//
		} catch (Exception ex) {

			EngineLogger.error("서비스를 시작하는 도중 오류가 발생하였습니다 : " + ex.getMessage());
			//
			String msg = "CEPEngineManager starting error : " + ex.getMessage();
			log.error(msg, ex);
		}
		//

	}

	/**
	 * 엔진 매니저를 셧다운한다.
	 */
	public void shutdown() {

		is_shutdown = true;

		if (started.get()) {
			

			EngineLogger.info("서비스를 셧다운중 입니다... 잠시후 서버가 종료됩니다.");
			
			System.out.println("------------------------------------------------------------------------");
			System.out.println("플랜트펄스 플랫폼 - 셧다운");
			System.out.println("------------------------------------------------------------------------");
			
			try {
				System.out.println("데이터 정리를 위한 대기중 ... (3초)");
				System.setProperty("log4j.shutdownHookEnabled", Boolean.toString(false));
				Thread.sleep(3 * 1000);
			} catch (Exception ex) {
				ex.printStackTrace();
			}

			//
			@SuppressWarnings("unchecked")
			List<Logger> loggers = Collections.<Logger>list(LogManager.getCurrentLoggers());
			loggers.add(LogManager.getRootLogger());
			for ( Logger logger : loggers ) {
			    logger.setLevel(Level.OFF);
			    Enumeration<?> enumeration = logger.getAllAppenders();
			    while(enumeration.hasMoreElements()) {
			    	Appender aHat = (Appender) enumeration.nextElement();
			    	aHat.close();
			    };
			};
			LogManager.shutdown();
			System.out.println("로그 매니저를 중지하였습니다.");
			
			data_flow_pipe.close();
			System.out.println("데이터 플로우 파이프를 중지하였습니다.");
			
			DBDataPointBufferFactory.getDBDataBuffer().stop();
			System.out.println("데이터베이스 저장 버퍼를 중지하였습니다.");
			
			// 잡스케쥴러 중지
			JobSchedulerFactory.getInstance().resumeAll();
			JobSchedulerFactory.getInstance().shutdown();
			System.out.println("잡 스케쥴러를 중지하였습니다.");
			//

			// JDBC 데이터소스 중지
			meta_checkpoint_timer.stop();
			HSQLDBDataSource.getInstance().close();
			Enumeration<Driver> drivers = DriverManager.getDrivers();
			while (drivers.hasMoreElements()) {
				Driver driver = drivers.nextElement();
				try {
					DriverManager.deregisterDriver(driver);
				} catch (SQLException e) {
					log.error("JDBC 드라이버 제거 에러 : " + driver, e);
				}
			}
			;
			System.out.println("JDBC 데이터소스를 중지하였습니다.");

			// 타이머 중지
			test_event_timer.stop();
			temp_del_timer.stop();
			// java_gc_timer.stop();
			query_expire_timer.stop();
			hdd_monitoring_timer.stop();
			cassandra_health_timer.stop();
			mq_and_db_health_timer.stop();
			data_flow_error_timer.stop();
			network_latency_timer.stop();
			performance_alarm_timer.stop();
			system_run_duration_timer.stop();
			jmx_tdb_sync_timer.start();
			point_stream_count_timer.stop();
			point_stream_timeout_timer.stop();
			storage_session_monitoring_timer.stop();
			jmx.stop();
			// ntpdate_sync_timer.stop();
			System.out.println("모니터링 타이머를 중지하였습니다.");

			//
			TimerExecuterPool.getInstance().shutdown();
			System.out.println("타이머 풀을 중지하였습니다.");

			// 메세지 브로커 중지
			MessageServerBroker broker = MultiProtocolMessageServerBroker.getInstance();
			broker.shutdown();
			System.out.println("멀티 프로토콜 메세지 브로커를 중지하였습니다.");
			

			// 스트림 프로세서 중지
			stream_processor.stop();
			System.out.println("스트림 프로세서를 중지하였습니다.");

			// 스토리지 프로세서 중지
			storage_processor.stop();
			System.out.println("스토리지 프로세서를 중지하였습니다.");

			// CEP 엔진 중지
			cep_service_monitor.stop();
			provider.getEPAdministrator().destroyAllStatements();
			provider.destroy();
			System.out.println("CEP 엔진을 중지하였습니다.");

			// 시계열 디비 중지
			this.getTimeseries_database().close();
			System.out.println("시계열 데이터베이스 클라이언트를 중지하였습니다.");

			// 레디스 중지
			RedisInMemoryClient.getInstance().shutdown();
			System.out.println("인-메모리 레디스 클라이언트를 중지하였습니다.");

			// 카산드라 세션 중지
			Map<String, Session> c_s_map = SessionCacheFactory.getInstance().getMap();
			for( String key : c_s_map.keySet() ){
				Session session = c_s_map.get(key);
				if(!session.isClosed()) {
					session.close();
					if(!session.getCluster().isClosed()) {
						session.getCluster().close();
					}
				}
		    }
			System.out.println("데이터베이스 카산드라 연결을 중지하였습니다.");
			

			// 비동기 실행 풀 중지
			AsynchronousExecutorPool.getInstance().shutdown();
			System.out.println("비동기 쓰레드 풀링 시스템을 중지하였습니다.");
			
			// 종료 메세지
			System.out.println("플랜트펄스 플랫폼의 모든 서비스가 정상적으로 종료되었습니다.");
			started.set(false);
			
		}
	}

	public EPServiceProvider getProvider() {
		if (provider == null) {
			log.error("CEPEngineManager is not stated.");
			return null;
		}
		;
		return provider;
	}

	public boolean isStarted() {
		return started.get();
	}

	public Date getStartedDate() {
		return started_date;
	}

	public MessageServerBroker getMessageBroker() {
		return message_broker;
	}

	public StorageProcessor getStorage_processor() {
		return storage_processor;
	}

	public void setStorage_processor(StorageProcessor storage_processor) {
		this.storage_processor = storage_processor;
	}

	public DataFlowAdjuster getData_flow_adjuster() {
		return data_flow_adjuster;
	}

	public void setData_flow_adjuster(DataFlowAdjuster data_flow_adjuster) {
		this.data_flow_adjuster = data_flow_adjuster;
	}

	public SiteMetaModelUpdater getSite_meta_model_updater() {
		return site_meta_model_updater;
	}

	public void setSite_meta_model_updater(SiteMetaModelUpdater site_meta_model_updater) {
		this.site_meta_model_updater = site_meta_model_updater;
	}

	public StreamProcessor getStream_processor() {
		return stream_processor;
	}

	public StorageSessionMonitorTimer getStorage_session_monitoring_timer() {
		return storage_session_monitoring_timer;
	}

	public TimeSeriesDatabase getTimeseries_database() {
		return timeseries_database;
	}

	public Date getStart_date() {
		return start_date;
	}

	public Date getStarted_date() {
		return started_date;
	}

	public boolean isShutdown() {
		return is_shutdown;
	}

	public DataDistributionService getData_distribution_service() {
		return data_distribution_service;
	}

	public DataFlowPipe getData_flow_pipe() {
		return data_flow_pipe;
	}

	public MQandDBHealthCheckTimer getMq_and_db_health_timer() {
		return mq_and_db_health_timer;
	}

	public PipelineMetricRegistry getPipeline_metric_registry() {
		return pipeline_metric_registry;
	}

}
