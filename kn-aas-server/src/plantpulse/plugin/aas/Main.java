package plantpulse.plugin.aas;

import java.util.ArrayList;
import java.util.List;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import plantpulse.domain.Asset;
import plantpulse.domain.Metadata;
import plantpulse.domain.Site;
import plantpulse.domain.Tag;
import plantpulse.plugin.aas.asset.PlantPulseAssetList;
import plantpulse.plugin.aas.config.JSONConfigLoader;
import plantpulse.plugin.aas.dao.AssetDAO;
import plantpulse.plugin.aas.datasouce.HSQLDBDataSource;
import plantpulse.plugin.aas.messaging.KafkaPluginMessageListener;
import plantpulse.plugin.aas.messaging.PluginMessageListener;
import plantpulse.plugin.aas.server.AASServer;
import plantpulse.plugin.aas.test.TestTimer;
import plantpulse.plugin.aas.timer.AssetDBDataFindTimer;
import plantpulse.plugin.aas.timer.DataSaveTimer;
import plantpulse.plugin.aas.utils.ConstantsJSON;

/**
 * Main
 * 
 * @author leesa
 *
 */
public class Main {
	

	private static final Log log = LogFactory.getLog(Main.class);

	public static void main(String[] args) {
		
		try {
			

			String config_path = args[0];

			System.out.println("=================================================================");
			System.out.println("||| AAS SERVER PLUGIN START...");
			System.out.println("=================================================================");
			System.out.println("CONFIG_PATH=[" + config_path + "]"); //
			System.out.println("Configuration Load...");
			ConstantsJSON.setConfig(JSONConfigLoader.fileLoad(config_path));
			
			//
			log.info("AAS 플러그-인 시작중...");
			//
			AASServer server = new AASServer();
			server.start();
			log.info("REGISTRY_SERVER_URL : " + server.getRegistryServerUrl());
			log.info("AAS_SERVER_URL : " + server.getAASServerUrl());
			//
			log.info("AAS 서버를 시작하였습니다.");
			
			//
			HSQLDBDataSource.getInstance().init();
			log.info("JDBC 데이터소스 초기화를 완료하였습니다.");
			//
			PlantPulseAssetList asset_list = new PlantPulseAssetList(server);
			List<Asset> total_asset_list = asset_list.totalAssets();
			log.info("사이트 및 에셋 모델 생성을 완료하였습니다.");
			
			
			//데이터베이스에서 가져와야 하는 것
			DataSaveTimer timer = new DataSaveTimer(server, total_asset_list);
			timer.start();
			log.info("AAS 데이터 저장 타이머를 시작하였습니다.");
			
			/*
			//데이터베이스에서 가져와야 하는 것
			AssetDBDataFindTimer timer = new AssetDBDataFindTimer(server, total_asset_list);
			timer.start();
			log.info("에셋 데이터 DB 파인더를 시작하였습니다.");
			
			//메세징서버에서 가져와야 하는 것
			PluginMessageListener p_listener = new KafkaPluginMessageListener(server);
			p_listener.init();
			p_listener.startListener();
			log.info("에셋 데이터 메세지 수신자를 시작하였습니다.");
			*/
			
			Thread.sleep(2 * 1000);

			//테스트 타이머
			TestTimer test = new TestTimer(server);
			test.start();
			
			//셧다운 훅 처리
			Runtime.getRuntime().addShutdownHook(new ProcessorHook(server, null));
			
			log.info("AAS 플러그-인을 정상적으로 시작하였습니다.");
			
		} catch (Exception ex) {
			log.error("AAS 플러그-인 시작중 오류가 발생하였습니다 : " + ex.getMessage() , ex);
		}

	}

}
