package plantpulse.plugin.opcua;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import plantpulse.json.JSONArray;
import plantpulse.json.JSONObject;
import plantpulse.plugin.opcua.config.JSONConfigLoader;
import plantpulse.plugin.opcua.dao.TagDAO;
import plantpulse.plugin.opcua.datasouce.HSQLDBDataSource;
import plantpulse.plugin.opcua.messaging.KafkaPluginMessageListener;
import plantpulse.plugin.opcua.messaging.PluginMessageListener;
import plantpulse.plugin.opcua.server.PlantPulseNamespace;
import plantpulse.plugin.opcua.server.PlantPulseServer;
import plantpulse.plugin.opcua.utils.ConstantsJSON;
import plantpulse.plugin.opcua.utils.OPCServerTagInsert;

/**
 * Main
 * @author leesa
 *
 */
public class Main {
	
	private static final Log log = LogFactory.getLog(Main.class);
	
	private static PlantPulseServer server;
	private static PlantPulseNamespace namespace  ;
	private static PluginMessageListener p_listener = null;
	
	public static void main(String[] args){
		try {

			
			String config_path = args[0];
			String source_type = args[1];

			System.out.println("=================================================================");
			System.out.println("||| PLANTPULSE OPC-UA SERVER PLUGIN START...");
			System.out.println("=================================================================");
			System.out.println("CONFIG_PATH=[" + config_path + "]"); //
			System.out.println("Configuration load...");
			ConstantsJSON.setConfig(JSONConfigLoader.fileLoad(config_path));

			log.info("OPC-UA 서버 플러그-인 시작중 ...");
			
			server = new PlantPulseServer();
			server.startup();
			log.info("OPC-UA 서버를 시작하였습니다.");
			
			PlantPulseNamespace namespace = server.getNamespace();
			log.info("OPC-UA 서버의 네임스페이스를 정상적으로 가져오기 하였습니다.");

			//
			p_listener = new KafkaPluginMessageListener(namespace);
			p_listener.init();
			p_listener.startListener();
			log.info("포인트 메세지 수신자를 시작하였습니다.");

		
			//태그로 등록할 데이터 불러옴
			JSONArray list = null;
			if("db".equals(source_type)){
				HSQLDBDataSource.getInstance().init();
				TagDAO tdao = new TagDAO();
				list = tdao.selectTagList();
			}else{
				String csv_path = source_type;
				OPCServerTagInsert opcsti = new OPCServerTagInsert();
				opcsti.setPath(csv_path);
				list = opcsti.insertTagInfo();
			}
			
			//
			log.info("등록될 전체 태그 건수 : 노드=[" + list.size() + "], 소스=[" + source_type + "]");

			//태그를 OPC-노드로 등록
			int i = 0;
			for(Object o : list){
				JSONObject tag = JSONObject.fromObject(o);
			    String node_id = namespace.addNode(tag);
			    log.info("태그 등록 : 노드_ID=[" + node_id + "], 태그=[" + tag.toString() + "]");
			};

			//셧다운 훅 처리
			Runtime.getRuntime().addShutdownHook(new ProcessorHook(server, namespace, p_listener));
				
			//
			log.info("OPC-UA 서버 [TCP] = opc.tcp://0.0.0.0:" + PlantPulseServer.TCP_BIND_PORT + "/opcua");
			log.info("OPC-UA 서버 [SSL] = opc.tcp://0.0.0.0:" + PlantPulseServer.HTTPS_BIND_PORT + "/opcua");
			
			//
			log.info("OPC-UA 서버 플러그-인을 정상적으로 시작하였습니다.");

			
			System.out.println("=================================================================");
			System.out.println("OPC-UA server started successfully."); //


		} catch (Exception e) {
			log.error("OPC-UA 서버 시작 실패: " + e.getMessage(), e);
			if(p_listener != null) {
				p_listener.stopListener();
				p_listener.close();
			}
			
			if(namespace != null) namespace.shutdown();
			if(server != null) server.shutdown();
			log.error("OPC-UA 서버 및 네임스페이스가 셧다운 되었습니다.");
			
		}finally {
			//
		}
	}

}
