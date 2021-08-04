package plantpulse.plugin.aas.server;

import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.eclipse.basyx.aas.manager.ConnectedAssetAdministrationShellManager;
import org.eclipse.basyx.aas.metamodel.api.parts.asset.AssetKind;
import org.eclipse.basyx.aas.metamodel.map.AssetAdministrationShell;
import org.eclipse.basyx.aas.metamodel.map.descriptor.CustomId;
import org.eclipse.basyx.aas.metamodel.map.parts.Asset;
import org.eclipse.basyx.aas.registration.proxy.AASRegistryProxy;
import org.eclipse.basyx.components.aas.AASServerComponent;
import org.eclipse.basyx.components.aas.configuration.AASServerBackend;
import org.eclipse.basyx.components.aas.configuration.BaSyxAASServerConfiguration;
import org.eclipse.basyx.components.configuration.BaSyxContextConfiguration;
import org.eclipse.basyx.components.registry.RegistryComponent;
import org.eclipse.basyx.components.registry.configuration.BaSyxRegistryConfiguration;
import org.eclipse.basyx.components.registry.configuration.RegistryBackend;
import org.eclipse.basyx.submodel.metamodel.api.identifier.IIdentifier;
import org.eclipse.basyx.submodel.metamodel.api.submodelelement.dataelement.IProperty;
import org.eclipse.basyx.submodel.metamodel.map.Submodel;
import org.eclipse.basyx.submodel.metamodel.map.submodelelement.dataelement.property.Property;

import plantpulse.json.JSONObject;
import plantpulse.json.JSONArray;

import plantpulse.domain.Metadata;
import plantpulse.domain.Site;
import plantpulse.domain.Tag;
import plantpulse.event.opc.Point;
import plantpulse.plugin.aas.cache.TagCacheFactory;
import plantpulse.plugin.aas.utils.AliasUtils;

/**
 * AASServer
 * 
 * @author leesa
 *
 */
public class AASServer {
	

	private static final Log log = LogFactory.getLog(AASServer.class);
	
	public static final int REGISTRY_PORT = 4800;
	public static final int AAS_PORT      = 4801;
	
	// Server URLs
	public static final String REGISTRY_SERVER_URL = "http://127.0.0.1:" + REGISTRY_PORT + "/registry";
	public static final String AAS_SERVER_URL = "http://127.0.0.1:" + AAS_PORT + "/aas";


	private RegistryComponent registry = null;
	private AASServerComponent aasServer = null;
	private ConnectedAssetAdministrationShellManager manager = null;
	
	private Map<String, Submodel> info_submodel_map   = new HashMap<>();
	private Map<String, Submodel> tag_submodel_map   = new HashMap<>();
	private Map<String, Submodel> point_submodel_map = new HashMap<>();
	private Map<String, Submodel> alarm_submodel_map = new HashMap<>();
	private Map<String, Submodel> timeline_submodel_map = new HashMap<>();
	private Map<String, Submodel> metadata_submodel_map = new HashMap<>();
	
	
	private DateFormat date_format = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS");
	
	/**
	 * AAS 시작
	 * @throws Exception
	 */
	public void start() throws Exception {
		
		// Create Infrastructure
		startRegistry();
		startAASServer();
		//
		Thread.sleep(1 * 1000);
		
		// Create Manager - This manager is used to interact with an AAS server
		 manager = new ConnectedAssetAdministrationShellManager(new AASRegistryProxy(REGISTRY_SERVER_URL));
	}
	
	/**
	 * AAS 중지
	 */
	public void stop () {
		stopRegistry();
		stopAASServer();
	}

	/**
	 * 사이트 AAS 생성
	 * @param site
	 */
	public void createSite(Site site) {
		CustomId asset_id = new CustomId("BASYX.ASSET.PLANTPULSE." + site.getSite_id());
		CustomId aas_id   = new CustomId("BASYX.AAS.PLANTPULSE." + site.getSite_id());
		//
		Asset asset = new Asset(site.getSite_id(), asset_id, AssetKind.INSTANCE);
		AssetAdministrationShell shell = new AssetAdministrationShell(site.getSite_name(), aas_id, asset);
		// The manager uploads the AAS and registers it in the Registry server
		manager.createAAS(shell, AAS_SERVER_URL);
				
		// Create submodel
		IIdentifier info_id = new CustomId("BASYX.SUBMODEL.PLANTPULSE." + site.getSite_id() + ".INFO");
		Submodel info_submodel = new Submodel("INFO", info_id);
		info_submodel.addSubmodelElement(new Property("ID", site.getSite_id()));
		info_submodel.addSubmodelElement(new Property("NAME", site.getSite_name()));
		info_submodel.addSubmodelElement(new Property("LAT", site.getLat()));
		info_submodel.addSubmodelElement(new Property("LNG", site.getLng()));
		info_submodel.addSubmodelElement(new Property("DESCRIPTION", site.getDescription()));
		// Push the Submodel to the AAS server
		manager.createSubmodel(shell.getIdentification(), info_submodel);	
	};
	
	/**
	 * 에셋 AAS 생성
	 * @param passet
	 * @param tag_list
	 * @param metadata_list
	 */
	public void createAsset(plantpulse.domain.Asset passet, List<plantpulse.domain.Tag> tag_list, List<plantpulse.domain.Metadata> metadata_list) {
		
		//에셋 만들기
		CustomId asset_id = new CustomId("BASYX.ASSET.PLANTPULSE." + passet.getSite_id() + "." + passet.getAsset_id());
		CustomId aas_id   = new CustomId("BASYX.AAS.PLANTPULSE." + passet.getSite_id() + "." + passet.getAsset_id());
		
		//
		Asset asset = new Asset(passet.getAsset_id(), asset_id, AssetKind.INSTANCE);
		AssetAdministrationShell shell = new AssetAdministrationShell(passet.getAsset_name(), aas_id, asset);
		manager.createAAS(shell, AAS_SERVER_URL);
		
		//---------------------------------------------------------------------------------------------
		//서브모델 목록 : 기본정보/태그/포인트/알람/타임라인/메타데이터
		//---------------------------------------------------------------------------------------------
		
		//0. 기본 정보 등록
		IIdentifier info_id = new CustomId("BASYX.SUBMODEL.PLANTPULSE." + passet.getSite_id() + "." + passet.getAsset_id() + ".INFO");
		Submodel info_submodel = new Submodel("INFO", info_id);
		info_submodel.addSubmodelElement(new Property("ID", passet.getAsset_id()));
		info_submodel.addSubmodelElement(new Property("NAME", passet.getAsset_name()));
		info_submodel.addSubmodelElement(new Property("TYPE", passet.getAsset_type()));
		info_submodel.addSubmodelElement(new Property("IMAGE",       (passet.getAsset_svg_img() == null) ? "" : passet.getAsset_svg_img()));
		info_submodel.addSubmodelElement(new Property("TABLE_TYPE",  (passet.getTable_type() == null) ? "" : passet.getTable_type()));
		info_submodel.addSubmodelElement(new Property("DESCRIPTION", passet.getDescription()));
		info_submodel.addSubmodelElement(new Property("INSERT_DATE", passet.getInsert_date()));
		info_submodel.addSubmodelElement(new Property("UPDATE_DATE", (passet.getUpdate_date() == null) ? "" : passet.getUpdate_date()));
		manager.createSubmodel(shell.getIdentification(), info_submodel);
		info_submodel_map.put(info_id.getId(), info_submodel);
		
		// 1. 태그 서브모델 등록
		IIdentifier tag_id = new CustomId("BASYX.SUBMODEL.PLANTPULSE." + passet.getSite_id() + "." + passet.getAsset_id() + ".TAG");
		Submodel tag_submodel = new Submodel("TAG", tag_id);
		for(int i=0; i < tag_list.size(); i++) {
			Tag tag = tag_list.get(i);
			TagCacheFactory.getInstance().put(tag.getTag_id(), tag);
			Property prop = new Property(AliasUtils.getAliasName(tag), JSONObject.fromObject(tag).toString());
			tag_submodel.addSubmodelElement(prop);
			log.debug("Tag submodel property added : tag_id=["+tag.getTag_id()+"], alias_name=[" + AliasUtils.getAliasName(tag) +"]");
		};
		manager.createSubmodel(shell.getIdentification(), tag_submodel);
		tag_submodel_map.put(tag_id.getId(), tag_submodel);
				
		// 2. 포인트 서브모델 등록
		IIdentifier point_id = new CustomId("BASYX.SUBMODEL.PLANTPULSE." + passet.getSite_id() + "." + passet.getAsset_id() + ".POINT");
		Submodel point_submodel = new Submodel("POINT", point_id);
		for(int i=0; i < tag_list.size(); i++) {
			Tag tag = tag_list.get(i);
			Point point = new Point();
			Property prop = new Property(AliasUtils.getAliasName(tag), JSONObject.fromObject(point).toString());
			point_submodel.addSubmodelElement(prop);
			log.debug("Point submodel property added : tag_id=["+tag.getTag_id()+"], alias_name=[" + AliasUtils.getAliasName(tag) +"]");
		};
		manager.createSubmodel(shell.getIdentification(), point_submodel);
		point_submodel_map.put(point_id.getId(), point_submodel);
		
		// 3. 타임라인 서브모델 등록
		IIdentifier alarm_id = new CustomId("BASYX.SUBMODEL.PLANTPULSE." + passet.getSite_id() + "." + passet.getAsset_id() + ".ALARM");
		Submodel alarm_submodel = new Submodel("ALARM", alarm_id);
		Property alarm_date       = new Property("UPDATE_TIMESTAMP", (new Date()).getTime());
		Property alarm_json_array = new Property("ARRAY",  (new JSONArray()).toString());
		alarm_submodel.addSubmodelElement(alarm_date);
		alarm_submodel.addSubmodelElement(alarm_json_array);
		log.debug("Alarm submodel property added : asset_id=[" + passet.getAsset_id() + "]");
		manager.createSubmodel(shell.getIdentification(), alarm_submodel);
		alarm_submodel_map.put(alarm_id.getId(), alarm_submodel);
				
		// 4. 타임라인 서브모델 등록
		IIdentifier timeline_id = new CustomId("BASYX.SUBMODEL.PLANTPULSE." + passet.getSite_id() + "." + passet.getAsset_id() + ".TIMELINE");
		Submodel timeline_submodel = new Submodel("TIMELINE", timeline_id);
		Property timeline_date       = new Property("UPDATE_TIMESTAMP", (new Date()).getTime());
		Property timeline_json_array = new Property("ARRAY",  (new JSONArray()).toString());
		timeline_submodel.addSubmodelElement(timeline_date);
		timeline_submodel.addSubmodelElement(timeline_json_array);
		log.debug("Timeline submodel property added : asset_id=[" + passet.getAsset_id() + "]");
		manager.createSubmodel(shell.getIdentification(), timeline_submodel);
		timeline_submodel_map.put(timeline_id.getId(), timeline_submodel);

		//5. 메타데이터 서브모델 등록
		IIdentifier metadata_id = new CustomId("BASYX.SUBMODEL.PLANTPULSE." + passet.getSite_id() + "." + passet.getAsset_id() + ".METADATA");
		Submodel metadata_submodel = new Submodel("METADATA", metadata_id);
		for(int i=0; i < metadata_list.size(); i++) {
			Metadata metadata = metadata_list.get(i);
			Property prop = new Property(metadata.getKey(), metadata.getValue());
			metadata_submodel.addSubmodelElement(prop);
			log.debug("Metadata submodel property added : asset_id=[" + metadata.getObject_id() + "], key=["+ metadata.getKey()+"], value=[" + metadata.getValue() +"]");
		};
		manager.createSubmodel(shell.getIdentification(), metadata_submodel);
		metadata_submodel_map.put(metadata_id.getId(), metadata_submodel);
		
		log.info("Asset created : asset_id=["+passet.getAsset_id()+"], tag_size=[" + tag_list.size() +"], metadata_size=[" + metadata_list.size() + "]");
	};
	
	/**
	 * updatePoint
	 * @param point
	 */
	public void updatePoint(Point point) {
		Tag tag = TagCacheFactory.getInstance().get(point.getTag_id());
		if(tag == null) {
			log.error("Tag is NULL : " + point.getTag_id());
			return ;
		}
		long timestamp = point.getTimestamp();
		String point_id = "BASYX.SUBMODEL.PLANTPULSE." + tag.getSite_id() + "." + tag.getLinked_asset_id() + ".POINT";
		Submodel point_submodel = point_submodel_map.get(point_id); //
		if(point_submodel != null) {
			IProperty prop = point_submodel.getProperties().get(AliasUtils.getAliasName(tag));
			if(prop != null) {
				prop.setValue(JSONObject.fromObject(point).toString());
			}else {
				log.error("Point proprerty is NULL : " + AliasUtils.getAliasName(tag));
			}
        }else {
        	log.error("Point submodel is NULL : " + point_id);
        }
		//
		log.debug("[" + point_id + "] Point changed : asset_id=[" + tag.getLinked_asset_id() + "], alias_name=[" + AliasUtils.getAliasName(tag) + "], value=[" + point.getValue() + "], timestamp=[" + date_format.format(new Date(timestamp)) + "]");
	}
	
	
	/**
	 * updateAlarm
	 * @param json_array
	 */
	public void updateAlarm(plantpulse.domain.Asset passet, long timestamp, JSONArray array) {

		String alarm_id = "BASYX.SUBMODEL.PLANTPULSE." + passet.getSite_id() + "." + passet.getAsset_id() + ".ALARM";
		Submodel alarm_submodel = alarm_submodel_map.get(alarm_id); //
		if(alarm_submodel != null) {
			alarm_submodel.getProperties().get("UPDATE_TIMESTAMP").setValue(timestamp);
			alarm_submodel.getProperties().get("ARRAY").setValue(array.toString());
		}else {
			log.error("Alarm submodel is NULL : " + alarm_id);
		}
	}
	
	/**
	 * updateTimeline
	 * @param json_array
	 */
	public void updateTimeline(plantpulse.domain.Asset passet, long timestamp, JSONArray array) {
		String timeline_id = "BASYX.SUBMODEL.PLANTPULSE." + passet.getSite_id() + "." + passet.getAsset_id() + ".TIMELINE";
		Submodel timeline_submodel = timeline_submodel_map.get(timeline_id); //
		if(timeline_submodel != null) {
			timeline_submodel.getProperties().get("UPDATE_TIMESTAMP").setValue(timestamp);
			timeline_submodel.getProperties().get("ARRAY").setValue(array.toString());
		}else {
			log.error("Timeline submodel is NULL : " + timeline_id);
		}
	}

	
	/**
	 * Starts an empty registry 
	 */
	private  void startRegistry() {
		BaSyxContextConfiguration contextConfig = new BaSyxContextConfiguration(REGISTRY_PORT, "/registry");
		BaSyxRegistryConfiguration registryConfig = new BaSyxRegistryConfiguration(RegistryBackend.INMEMORY);
		registry = new RegistryComponent(contextConfig, registryConfig);
		// Start the created server
		registry.startComponent();
	}

	/**
	 * Startup an empty server 
	 */
	private  void startAASServer() {
		BaSyxContextConfiguration contextConfig = new BaSyxContextConfiguration(AAS_PORT, "/aas");
		BaSyxAASServerConfiguration aasServerConfig = new BaSyxAASServerConfiguration(AASServerBackend.INMEMORY, "", REGISTRY_SERVER_URL);
		aasServer = new AASServerComponent(contextConfig, aasServerConfig);

		// Start the created server
		aasServer.startComponent();
	}
	
	public String getRegistryServerUrl() {
		return REGISTRY_SERVER_URL;
	}
	
	public String getAASServerUrl() {
		return AAS_SERVER_URL;
	}
	
	
	private  void stopRegistry() {
		registry.stopComponent();
	}
	
	private  void stopAASServer() {
		aasServer.stopComponent();
	}

	public RegistryComponent getRegistry() {
		return registry;
	}

	public AASServerComponent getAasServer() {
		return aasServer;
	}

	public ConnectedAssetAdministrationShellManager getManager() {
		return manager;
	};
	
	
}
