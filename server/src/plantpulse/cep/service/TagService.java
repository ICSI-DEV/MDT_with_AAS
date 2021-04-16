package plantpulse.cep.service;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.commons.lang.StringUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import plantpulse.cep.dao.TagDAO;
import plantpulse.cep.engine.deploy.AssetTemplateToEventDeployer;
import plantpulse.cep.engine.utils.TagUtils;
import plantpulse.cep.service.client.StorageClient;
import plantpulse.cep.service.support.alarm.UserDefinedTagAlarm;
import plantpulse.cep.service.support.alarm.UserDefinedTagAlarmDeployer;
import plantpulse.cep.service.support.alarm.eql.BooleanTypeEQLGenerator;
import plantpulse.cep.service.support.alarm.eql.NumbericTypeEQLGenerator;
import plantpulse.cep.service.support.security.SecurityTools;
import plantpulse.cep.service.support.tag.aggregation.TagAggregationDeployer;
import plantpulse.cep.service.support.tag.calculation.TagCalculationDeployer;
import plantpulse.cep.service.support.tag.forecast.ForecastTagDeployer;
import plantpulse.domain.AlarmConfig;
import plantpulse.domain.Tag;

@Service
public class TagService {

	private static final Log log = LogFactory.getLog(TagService.class);
	
	
	@Autowired
	private TagDAO tag_dao;

	@Autowired
	private StorageClient storage_client;
	

	public long selectTagCountTotal() {
		try {
			return tag_dao.selectTagCountTotal();
		} catch (Exception e) {
			return 0;
		}
	}
	
	public long selectTagCountByLinkedAssetId(String asset_id) {
		try {
			return tag_dao.selectTagCountByLinkedAssetId(asset_id);
		} catch (Exception e) {
			return 0;
		}
	}

	public List<Map<String, String>> selectTagAllForSelect() throws Exception {
		List<Map<String, String>> result = new ArrayList<Map<String, String>>();
		try {
			List<Map<String, String>> list = tag_dao.selectTagAllForSelect();
			for (int i = 0; list != null && i < list.size(); i++) {
				Map<String, String> one = list.get(i);
				if (SecurityTools.hasPermission(one.get("id"))) {
					result.add(one);
				}
			}
			return result;
		} catch (Exception e) {
			log.warn("Can not read OPC Tree data." + e.getMessage(), e);
			return null;
		}
	}

	public Map<String, String> selectTagMapForSelect() throws Exception {
		List<Map<String, String>> list = tag_dao.selectTagAllForSelect();
		Map<String, String> map = new HashMap<String, String>();
		for (int i = 0; i < list.size(); i++) {
			Map<String, String> one = list.get(i);
			if (SecurityTools.hasPermission(one.get("id"))) {
				map.put(one.get("id"), one.get("name"));
			}
		}
		return map;
	}

	/**
	 * opc_id 또는 linked_asset_id로 tag list를 조회한다.
	 * 
	 * @param tag
	 * @param searchType
	 *            ("opc" or "asset")
	 * @return
	 */
	public List<Tag> selectTagList(Tag tag, String searchType) {
		try {
			return tag_dao.selectTagList(tag, searchType);
		} catch (Exception e) {
			return null;
		}
	}
	
	public List<Tag> selectTagListByAssetId(String asset_id) {
		try {
			return tag_dao.selectTagListByAssetId(asset_id);
		} catch (Exception e) {
			return null;
		}
	}
	
	
	public List<Tag> selectTagListByAssetId(String asset_id, int limit) {
		try {
			return tag_dao.selectTagListByAssetId(asset_id, limit);
		} catch (Exception e) {
			return null;
		}
	}
	
	public List<Tag> selectTagListByOpcId(String opc_id) {
		try {
			return tag_dao.selectTagListByOpcId(opc_id);
		} catch (Exception e) {
			return null;
		}
	};
	
	public List<Tag> selectTagList(Tag tag, String searchType, int offset) {
		try {
			return tag_dao.selectTagList(tag, searchType, offset);
		} catch (Exception e) {
			return null;
		}
	}

	public Tag selectTag(String tag_id) throws Exception {
		return tag_dao.selectTag(tag_id);
	}

	public Tag selectTagInfo(String tagID) {
		try {
			return tag_dao.selectTag(tagID);
		} catch (Exception e) {
			log.warn("Can not read Point info. : " + e.getMessage(), e);
			return null;
		}
	}

	public Tag selectTagInfoWithSiteAndOpc(String tagID) {
		try {
			return tag_dao.selectTagInfoWithSiteAndOpc(tagID);
		} catch (Exception e) {
			log.warn("Can not read Point info. : " + e.getMessage(), e);
			return null;
		}
	}

	public void updateTagAsset(String tagID, String linkedAssetID) throws Exception {
		tag_dao.updateTagAsset(tagID, linkedAssetID);
		//
		AssetTemplateToEventDeployer deployer = new AssetTemplateToEventDeployer();
		deployer.deploy();
	}

	public void deleteTagMappingInfo(Tag tag) throws Exception {
		tag_dao.deleteTagMappingInfo(tag);
	}

	public void deleteAllTagMappingInfo(String[] selectedAssetIDs) throws Exception {
		for (int i = 0; i < selectedAssetIDs.length; i++) {
			tag_dao.deleteTagMappingInfoByTagID(selectedAssetIDs[i]);
		}
	}

	/**
	 * tag_id 또는 opc_id로 Tag를 삭제한다.
	 * 
	 * @param tag
	 * @param deleteType  ("tag_id" or "opc_id" 기준으로 삭제)
	 * @throws Exception
	 */
	public void deleteTag(Tag tag) throws Exception {
		
		//태그에 연결된 알람 설정 제거
	    AlarmConfigService service = new AlarmConfigService();
	    service.rmoveAllAlarmByTag(tag.getTag_id()); 
	    // 예측, 집계, 계산 설정 제거
		ForecastTagDeployer forecast_deployer = new ForecastTagDeployer();
		forecast_deployer.undeploy(tag);
		TagAggregationDeployer agg_deployer = new TagAggregationDeployer();
		agg_deployer.undeploy(tag);
		TagCalculationDeployer cal_deployer = new TagCalculationDeployer();
		cal_deployer.undeploy(tag);
		
	    //TODO 태그 삭제시, 알람과 대시보드에서 연관된 건을 삭제처리해야 함.
		
		tag_dao.deleteTag(tag);
		
	}
	
	
	/**
	 * 
	 * @param list
	 * @throws Exception
	 */
	public void deleteTagList(List<Tag> list) throws Exception {
		for (Tag tag : list) {
			deleteTag(tag);
		};
	}

	/**
	 * 
	 * @param list
	 * @throws Exception
	 */
	public void deleteTagsByIdList(List<String> list) throws Exception {
		//
		for (String tagId : list) {
			Tag tag = new Tag();
			tag.setTag_id(tagId);
			deleteTag(tag);
		}
	}
	
	public void insertVituralTag(Tag tag) throws Exception {
		tag_dao.insertVituralTag(tag);
	}

	public void updateTag(Tag tag) throws Exception {
		tag_dao.updateTag(tag);
	}

	public List<Map<String, Object>> selectTagConfigList() {
		try {
			return tag_dao.selectTagConfigList();
		} catch (Exception e) {
			log.error("Can not read TagConfig list(Company/Site/OPC/Tag's List). : " + e.getMessage(), e);
			return null;
		}
	}
	
	
	public List<Map<String, Object>> searchTagConfigList(Tag tag) {
		return searchTagConfigList(tag, 0);
	}

	public List<Map<String, Object>> searchTagConfigList(Tag tag, int limit) {
		List<Map<String, Object>> list = new ArrayList<Map<String, Object>>();
		try {
			
			long start = System.currentTimeMillis();
			
			log.debug("Tag config list search : tag=[" + tag.toString() + "], limit=[" + limit + "]");
			
			//
			//TODO 사용자별 퍼미션이 있는, 태그검색으로 변경, 처음 로그인시, 테이블 빌드
			//전체 검색 후, 필터링 (퍼미션 체크가 필요하기 때문)
			List<Map<String, Object>> result = null;
			if(SecurityTools.isAdminRole()) {
				result = tag_dao.searchTagConfigList(tag, limit); 
			}else {
				result = tag_dao.searchTagConfigList(tag); 	
			};
			
			if(result != null) {
				//
				for (int i = 0; result != null && i < result.size(); i++) {
					Map<String, Object> row = result.get(i);
					Tag param_tag = new Tag();
					param_tag.setTag_id((String) row.get("TAG_ID"));
					long data_count = 0;
					try {
						data_count = storage_client.forSelect().countPoint(param_tag);
					} catch (Exception ex) {
						data_count = 0;
					}
					row.put("DATA_COUNT", data_count);
					//
					if (SecurityTools.hasPermission((String) row.get("TAG_ID"))) {
						list.add(row);
					}
					;
					//
					if(limit >= 0){ //limit 이 0 보다 작을 경우 모두 조회
						if (list.size() >= limit) { // 맥스 태그 조회 사이트
							break;
						}
					}
				};
			};
			
			long end = System.currentTimeMillis() - start;
			log.info("Tag config list search process completed : process_time=[" + end + "]ms");

			return list;
		} catch (Exception e) {
			log.error("Can not read tag config list(Company/Site/OPC/Tag's List). : " + e.getMessage(), e);
			return null;
		}
	}
	
	
	

	public long selectTagCountTotal(Tag tag) {
		List<Map<String, Object>> list = new ArrayList<Map<String, Object>>();
		try {
			List<Map<String, Object>> result = tag_dao.searchTagConfigList(tag);
			for (int i = 0; result != null && i < result.size(); i++) {
				Map<String, Object> row = result.get(i);
				Tag param_tag = new Tag();
				param_tag.setTag_id((String) row.get("TAG_ID"));
				//
				if (SecurityTools.hasPermission((String) row.get("TAG_ID"))) {
					list.add(row);
				}

			}
			return list.size();
		} catch (Exception e) {
			log.error("Can not read TagConfig list(Company/Site/OPC/Tag's List). : " + e.getMessage(), e);
			return 0;
		}
	}

	public void deployAlarm(Tag tag) throws Exception {
		//
		
		AlarmConfigService alarm_config_service = new AlarmConfigService();
		AlarmConfig alarm_config = new AlarmConfig();
		alarm_config.setAlarm_type("TAG");
		alarm_config.setAlarm_config_priority("BAND");
		alarm_config.setAlarm_config_id("ALARM_CONFIG_" + tag.getTag_id());
		alarm_config.setAlarm_config_name("[" + tag.getTag_name() + "] 태그 밴드 알람");
		alarm_config.setTag_id(tag.getTag_id());
		alarm_config.setRecieve_me(tag.isRecieve_me());
		alarm_config.setRecieve_others(tag.getRecieve_others());
		alarm_config.setSend_email(tag.isSend_email());
		alarm_config.setSend_sms(tag.isSend_sms());
		alarm_config.setInsert_user_id("admin");
		
		//
		//Character, Short, Integer, Long, Float, Double
		if(TagUtils.isNumbericDataType(tag)) {
			if (isSetTagBandValue(tag)) { //
				alarm_config.setEpl(NumbericTypeEQLGenerator.getEQL(tag));
				alarm_config_service.enableAlarmConfigForTagBand(alarm_config, tag);
			} else {
				//
				alarm_config.setEpl("");
				alarm_config_service.disableAlarmConfigForTagBand(alarm_config);
			}
	    //Boolean
		}else if(TagUtils.isBooleanDataType(tag)){
			if (isSetTagBooleanValue(tag)) { //
				alarm_config.setEpl(BooleanTypeEQLGenerator.getEQL(tag));
				alarm_config_service.enableAlarmConfigForTagBand(alarm_config, tag);
			} else {
				//
				alarm_config.setEpl("");
				alarm_config_service.disableAlarmConfigForTagBand(alarm_config);
			}
		};
		
		
	}
	
	public void deployUserDefinedAlarm(Tag tag) throws Exception {
		if(StringUtils.isNotEmpty(tag.getUser_defined_alarm_class())){
			UserDefinedTagAlarm user_defined_alarm = (UserDefinedTagAlarm) Class.forName(tag.getUser_defined_alarm_class()).newInstance();
			UserDefinedTagAlarmDeployer deployer = new UserDefinedTagAlarmDeployer();
			//
			deployer.deploy(tag, user_defined_alarm.configure(tag));
		};
	}
	
	
	public void deployCalculation(Tag tag) throws Exception {
		TagCalculationDeployer deployer = new TagCalculationDeployer();
		if ("Y".equals(tag.getUse_calculation())) { 
			deployer.undeploy(tag);
			deployer.deploy(tag);
		}else{
			deployer.undeploy(tag);
		}
	};
	
	
	public void deployAggregation(Tag tag) throws Exception {
		TagAggregationDeployer deployer = new TagAggregationDeployer();
		if ("Y".equals(tag.getUse_aggregation())) { 
			deployer.undeploy(tag);
			deployer.deploy(tag);
		}else{
			deployer.undeploy(tag);
		}
	};
	
	
	public void deployForecast(Tag tag) throws Exception {
		ForecastTagDeployer deployer = new ForecastTagDeployer();
		if ("Y".equals(tag.getUse_ml_forecast())) { 
			deployer.deploy(tag);
		}else{
			deployer.undeploy(tag);
		}
	}
	
	public long selectTagPointCount(Tag tag) {
		long pointCount = 0;
		try {
			return storage_client.forSelect().countPoint(tag);
		} catch (Exception ex) {
			return pointCount;
		}
	};
	
	
	public void refreshTag(Tag _tag) throws Exception{	
		log.debug("[" + _tag.getTag_id() + "] Refrshing ... ");
		if(_tag.getTag_id() == null){
			throw new Exception("TAG_ID is NULL.");
		};
		Tag tag = this.selectTag(_tag.getTag_id());
		if(tag == null){
			throw new Exception("Refresh target tag is NULL.");
		};
		this.deployAlarm(tag);
		this.deployCalculation(tag);
		this.deployAggregation(tag);
		this.deployForecast(tag);
	};
	
	
	private boolean isSetTagBandValue(Tag tag) {
		//
		boolean isNotEmpty = false;
		if (StringUtils.isNotEmpty(tag.getTrip_hi())) {
			isNotEmpty = true;
		} else if (StringUtils.isNotEmpty(tag.getHi_hi())) {
			isNotEmpty = true;
		} else if (StringUtils.isNotEmpty(tag.getHi())) {
			isNotEmpty = true;
		} else if (StringUtils.isNotEmpty(tag.getLo())) {
			isNotEmpty = true;
		} else if (StringUtils.isNotEmpty(tag.getLo_lo())) {
			isNotEmpty = true;
		} else if (StringUtils.isNotEmpty(tag.getTrip_lo())) {
			isNotEmpty = true;
		}
		;
		return isNotEmpty;
	}
	
	
	private boolean isSetTagBooleanValue(Tag tag) {
		if (StringUtils.isEmpty(tag.getBool_true()) && StringUtils.isEmpty(tag.getBool_false())) {
			return false;
		}else {
			return true;
		}

	}
}
