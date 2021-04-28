package plantpulse.cep.service.support.alarm;

import java.util.ArrayList;
import java.util.Date;
import java.util.List;

import org.apache.commons.lang.StringUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.json.JSONObject;

import com.espertech.esper.client.EventBean;
import com.espertech.esper.client.UpdateListener;

import plantpulse.cep.dao.AlarmDAO;
import plantpulse.cep.dao.AssetDAO;
import plantpulse.cep.dao.OPCDAO;
import plantpulse.cep.dao.SecurityDAO;
import plantpulse.cep.dao.SiteDAO;
import plantpulse.cep.dao.TagDAO;
import plantpulse.cep.engine.CEPEngineManager;
import plantpulse.cep.engine.utils.AliasUtils;
import plantpulse.cep.engine.utils.TemplateUtils;
import plantpulse.cep.service.client.StorageClient;
import plantpulse.cep.service.support.tag.TagLocationFactory;
import plantpulse.domain.AlarmConfig;
import plantpulse.domain.Asset;
import plantpulse.domain.OPC;
import plantpulse.domain.Site;
import plantpulse.domain.Tag;

public class EQLAlarmEventListener implements UpdateListener {

	private static final Log log = LogFactory.getLog(EQLAlarmEventListener.class);

	private AlarmConfig alarm_config;

	public EQLAlarmEventListener(AlarmConfig alarm_config) {
		this.alarm_config = alarm_config;
	}

	@Override
	public void update(EventBean[] newEvents, EventBean[] arg1) {

		try {

			if (newEvents != null && newEvents.length > 0) {
				EventBean event = newEvents[0];

				//
				if (event != null) {
					String json_text = CEPEngineManager.getInstance().getProvider().getEPRuntime().getEventRenderer().renderJSON("json", event);
					JSONObject json = JSONObject.fromObject(JSONObject.fromObject(json_text).getJSONObject("json").toString());
					log.debug("Alarm event underlying : json=" + json.toString());

					Date alarm_date = new Date();
					if (json.has("timestamp")) alarm_date = new Date(json.getLong("timestamp"));

					AlarmRejectHandler handler = new AlarmRejectHandler();
					if (!handler.aleadyAlarmed(alarm_config.getAlarm_config_id(), alarm_config.getInsert_user_id(), alarm_config.getAlarm_config_priority())) {

						// 이벤트 전송
						TagDAO tag_dao = new TagDAO();
						Tag tag = tag_dao.selectTag(alarm_config.getTag_id());
						
						OPCDAO opc_dao = new OPCDAO();
						OPC opc = opc_dao.selectOpc(tag.getOpc_id());
						
						AssetDAO asset_dao = new AssetDAO();
						Asset asset = asset_dao.selectAsset(tag.getLinked_asset_id());
						
						SiteDAO site_dao = new SiteDAO();
						Site site = site_dao.selectSite(tag.getSite_id());
						
						AlarmDAO alarm_dao = new AlarmDAO();
						long alarm_seq = alarm_dao.getNextAlarmSeq();

						plantpulse.event.opc.Alarm alarm = new plantpulse.event.opc.Alarm();
						alarm.setTimestamp(alarm_date.getTime());
						alarm.setSite_id(tag.getSite_id());
						alarm.setOpc_id(tag.getOpc_id());
						alarm.setTag_id(alarm_config.getTag_id());
						alarm.setPriority(alarm_config.getAlarm_config_priority());
						alarm.setDescription(TemplateUtils.parse(alarm_config.getMessage(), json));
						alarm.setConfig_id(alarm_config.getAlarm_config_id());
						CEPEngineManager.getInstance().getProvider().getEPRuntime().sendEvent(alarm);

						// 카산드라 저장
						JSONObject data = new JSONObject();
						data.put("site_id", tag.getSite_id());
						data.put("opc_id", tag.getOpc_id());
						data.put("tag_id", alarm_config.getTag_id());
						data.put("tag_name", tag.getTag_name());
						data.put("asset_id", tag.getLinked_asset_id());
						data.put("alias_name", AliasUtils.getAliasName(tag));
						data.put("timestamp", alarm_date.getTime() + "");
						data.put("priority", alarm_config.getAlarm_config_priority());
						data.put("description", TemplateUtils.parse(alarm_config.getMessage(), json));
						data.put("alarm_seq", alarm_seq);
						data.put("alarm_config_id", alarm_config.getAlarm_config_id());
						
						//28 컬럼 추가 (2018_08_21)
						data.put("site_id", site.getSite_id());
						data.put("site_name", site.getSite_name());
						data.put("site_description", StringUtils.isNotEmpty(site.getDescription()) ? site.getDescription() : "");
						data.put("opc_id", opc.getOpc_id());
						data.put("opc_name", opc.getOpc_name());
						data.put("opc_description", StringUtils.isNotEmpty(opc.getDescription()) ? opc.getDescription() : "");
						data.put("tag_name", tag.getTag_name());
						data.put("tag_description", StringUtils.isNotEmpty(tag.getDescription()) ? tag.getDescription() : "");
						if(asset != null & StringUtils.isNotEmpty(asset.getAsset_id())) {
							data.put("asset_name",        asset.getAsset_name());
							data.put("asset_description", StringUtils.isNotEmpty(asset.getDescription()) ? asset.getDescription() : "");
							Asset area = asset_dao.selectAsset(asset.getParent_asset_id());
							data.put("area_id",          area.getAsset_id());
							data.put("area_name",        area.getAsset_name());
							data.put("area_description", StringUtils.isNotEmpty(area.getDescription()) ? area.getDescription() : "");
						}else {
							data.put("asset_name",        "");
							data.put("asset_description", "");
							data.put("area_id",           "");
							data.put("area_name",         "");
							data.put("area_description",  "");
						}
						
						data.put("tag_java_type", tag.getJava_type());
						data.put("tag_interval", tag.getInterval());
						data.put("tag_alias_name", tag.getAlias_name());
						data.put("tag_unit", tag.getUnit());
						data.put("tag_display_format", tag.getDisplay_format());
						data.put("tag_importance", tag.getImportance());
						data.put("tag_lat", tag.getLat());
						data.put("tag_lng", tag.getLng());
						data.put("tag_min_value", tag.getMin_value());
						data.put("tag_max_value", tag.getMax_value());
						data.put("tag_note", tag.getNote());
						data.put("tag_location", TagLocationFactory.getInstance().getTagLocation(tag.getTag_id()));
						
						data.put("alarm_config_name", alarm_config.getAlarm_config_name());
						data.put("alarm_config_description", StringUtils.isNotEmpty(alarm_config.getAlarm_config_desc()) ? alarm_config.getAlarm_config_desc() : "");
						data.put("alarm_config_epl", alarm_config.getEpl());
						data.put("alarm_config_recieve_me", alarm_config.isRecieve_me());
						data.put("alarm_config_recieve_others", alarm_config.getRecieve_others());
						data.put("alarm_config_insert_user_id", alarm_config.getInsert_user_id());
						
						log.debug("__ELQ_ALARM_JSON_DATA = " + data.toString());
						
						//알람 저장
						StorageClient client = new StorageClient();
						client.forInsert().insertAlarm(alarm, data);
						
						//에셋 알람 푸시
						CEPEngineManager.getInstance().getData_distribution_service().sendTagAlarm(alarm_config.getTag_id(), data);
						if(StringUtils.isNotEmpty(tag.getLinked_asset_id())){
							(new AssetAlarmManager()).insertAssetAlarm(data);
						}

						// RECIVE ME (태그 보안 그룹의 사용자에게 전송하는 것을 의미)
						List<String> pushed_user_list = new ArrayList<String>();
						if (alarm_config.isRecieve_me()) {
							SecurityDAO dao = new SecurityDAO();
							String[] security_user_array = dao.getUserListBySecurityOfTagId(tag.getTag_id()); //태그에 대한 권한을 가지는 사용자
							for (int i = 0; i < security_user_array.length; i++) {
								AlarmPushService service = new AlarmPushServiceImpl();
								if(!pushed_user_list.contains(security_user_array[i])) {
									service.push(alarm_seq, alarm_date, alarm_config.getAlarm_config_priority(), TemplateUtils.parse(alarm_config.getMessage(), json), security_user_array[i],
											alarm_config.getAlarm_config_id(), alarm_config.getTag_id());
									pushed_user_list.add(security_user_array[i]);
								}
							}

						}
						;

						// RECIVE OTHERS
						if (StringUtils.isNotEmpty(alarm_config.getRecieve_others())) {
							String[] others_user_array = alarm_config.getRecieve_others().split(",");
							for (int i = 0; i < others_user_array.length; i++) {
								AlarmPushService service = new AlarmPushServiceImpl();
								if(!pushed_user_list.contains(others_user_array[i])) {
									service.push(alarm_seq, alarm_date, alarm_config.getAlarm_config_priority(), TemplateUtils.parse(alarm_config.getMessage(), json), others_user_array[i],
											alarm_config.getAlarm_config_id(), alarm_config.getTag_id());
									pushed_user_list.add(others_user_array[i]);
								};
							}
						}
					}

				}

			}

		} catch (Exception ex) {
			log.error("EQL alarm update listener failed : " + ex.getMessage(), ex);
		}
	}

}
