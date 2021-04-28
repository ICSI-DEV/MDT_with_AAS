package plantpulse.cep.service.support.alarm;

import java.util.Date;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

import javax.mail.internet.InternetAddress;

import org.apache.commons.lang.StringUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.json.JSONObject;

import plantpulse.cep.context.EngineContext;
import plantpulse.cep.dao.TagDAO;
import plantpulse.cep.domain.Alarm;
import plantpulse.cep.engine.async.AsynchronousExecutor;
import plantpulse.cep.engine.async.Worker;
import plantpulse.cep.engine.monitoring.MonitorConstants;
import plantpulse.cep.engine.utils.PropertiesUtils;
import plantpulse.cep.listener.ResultServiceManager;
import plantpulse.cep.listener.push.PushClient;
import plantpulse.cep.service.AlarmConfigService;
import plantpulse.cep.service.AlarmService;
import plantpulse.cep.service.UserService;
import plantpulse.cep.service.support.alarm.sms.SMSSender;
import plantpulse.cep.service.support.tag.TagLocation;
import plantpulse.cep.service.support.tag.TagLocationFactory;
import plantpulse.domain.AlarmConfig;
import plantpulse.domain.Tag;
import plantpulse.domain.User;
import plantpulse.server.mail.STMPService;

/**
 * AlarmPushServiceImpl
 * 
 * @author lhh
 * 
 */
public class AlarmPushServiceImpl implements AlarmPushService {

	private static final Log log = LogFactory.getLog(AlarmPushServiceImpl.class);
	
	public static final String PUSH_NEW_ALARM_URL = "/alarm/new";

	private UserService user_service = new UserService();
	private AlarmService alarm_service = new AlarmService();
	private AlarmConfigService alarm_config_service = new AlarmConfigService();

	private TagDAO tag_dao = new TagDAO();

	@Override
	public void push(long alarm_seq, final Date alarm_date, final String priority, final String description, final String insert_user_id, final String alarm_config_id, String tag_id)
			throws Exception {

		// 이미 알림이 발생시 리젝트 (1분)
		if (StringUtils.isNotEmpty(alarm_config_id)) {
			AlarmRejectHandler hadler = new AlarmRejectHandler();
			if (hadler.aleadyAlarmed(alarm_config_id, insert_user_id, priority)) {
				return;
			}
		}

		//
		try {

			Alarm alarm = new Alarm();
			alarm.setAlarm_seq(alarm_seq);
			alarm.setAlarm_config_id(alarm_config_id);
			alarm.setAlarm_date(alarm_date);
			alarm.setPriority(priority);
			alarm.setDescription(description);
			alarm.setInsert_user_id(insert_user_id);
			alarm_service.saveAlarm(alarm);
		} catch (Exception ex) {
			log.error("Alarm save failed : " + ex.getMessage(), ex);
		}

		// 신규알람을 푸시
		try {
			Map<String, Object> data = new HashMap<String, Object>();
			data.put("alarm_seq", alarm_seq);
			data.put("alarm_date", alarm_date);
			data.put("priority", priority);
			data.put("description", description);
			data.put("insert_user_id", insert_user_id);
			data.put("alarm_config_id", alarm_config_id);
			data.put("tag_id", tag_id);

			// 위치 추가
			Tag tag = tag_dao.selectTagInfoWithSiteAndOpc(tag_id);
			data.put("tag_id", tag.getTag_id());
			data.put("tag_name", tag.getTag_name());
			data.put("opc_id", tag.getOpc_id());
			data.put("opc_name", tag.getOpc_name());
			data.put("site_id", tag.getSite_id());
			data.put("site_name", tag.getSite_name());

			//
			TagLocation location = new TagLocation();
			data.put("tag_location", location.getLocation(tag_id, true));

			//

			JSONObject json = JSONObject.fromObject(data);
			PushClient client = ResultServiceManager.getPushService().getPushClient(PUSH_NEW_ALARM_URL + "/" + insert_user_id);
			client.sendJSON(json);
		} catch (Exception ex) {
			log.error("Alarm push failed : " + ex.getMessage(), ex);
		}

		//

		// 메일 전송
		AsynchronousExecutor async = new AsynchronousExecutor();
		async.execute(new Worker() {
			public String getName() { return "AlarmPushServiceImpl"; };
			@Override
			public void execute() {
				if (StringUtils.isNotEmpty(alarm_config_id)) {
					try {
						AlarmConfig alarm_config = alarm_config_service.getAlarmConfig(alarm_config_id);
						if (alarm_config.isSend_email()) {

							try {
								User user = user_service.getUser(insert_user_id);
								InternetAddress[] to = new InternetAddress[] { new InternetAddress(user.getEmail()) };
								InternetAddress[] cc = null;
								InternetAddress from = null;
								String templateDir = EngineContext.getInstance().getProperty("_ROOT") + "/WEB-INF/template/mail/";
								String templateFileName = "new_alarm.html";

								TagDAO tag_dao = new TagDAO();
								Tag tag = tag_dao.selectTag(alarm_config.getTag_id());
								
								Map<String, Object> data = new HashMap<String, Object>();
								data.put("tag_name", tag.getTag_name());
								data.put("tag_description", tag.getDescription());
								data.put("tag_location", TagLocationFactory.getInstance().getTagLocation(tag_id));
								data.put("alarm_date", alarm_date);
								data.put("priority", priority);
								data.put("priority_color", getAlarmPriorityColor(priority));
								data.put("description", description);
								data.put("insert_user_id", insert_user_id);
								data.put("alarm_config_id", alarm_config_id);
								data.put("alarm_config_name", alarm_config.getAlarm_config_name());
								
								

								// 메일 전송 로직
								STMPService.sendTemplateMail(to, cc, from, alarm_config.getAlarm_config_name(), templateDir, templateFileName, data);
							} catch (Exception ex) {
								log.error("Mail send failed.", ex);
							}
						}
						;

						if (alarm_config.isSend_sms()) {
							
							try {
								User user = user_service.getUser(insert_user_id);

								String phone = user.getPhone();
								
								TagDAO tag_dao = new TagDAO();
								Tag tag = tag_dao.selectTag(alarm_config.getTag_id());

								Map<String, Object> data = new HashMap<String, Object>();
								data.put("tag_name", tag.getTag_name());
								data.put("tag_description", tag.getDescription());
								data.put("tag_location", TagLocationFactory.getInstance().getTagLocation(tag_id));
								data.put("alarm_date", alarm_date);
								data.put("priority", priority);
								data.put("priority_color", getAlarmPriorityColor(priority));
								data.put("description", description);
								data.put("insert_user_id", insert_user_id);
								data.put("alarm_config_id", alarm_config_id);
								data.put("alarm_config_name", alarm_config.getAlarm_config_name());

								final String MAIL_CONFIG_PATH = "/mail.properties";
								final Properties config = PropertiesUtils.read(MAIL_CONFIG_PATH);
								SMSSender sender = (SMSSender) Class.forName(config.getProperty("sms.sender.class")).newInstance();
								sender.send(phone, data);
							} catch (Exception ex) {
								log.error("SMS send failed.", ex);
							}
						}
						;

					} catch (Exception ex) {
						log.error("Alarm mail & sms send failed : " + ex.getMessage(), ex);
					}
				}
			}
		});

		//
		log.debug("Alarm pushed : description=" + description);
		//
	}
	
	
	public String getAlarmPriorityColor(String priority) {
		if(priority.equals("INFO")) {
			return "GREEN";
		}else if(priority.equals("WARN")) {
			return "ORANGE";
		}else if(priority.equals("ERROR")) {
			return "RED";
		}else {
			return "GRAY";
		}
	}

}
