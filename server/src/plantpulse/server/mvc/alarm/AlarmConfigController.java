package plantpulse.server.mvc.alarm;

import java.util.ArrayList;
import java.util.List;

import javax.servlet.http.HttpServletRequest;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.realdisplay.framework.util.StringUtils;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Controller;
import org.springframework.ui.Model;
import org.springframework.validation.BindingResult;
import org.springframework.web.bind.annotation.ModelAttribute;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.ResponseBody;

import plantpulse.cep.service.AlarmConfigService;
import plantpulse.cep.service.TagService;
import plantpulse.cep.service.UserService;
import plantpulse.domain.AlarmConfig;
import plantpulse.domain.Tag;
import plantpulse.domain.User;
import plantpulse.server.mvc.CRUDKeys;
import plantpulse.server.mvc.JSONResult;
import plantpulse.server.mvc.Result;
import plantpulse.server.mvc.util.SessionUtils;

@Controller
public class AlarmConfigController {

	private static final Log log = LogFactory.getLog(AlarmConfigController.class);

	private AlarmConfigService alarm_config_service = new AlarmConfigService();
	private UserService user_service = new UserService();

	@Autowired
	private TagService tag_service;

	//
	public Model setDefault(Model model, String insert_user_id) throws Exception {
		List<AlarmConfig> alarm_config_list = alarm_config_service.getAlarmConfigListByInsertUserId(insert_user_id);
		alarm_config_list = alarm_config_service.setAlarmConfigStatus(alarm_config_list); //
		model.addAttribute("alarm_config_list", alarm_config_list);
		return model;
	}

	@RequestMapping(value = "/alarm/config/index", method = RequestMethod.GET)
	public String index(Model model, HttpServletRequest request) throws Exception {
		setDefault(model, SessionUtils.getUserFromSession(request).getUser_id());
		return "alarm/config/index";
	}

	@RequestMapping(value = "/alarm/config/add", method = RequestMethod.GET)
	public String add(Model model, HttpServletRequest request) throws Exception {
		AlarmConfig alarm_config = new AlarmConfig();
		alarm_config.setMode(CRUDKeys.INSERT);
		alarm_config.setRecieve_me(true);
		model.addAttribute("alarm_config", alarm_config);
		return "alarm/config/form";
	}

	@RequestMapping(value = "/alarm/config/edit/{alarm_config_id}", method = RequestMethod.GET)
	public String form(@PathVariable("alarm_config_id") String alarm_config_id, Model model, HttpServletRequest request) throws Exception {
		AlarmConfig alarm_config = alarm_config_service.getAlarmConfig(alarm_config_id);
		alarm_config.setMode(CRUDKeys.UPDATE);
		model.addAttribute("alarm_config", alarm_config);
		//
		List<User> recieve_others_list = new ArrayList<User>();
		if (StringUtils.isNotEmpty(alarm_config.getRecieve_others())) {
			String[] user_id_array = alarm_config.getRecieve_others().split(",");
			for (int i = 0; i < user_id_array.length; i++) {
				recieve_others_list.add(user_service.getUser(user_id_array[i]));
			}
		}
		model.addAttribute("recieve_others_list", recieve_others_list);

		Tag tag = tag_service.selectTag(alarm_config.getTag_id());
		model.addAttribute("tag", tag);

		return "alarm/config/form";
	}

	@RequestMapping(value = "/alarm/config/save", method = RequestMethod.POST)
	public String save(@ModelAttribute("alarm_config") AlarmConfig alarm_config, BindingResult br, Model model, HttpServletRequest request) throws Exception {

		try {
			String insert_user_id = SessionUtils.getUserFromSession(request).getUser_id();
			//
			if (alarm_config.getMode().equals(CRUDKeys.INSERT)) {
				//
				alarm_config.setAlarm_config_id("ALARM_CONFIG_" + System.currentTimeMillis());
				alarm_config.setInsert_user_id(insert_user_id);
				//
				alarm_config_service.saveAlarmConfig(alarm_config);
				alarm_config_service.deployAlarmConfig(alarm_config);
			} else if (alarm_config.getMode().equals(CRUDKeys.UPDATE)) {
				alarm_config_service.updateAlarmConfig(alarm_config);
				alarm_config_service.redeployAlarmConfig(alarm_config);
			}
			//
			setDefault(model, insert_user_id);
			model.addAttribute("result", new Result(Result.SUCCESS, "알람 설정을 저장하였습니다."));
			return "alarm/config/index";

		} catch (Exception ex) {
			model.addAttribute("result", new Result(Result.SUCCESS, "알람 설정을 저장할 수 없습니다 : 메세지=[" + ex.getMessage() + "]"));
			log.warn(ex);
			return "alarm/config/form";
		}
	}

	@RequestMapping(value = "/alarm/config/delete/{alarm_config_id}", method = RequestMethod.POST)
	public @ResponseBody JSONResult delete(@PathVariable String alarm_config_id, Model model, HttpServletRequest request) throws Exception {
		//
		AlarmConfigService alarm_config_service = new AlarmConfigService();
		alarm_config_service.undeployAlarmConfig(alarm_config_service.getAlarmConfig(alarm_config_id));
		//
		alarm_config_service.deleteAlarmConfig(alarm_config_id);
		//
		return new JSONResult(Result.SUCCESS, "알람 설정을 삭제하였습니다.");
	}

	@RequestMapping(value = "/alarm/config/deploy/{alarm_config_ids}", method = RequestMethod.POST)
	public @ResponseBody JSONResult deployAlarmConfig(@PathVariable String alarm_config_ids, Model model, HttpServletRequest request) {
		//
		JSONResult json = new JSONResult();
		try {

			String[] ids = alarm_config_ids.split(",");
			for (int i = 0; i < ids.length; i++) {

				AlarmConfig alarm_config = alarm_config_service.getAlarmConfig(ids[i]);
				AlarmConfigService alarm_config_service = new AlarmConfigService();
				alarm_config_service.deployAlarmConfig(alarm_config);
			}
			json.setStatus(JSONResult.SUCCESS);
			json.setMessage("알람 설정 배치를 완료하였습니다.");

		} catch (Exception e) {
			json.setStatus(JSONResult.ERROR);
			json.setMessage("알람 설정 배치도중 오류가 발생하였습니다. : 메세지=" + e.getMessage());
			log.error(e);
		}
		return json;
	}

	@RequestMapping(value = "/alarm/config/undeploy/{alarm_config_ids}", method = RequestMethod.POST)
	public @ResponseBody JSONResult undeployAlarmConfig(@PathVariable String alarm_config_ids, Model model, HttpServletRequest request) {
		//
		JSONResult json = new JSONResult();
		try {

			String[] ids = alarm_config_ids.split(",");
			for (int i = 0; i < ids.length; i++) {
				AlarmConfig alarm_config = alarm_config_service.getAlarmConfig(ids[i]);
				AlarmConfigService alarm_config_service = new AlarmConfigService();
				alarm_config_service.undeployAlarmConfig(alarm_config);

			}
			json.setStatus(JSONResult.SUCCESS);
			json.setMessage("알람 설정 해제를 완료하였습니다.");
		} catch (Exception e) {
			json.setStatus(JSONResult.ERROR);
			json.setMessage("알람 설정 해제도중 오류가 발생하였습니다. : 메세지=" + e.getMessage());
			log.error(e);
		}
		return json;

	}

	@RequestMapping(value = "/alarm/config/redeploy", method = RequestMethod.POST)
	public @ResponseBody JSONResult redeployAlarmConfig(Model model, HttpServletRequest request) {
		//
		JSONResult json = new JSONResult();
		try {
			//
			String insert_user_id = SessionUtils.getUserFromSession(request).getUser_id();
			List<AlarmConfig> list = alarm_config_service.getAlarmConfigListByInsertUserId(insert_user_id);
			for (int i = 0; list != null && i < list.size(); i++) {
				AlarmConfig alarm_config = list.get(i);
				AlarmConfigService alarm_config_service = new AlarmConfigService();
				alarm_config_service.redeployAlarmConfig(alarm_config);
			}
			json.setStatus(JSONResult.SUCCESS);
			json.setMessage("알람 설정 재배치를 완료하였습니다.");
		} catch (Exception e) {
			json.setStatus(JSONResult.ERROR);
			json.setMessage("알람 설정 재배치도중 오류가 발생하였습니다. : 메세지=" + e.getMessage());
			log.error(e);
		}
		return json;

	}

}
