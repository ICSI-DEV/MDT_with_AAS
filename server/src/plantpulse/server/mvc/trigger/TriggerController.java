package plantpulse.server.mvc.trigger;

import java.util.List;

import org.apache.commons.lang.StringUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.springframework.stereotype.Controller;
import org.springframework.ui.Model;
import org.springframework.validation.BindingResult;
import org.springframework.web.bind.annotation.ModelAttribute;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.ResponseBody;

import plantpulse.cep.domain.Trigger;
import plantpulse.cep.service.TriggerService;
import plantpulse.cep.service.client.StorageClient;
//
import plantpulse.server.mvc.CRUDKeys;
import plantpulse.server.mvc.JSONResult;
import plantpulse.server.mvc.Result;

@Controller
public class TriggerController {

	private static final Log log = LogFactory.getLog(TriggerController.class);

	private TriggerService trigger_service = new TriggerService();
	private StorageClient storage_client = new StorageClient();

	public Model setDefault(Model model) throws Exception {
		List<Trigger> trigger_list = trigger_service.getTriggerList();
		trigger_list = trigger_service.setTriggerStatus(trigger_list); //
		model.addAttribute("trigger_list", trigger_list);
		return model;
	}

	@RequestMapping(value = "/trigger/index", method = RequestMethod.GET)
	public String index(Model model) throws Exception {
		setDefault(model);
		return "trigger/index";
	}

	@RequestMapping(value = "/trigger/add", method = RequestMethod.GET)
	public String add(Model model) throws Exception {
		Trigger trigger = new Trigger();
		trigger.setMode(CRUDKeys.INSERT);
		model.addAttribute("trigger", trigger);
		return "trigger/form";
	}

	@RequestMapping(value = "/trigger/edit/{trigger_id}", method = RequestMethod.GET)
	public String form(@PathVariable("trigger_id") String trigger_id, Model model) throws Exception {
		Trigger trigger = trigger_service.getTrigger(trigger_id);
		trigger.setMode(CRUDKeys.UPDATE);
		model.addAttribute("trigger", trigger);
		return "trigger/form";
	}

	@RequestMapping(value = "/trigger/save", method = RequestMethod.POST)
	public String save(@ModelAttribute("trigger") Trigger trigger, BindingResult br, Model model) throws Exception {

		try {

			//
			if (trigger.isUse_mq()) {
				if (StringUtils.isEmpty(trigger.getMq_destination())) {
					throw new Exception("데스티네이션이 입력되지 않았습니다.");
				}
			}

			//
			if (trigger.isUse_storage()) {
				//테이블 필드 검증
				if (trigger.getTrigger_attributes() == null || trigger.getTrigger_attributes().length < 1) {
					throw new Exception("테이블 컬럼이 입력되지 않았습니다.");
				} else {
					for (int i = 0; i < trigger.getTrigger_attributes().length; i++) {
						if (StringUtils.isEmpty(trigger.getTrigger_attributes()[i].getField_name())) {
							throw new Exception("테이블 컬럼 속성의 필드값이 빈 행이 존재합니다.");
						}
					}
				}
				
				try{
					//트리거 데이터 저장 테이블 생성
					storage_client.forInsert().createTableForTrigger(trigger);
				}catch(Exception te){
					throw new Exception("스토리지 테이블 생성 속서이 잘못되어 테이블을 생성할 수 없습니다.", te);
				}
			}

			//
			if (trigger.getMode().equals(CRUDKeys.INSERT)) {
				trigger_service.saveTrigger(trigger);
			} else if (trigger.getMode().equals(CRUDKeys.UPDATE)) {
				trigger_service.updateTrigger(trigger);
			}
			//
			setDefault(model);
			model.addAttribute("result", new Result(Result.SUCCESS, "트리거를 저장하였습니다."));
			return "trigger/index";

		} catch (Exception ex) {
			model.addAttribute("result", new Result(Result.SUCCESS, "트리거를 저장할 수 없습니다 : 메세지=[" + ex.getMessage() + "]"));
			log.warn(ex);
			return "trigger/form";
		}
	}

	@RequestMapping(value = "/trigger/delete/{trigger_id}", method = RequestMethod.POST)
	public @ResponseBody JSONResult delete(@PathVariable String trigger_id, Model model) throws Exception {
		//
		TriggerService trigger_service = new TriggerService();
		trigger_service.undeployTrigger(trigger_service.getTrigger(trigger_id));
		//
		trigger_service.deleteTrigger(trigger_id);
		//
		return new JSONResult(Result.SUCCESS, "트리거를 삭제하였습니다.");
	}

	@RequestMapping(value = "/trigger/deploy/{trigger_ids}", method = RequestMethod.POST)
	public @ResponseBody JSONResult deployTrigger(@PathVariable String trigger_ids) {
		//
		JSONResult json = new JSONResult();
		try {

			String[] ids = trigger_ids.split(",");
			for (int i = 0; i < ids.length; i++) {

				Trigger trigger = trigger_service.getTrigger(ids[i]);
				TriggerService trigger_service = new TriggerService();
				trigger_service.deployTrigger(trigger);
			}
			json.setStatus(JSONResult.SUCCESS);
			json.setMessage("트리거 배치를 완료하였습니다.");

		} catch (Exception e) {
			json.setStatus(JSONResult.ERROR);
			json.setMessage("트리거 배치도중 오류가 발생하였습니다. : 메세지=" + e.getMessage());
			log.error(e);
		}
		return json;
	}

	@RequestMapping(value = "/trigger/undeploy/{trigger_ids}", method = RequestMethod.POST)
	public @ResponseBody JSONResult undeployTrigger(@PathVariable String trigger_ids) {
		//
		JSONResult json = new JSONResult();
		try {

			String[] ids = trigger_ids.split(",");
			for (int i = 0; i < ids.length; i++) {
				Trigger trigger = trigger_service.getTrigger(ids[i]);
				TriggerService trigger_service = new TriggerService();
				trigger_service.undeployTrigger(trigger);

			}
			json.setStatus(JSONResult.SUCCESS);
			json.setMessage("트리거 해제를 완료하였습니다.");
		} catch (Exception e) {
			json.setStatus(JSONResult.ERROR);
			json.setMessage("트리거 해제도중 오류가 발생하였습니다. : 메세지=" + e.getMessage());
			log.error(e);
		}
		return json;

	}

	@RequestMapping(value = "/trigger/redeploy", method = RequestMethod.POST)
	public @ResponseBody JSONResult redeployTrigger() {
		//
		JSONResult json = new JSONResult();
		try {

			List<Trigger> list = trigger_service.getTriggerList();
			for (int i = 0; list != null && i < list.size(); i++) {
				Trigger trigger = list.get(i);
				TriggerService trigger_service = new TriggerService();
				trigger_service.redeployTrigger(trigger);
			}
			json.setStatus(JSONResult.SUCCESS);
			json.setMessage("트리거 재배치를 완료하였습니다.");
		} catch (Exception e) {
			json.setStatus(JSONResult.ERROR);
			json.setMessage("트리거 재배치도중 오류가 발생하였습니다. : 메세지=" + e.getMessage());
			log.error(e);
		}
		return json;

	}
}
