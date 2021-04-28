package plantpulse.server.mvc.event;

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

import plantpulse.cep.domain.Event;
import plantpulse.cep.service.CEPEventService;
import plantpulse.cep.service.CEPService;
import plantpulse.server.mvc.CRUDKeys;
import plantpulse.server.mvc.JSONResult;
import plantpulse.server.mvc.Result;

@Controller
public class EventController {

	private static final Log log = LogFactory.getLog(EventController.class);

	private CEPService cep_service = new CEPService();
	private CEPEventService cep_event_service = new CEPEventService();

	public Model setDefault(Model model) throws Exception {
		List<Event> event_list = cep_service.getEventList();
		event_list = cep_event_service.setEventStatus(event_list); //
		model.addAttribute("event_list", event_list);
		return model;
	}

	@RequestMapping(value = "/event/index", method = RequestMethod.GET)
	public String index(Model model) throws Exception {
		setDefault(model);
		return "event/index";
	}

	@RequestMapping(value = "/event/add", method = RequestMethod.GET)
	public String add(Model model) throws Exception {
		Event event = new Event();
		event.setMode(CRUDKeys.INSERT);
		model.addAttribute("event", event);
		return "event/form";
	}

	@RequestMapping(value = "/event/edit/{event_name}", method = RequestMethod.GET)
	public String form(@PathVariable("event_name") String event_name, Model model) throws Exception {
		Event event = cep_service.getEvent(event_name);
		event.setMode(CRUDKeys.UPDATE);
		model.addAttribute("event", event);
		return "event/form";
	}

	@RequestMapping(value = "/event/save", method = RequestMethod.POST)
	public String save(@ModelAttribute("event") Event event, BindingResult br, Model model) throws Exception {

		try {
			//
			if (event.getEvent_attributes() == null || event.getEvent_attributes().length < 1) {
				throw new Exception("이벤트 속성이 입력되지 않았습니다.");
			} else {
				for (int i = 0; i < event.getEvent_attributes().length; i++) {
					if (StringUtils.isEmpty(event.getEvent_attributes()[i].getField_name())) {
						throw new Exception("이벤트 속성의 필드값이 빈 행이 존재합니다.");
					}
				}
			}

			//
			if (event.getMode().equals(CRUDKeys.INSERT)) {
				cep_service.saveEvent(event);
			} else if (event.getMode().equals(CRUDKeys.UPDATE)) {
				cep_service.updateEvent(event);
			}
			//
			setDefault(model);
			model.addAttribute("result", new Result(Result.SUCCESS, "이벤트를 저장하였습니다."));
			return "event/index";

		} catch (Exception ex) {
			model.addAttribute("result", new Result(Result.ERROR, "이벤트를 저장할 수 없습니다 : 메세지=[" + ex.getMessage() + "]"));
			log.warn(ex);
			return "event/form";
		}
	}

	@RequestMapping(value = "/event/delete/{event_name}", method = RequestMethod.POST)
	public @ResponseBody JSONResult delete(@PathVariable String event_name, Model model) throws Exception {
		//
		CEPEventService cep_event_service = new CEPEventService();
		cep_event_service.undeployEvent(cep_service.getEvent(event_name));
		//
		cep_service.deleteEvent(event_name);
		//
		return new JSONResult(Result.SUCCESS, "이벤트를 삭제하였습니다.");
	}

	@RequestMapping(value = "/event/deploy/{event_names}", method = RequestMethod.POST)
	public @ResponseBody JSONResult deployEvent(@PathVariable String event_names) {
		//
		JSONResult json = new JSONResult();
		try {

			String[] ids = event_names.split(",");
			for (int i = 0; i < ids.length; i++) {

				Event event = cep_service.getEvent(ids[i]);
				CEPEventService cep_event_service = new CEPEventService();
				cep_event_service.deployEvent(event);
			}
			json.setStatus(JSONResult.SUCCESS);
			json.setMessage("이벤트 배치를 완료하였습니다.");

		} catch (Exception e) {
			json.setStatus(JSONResult.ERROR);
			json.setMessage("이벤트 배치도중 오류가 발생하였습니다. : 메세지=" + e.getMessage());
			log.error(e);
		}
		return json;
	}

	@RequestMapping(value = "/event/undeploy/{event_names}", method = RequestMethod.POST)
	public @ResponseBody JSONResult undeployEvent(@PathVariable String event_names) {
		//
		JSONResult json = new JSONResult();
		try {

			String[] ids = event_names.split(",");
			for (int i = 0; i < ids.length; i++) {
				Event event = cep_service.getEvent(ids[i]);
				CEPEventService cep_event_service = new CEPEventService();
				cep_event_service.undeployEvent(event);

			}
			json.setStatus(JSONResult.SUCCESS);
			json.setMessage("이벤트 해제를 완료하였습니다.");
		} catch (Exception e) {
			json.setStatus(JSONResult.ERROR);
			json.setMessage("이벤트 해제도중 오류가 발생하였습니다. : 메세지=" + e.getMessage());
			log.error(e);
		}
		return json;

	}

	@RequestMapping(value = "/event/redeploy", method = RequestMethod.POST)
	public @ResponseBody JSONResult redeployEvent() {
		//
		JSONResult json = new JSONResult();
		try {

			List<Event> list = cep_service.getEventList();
			for (int i = 0; list != null && i < list.size(); i++) {
				Event event = list.get(i);
				CEPEventService cep_event_service = new CEPEventService();
				cep_event_service.redeployEvent(event);
			}
			json.setStatus(JSONResult.SUCCESS);
			json.setMessage("이벤트 재배치를 완료하였습니다.");
		} catch (Exception e) {
			json.setStatus(JSONResult.ERROR);
			json.setMessage("이벤트 재배치도중 오류가 발생하였습니다. : 메세지=" + e.getMessage());
			log.error(e);
		}
		return json;

	}
}