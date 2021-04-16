package plantpulse.server.mvc.enginemanager;

import org.springframework.stereotype.Controller;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.ResponseBody;

import plantpulse.cep.engine.logging.EngineLogger;
import plantpulse.cep.service.CEPEngineRestartService;
import plantpulse.server.mvc.JSONResult;

@Controller
public class EngineManagerController {

	@RequestMapping(value = "/enginemanager/restart", method = RequestMethod.GET, headers = "Accept=*/*", produces = "application/json")
	public @ResponseBody JSONResult restart() throws Exception {
		JSONResult json = new JSONResult();
		try {

			//
			CEPEngineRestartService cepEngineRestartService = new CEPEngineRestartService();
			cepEngineRestartService.restart();

			if (EngineLogger.isHasError()) {
				json.setStatus("ERROR");
				json.setMessage("엔진을 재시작하였지만 일부 컴포넌트를 시작하는 도중 오류가 발생하였습니다.");
			} else {
				json.setStatus("SUCCESS");
				json.setMessage("엔진을 재시작하였습니다.");
			}

		} catch (Exception e) {
			json.setStatus("ERROR");
			json.setMessage("엔진을 재시작 시 오류가 발생하였습니다 : 메세지=[" + e.getMessage() + "]");
		}
		//
		return json;
	}

	@RequestMapping(value = "/enginemanager/log", method = RequestMethod.GET, headers = "Accept=*/*", produces = "application/json")
	public @ResponseBody JSONResult logs() throws Exception {
		JSONResult json = new JSONResult();
		try {

			//
			json.setStatus("SUCCESS");
			json.setDataValue("size", EngineLogger.getLogs().size());
			json.setDataValue("logs", EngineLogger.getLogs());

		} catch (Exception e) {
			json.setStatus("ERROR");
			json.setMessage("엔진 로그 조회 시 오류가 발생하였습니다. : 메세지=[" + e.getMessage() + "]");
		}
		//
		return json;
	}

}
