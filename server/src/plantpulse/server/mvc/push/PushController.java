package plantpulse.server.mvc.push;

import java.util.List;

import org.springframework.stereotype.Controller;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.ResponseBody;

import plantpulse.cep.listener.push.PushCacheMap;

@Controller
public class PushController {

	@RequestMapping(value = "/push/history", method = RequestMethod.GET)
	public @ResponseBody String history(@RequestParam String url, @RequestParam int limit) {
		//
		List<String> history = PushCacheMap.getInstance().getHistoryList(url, limit);
		// /System.out.println("history=" + history);
		return history == null ? "[]" : history.toString();

	}

	@RequestMapping(value = "/push/ws", method = RequestMethod.GET)
	public String ws() {
		return "push/ws";
	}

}