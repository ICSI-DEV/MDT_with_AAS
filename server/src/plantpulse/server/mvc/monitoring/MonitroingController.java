package plantpulse.server.mvc.monitoring;

import java.util.List;

import javax.servlet.http.HttpServletRequest;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.json.JSONObject;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Controller;
import org.springframework.ui.Model;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.ResponseBody;

import plantpulse.cep.engine.CEPEngineManager;
import plantpulse.cep.service.OPCService;
import plantpulse.cep.service.client.StorageClient;
import plantpulse.domain.OPC;

@Controller
public class MonitroingController {

	private static final Log log = LogFactory.getLog(MonitroingController.class);
	
	@Autowired
	private StorageClient client = new StorageClient();
	
	@Autowired
	private OPCService opc_service = new OPCService();

	@RequestMapping(value = "/monitoring/index", method = RequestMethod.GET)
	public String index(Model model, HttpServletRequest request) throws Exception {
		
		List<OPC> opc_list = opc_service.selectOpcList();
		
		model.addAttribute("opc_list", opc_list);
		//
		return "monitoring/index";
	}
	
	
	@RequestMapping(value = "/monitoring/opchealth", method = RequestMethod.GET)
	public @ResponseBody String opcCheck(@RequestParam("opc_id") String opc_id, Model model, HttpServletRequest request) throws Exception {
		JSONObject json = new JSONObject();
		try{
			OPC opc = new OPC();
			opc.setOpc_id(opc_id);
			json =  client.forSelect().selectOPCPointLastUpdate(opc);
			
			//
			long current_timesatmp = System.currentTimeMillis();
			//
			if(json.containsKey("last_update_timestamp") ){
				long update_timesatmp = json.getLong("last_update_timestamp");
				long updated_ms = current_timesatmp - update_timesatmp;
				json.put("updated_ms", updated_ms);
				json.put("updated_mm", ((updated_ms / 1000) / 60));
				if(updated_ms > ((60*1000) * 30)){
					json.put("color", "red");
				}else if(updated_ms > ((60*1000) * 10)){
					json.put("color", "orange");
				}else {
					json.put("color", "green");
				}
			}else{
				json.put("updated_mm",  "-");
				json.put("updated_ms", 0);
				json.put("color", "gray");
			}
			//
			
		}catch(Exception ex){
			log.error("OPC Health data getting error : opc_id=["  + opc_id + "]");
		}
		return json.toString();
	}
	
	
	@RequestMapping(value = "/monitoring/mqdbhealth", method = RequestMethod.GET)
	public @ResponseBody String mdbHealth(Model model, HttpServletRequest request) throws Exception {
		return CEPEngineManager.getInstance().getMq_and_db_health_timer().getSTATUS().toString();
	}
	
	
	
}