package plantpulse.server.mvc.system;

import javax.servlet.http.HttpServletRequest;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.json.JSONArray;
import org.json.JSONObject;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Controller;
import org.springframework.ui.Model;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.ResponseBody;

import plantpulse.cep.service.client.StorageClient;
import plantpulse.server.mvc.util.DateUtils;

@Controller
@RequestMapping(value = "/system")
public class SystemController  {

	private static final Log log = LogFactory.getLog(SystemController.class);
	
	@Autowired
	private StorageClient storage_client;
	
	@RequestMapping(value = "/health", method = RequestMethod.GET)
	public @ResponseBody String get(Model model, HttpServletRequest request) throws Exception {
 
		//
		String from = DateUtils.currDateBy00()  + ":00";
		String to   = DateUtils.currDateBy24()  + ":59";
		
		//
		JSONObject result = new JSONObject();
		result.put("from", DateUtils.toTimestamp(from));
		result.put("to", DateUtils.toTimestamp(to));
       
        //5. 헬스 데이터 리스트
        JSONArray health_list = new JSONArray();
        try{
        	health_list = storage_client.forSelect().selectSystemHealthStatus(from, to);
        }catch(Exception ex){
        	log.warn("System health list search error : " + ex.getMessage());
        };
        result.put("health_list", health_list);
        
        //
		return result.toString();
	}
	

}