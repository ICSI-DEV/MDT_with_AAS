package plantpulse.server.mvc.point;

import java.util.List;
import java.util.Map;

import javax.servlet.http.HttpServletRequest;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.json.JSONArray;
import org.json.JSONObject;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Controller;
import org.springframework.ui.Model;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.ResponseBody;

import plantpulse.cep.engine.utils.TagUtils;
import plantpulse.cep.service.TagService;
import plantpulse.cep.service.client.StorageClient;
import plantpulse.domain.Tag;

@Controller
public class PointController {

	private static final Log log = LogFactory.getLog(PointController.class);

	@Autowired
	private TagService tag_service;
	
	@Autowired
	private StorageClient storage_client;

	@RequestMapping(value = "/point/{tag_id}/last", method = RequestMethod.GET)
	public @ResponseBody String last(Model model, @PathVariable String tag_id, HttpServletRequest request) throws Exception {
		Tag tag = tag_service.selectTag(tag_id);
		Map<String,Object> result = storage_client.forSelect().selectPointLast(tag);
		return JSONObject.fromObject(result).toString();
	}
	
	@RequestMapping(value = "/point/{tag_id}/limit/{limit}", method = RequestMethod.GET)
	public @ResponseBody String limit(Model model, @PathVariable String tag_id, @PathVariable int limit, HttpServletRequest request) throws Exception {
		Tag tag = tag_service.selectTag(tag_id);
		List<Map<String,Object>> result = storage_client.forSelect().selectPointLast(tag, limit);
		return JSONArray.fromObject(result).toString();
	}
	
	@RequestMapping(value = "/point/{tag_id}/after/{timestamp}", method = RequestMethod.GET)
	public @ResponseBody String afterTimestamp(Model model, @PathVariable String tag_id, @PathVariable long timestamp, HttpServletRequest request) throws Exception {
		Tag tag = tag_service.selectTag(tag_id);
		List<Map<String,Object>> result = storage_client.forSelect().selectPointAfterTimestamp(tag, timestamp);
		return JSONArray.fromObject(result).toString();
	};
	
	
	@RequestMapping(value = "/point/{tag_id}/limit/{limit}/chart", method = RequestMethod.GET)
	public @ResponseBody String limit_chart(Model model, @PathVariable String tag_id, @PathVariable int limit, HttpServletRequest request) throws Exception {
		Tag tag = tag_service.selectTag(tag_id);
		JSONArray result = new JSONArray();
		if(TagUtils.isNumbericDataType(tag)) {
			result = storage_client.forSelect().selectPointValueList(tag, limit);
		}else if(TagUtils.isBooleanDataType(tag)) {
			result = storage_client.forSelect().selectPointValueList(tag, limit);
			JSONArray array = new JSONArray();
			for(int i=0; i < result.size(); i++) {
				long ts = result.getJSONArray(i).getLong(0);
				boolean bool_value = result.getJSONArray(i).getBoolean(1);
				
				JSONArray one = new JSONArray();
				one.add(ts);
				if(bool_value == true)  { one.add(1); }; 
				if(bool_value == false) { one.add(0);  };
				
				array.add(i, one);
			}
			result = array;
			
		}else {
			throw new Exception("Unsuppport data type. must be type [numberic, boolean]");
		}
		return result.toString();
	}


	

}
