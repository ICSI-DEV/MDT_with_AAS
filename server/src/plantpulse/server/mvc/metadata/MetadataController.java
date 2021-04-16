package plantpulse.server.mvc.metadata;

import java.util.ArrayList;
import java.util.List;

import javax.servlet.http.HttpServletRequest;

import org.apache.commons.lang.StringUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.HttpStatus;
import org.springframework.stereotype.Controller;
import org.springframework.ui.Model;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.ResponseBody;
import org.springframework.web.bind.annotation.ResponseStatus;

import plantpulse.cep.service.MetadataService;
import plantpulse.domain.Metadata;
import plantpulse.json.JSONArray;
import plantpulse.json.JSONObject;
import plantpulse.server.mvc.JSONResult;

@Controller
public class MetadataController {
	
	private static final Log log = LogFactory.getLog(MetadataController.class);

	@Autowired
	private MetadataService metadata_service;

	
	@RequestMapping(value = "/metadata/index", method = RequestMethod.GET)
	public String index(Model model, HttpServletRequest request) {
		return "metadata/index";
	}

	
	
	@RequestMapping(value = "/metadata/objects", method = RequestMethod.GET)
	public @ResponseBody String objects() throws Exception {
		return metadata_service.selectObjectListForMetadata().toString();
	}

	@RequestMapping(value = "/metadata/get/{object_id}", method = RequestMethod.GET, produces = "application/json; charset=utf8")
	public @ResponseBody String list(@PathVariable String object_id) {
		 JSONObject json = new JSONObject();
		try {
			List<Metadata> metadata_list = metadata_service.selectMetadataList(object_id);
			json.put("metadata_list", JSONArray.fromObject(metadata_list));
		} catch (Exception e) {
			log.error("Metadata list getting exception : " + e.getMessage(), e);
		}
		return json.toString();
	}

	@RequestMapping(value = "/metadata/update/{object_id}", method = RequestMethod.POST)
	@ResponseStatus(HttpStatus.OK)
	public @ResponseBody JSONResult update(@PathVariable String object_id, @RequestBody String body, HttpServletRequest request) {
		try {
			
			if(StringUtils.isEmpty(object_id)){
				throw new Exception("오브젝트가 선택되지 않았습니다.");
			}
			
			JSONArray array = JSONObject.fromObject(body).getJSONArray("metadata_list");
			List<Metadata> list = new ArrayList<Metadata>();
			for(int i=0; i < array.size(); i++) {
				JSONObject json = array.getJSONObject(i);
				if(StringUtils.isNotEmpty(json.getString("key")) &&
						StringUtils.isNotEmpty(json.getString("value"))) {
				Metadata e = new Metadata();
				e.setObject_id(object_id);
				e.setKey(json.getString("key"));
				e.setValue(json.getString("value"));
				e.setDescription(json.getString("description"));
				list.add(e);
				};
			}
			metadata_service.updateMetadataList(object_id, list);
			//
			return new JSONResult(JSONResult.SUCCESS, "메타데이터를 저장하였습니다.");
		} catch (Exception e) {
			log.error("Insert Exception : " + e.getMessage(), e);
			return new JSONResult(JSONResult.ERROR, "메타데이터를 저장할 수 없습니다. : 메세지 = [" + e.getMessage() + "]");
		}
	}

}
