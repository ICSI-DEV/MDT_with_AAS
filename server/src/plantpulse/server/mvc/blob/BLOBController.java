package plantpulse.server.mvc.blob;

import java.io.File;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;

import javax.servlet.http.HttpServletRequest;

import org.apache.commons.io.FileUtils;
import org.apache.commons.io.FilenameUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.json.JSONArray;
import org.json.JSONObject;
import org.realdisplay.framework.util.StringUtils;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Controller;
import org.springframework.ui.Model;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.ResponseBody;

import plantpulse.cep.service.BLOBService;
import plantpulse.server.mvc.JSONResult;
import plantpulse.server.mvc.file.DownloadKeys;
import plantpulse.server.mvc.util.DateUtils;

@Controller
public class BLOBController {


	private static final Log log = LogFactory.getLog(BLOBController.class);
	
	private static final int EXPORT_LIMIT = 1000; //TODO 출력 천만건 제안

	
	@Autowired
	private BLOBService blob_service;
	
	
	@RequestMapping(value = "/blob/index", method = RequestMethod.GET)
	public String index(Model model, HttpServletRequest request) throws Exception {
		return "blob/index";
	}
	
	
	@RequestMapping(value = "/blob/store", method = RequestMethod.GET)
	public String store(Model model, HttpServletRequest request) throws Exception {
		
		String search_date_from = DateUtils.currDateBy00();
		String search_date_to = DateUtils.currDateBy24();
		model.addAttribute("search_date_from", search_date_from);
		model.addAttribute("search_date_to", search_date_to);
		
		return "blob/store";
	}
	//
	
	@RequestMapping(value = "/blob/search", method = RequestMethod.GET)
	public @ResponseBody String search(Model model, HttpServletRequest request) throws Exception {
		
		Map<String,String> params = new HashMap<String, String>();
		params.put("tag_id", request.getParameter("tag_id"));
		params.put("search_date_from", request.getParameter("search_date_from") + ":00.000");
		params.put("search_date_to",   request.getParameter("search_date_to")   + ":59.999");
		//
		JSONArray  data = blob_service.selectBLOBList(params.get("tag_id"), params.get("search_date_from"), params.get("search_date_to"), EXPORT_LIMIT);
		//
		JSONObject result =  new JSONObject();
		result.put("data", data);
		return result.toString();
	}
	
	
	@RequestMapping(value = "/blob/download/{object_id}", method = RequestMethod.POST)
	public @ResponseBody JSONResult download(Model model, @PathVariable String object_id, HttpServletRequest request) {
		try {
			
			Map<String,Object> blob = blob_service.selectBLOBByObjectId(UUID.fromString(object_id));
			String org_file_path = (String) blob.get("file_path");
			String ext   = FilenameUtils.getExtension(org_file_path); 
			//
			String file_dir = System.getProperty("java.io.tmpdir") + "/plantpulse";
			String file_name =  object_id + "." + ext;
			String file_path = StringUtils.cleanPath(file_dir + "/" + file_name);

			// 파일생성
			Map<String,Object> object = blob_service.selectBLOBObject(UUID.fromString(object_id));
			byte[] data = (byte[]) object.get("object");
			if(data == null) { throw new Exception("BYTE[] 데이터가 없습니다 : object_id=[" + object_id + "]"); };
			FileUtils.writeByteArrayToFile(new File(file_path), data);

			request.getSession().setAttribute(DownloadKeys.FILE_PATH, file_path);
			request.getSession().setAttribute(DownloadKeys.SAVE_NAME, file_name);
			request.getSession().setAttribute(DownloadKeys.CONTENT_TYPE, "unknown");

			return new JSONResult(JSONResult.SUCCESS, "BLOB를 정상적으로 출력하였습니다.");
		} catch (Exception e) {
			log.error("BLOB Download exception : " + e.getMessage(), e);
			return new JSONResult(JSONResult.ERROR, "BLOB를 출력할 수 없습니다. : 메세지 = [" + e.getMessage() + "]");
		}
	}
	
	
}
