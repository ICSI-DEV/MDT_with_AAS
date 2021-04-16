package plantpulse.server.mvc.snapshot;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import javax.servlet.http.HttpServletRequest;

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

import plantpulse.cep.engine.utils.CSVUtils;
import plantpulse.cep.service.SiteService;
import plantpulse.cep.service.StorageService;
import plantpulse.cep.service.TagService;
import plantpulse.domain.Site;
import plantpulse.server.mvc.JSONResult;
import plantpulse.server.mvc.file.DownloadKeys;
import plantpulse.server.mvc.util.DateUtils;

/**
 * SnapshotController
 * 
 * @author lsb
 *
 */
@Controller
public class SnapshotController {

	private static final Log log = LogFactory.getLog(SnapshotController.class);

	@Autowired
	private StorageService storage_service;

	@Autowired
	private SiteService site_service;

	@Autowired
	private TagService tag_service;

	@RequestMapping(value = "/snapshot/index", method = RequestMethod.GET)
	public String index(Model model, HttpServletRequest request) throws Exception {
		String search_date_from = DateUtils.currDateByMinus10Minutes();
		String search_date_to = DateUtils.currDateBy24();
		model.addAttribute("total_log_count", 0);
		model.addAttribute("search_date_from", search_date_from);
		model.addAttribute("search_date_to", search_date_to);

		List<Site> site_list = site_service.selectSites();
		model.addAttribute("site_list", site_list);
		model.addAttribute("selected_site_id", (site_list != null && site_list.size() > 0) ? site_list.get(0).getSite_id() : "");
		return "snapshot/index";
	}

	@RequestMapping(value = "/snapshot/search/{site_id}/{search_date_from}/{search_date_to}/{term}", method = RequestMethod.GET)
	public @ResponseBody String search(@PathVariable String site_id, @PathVariable String search_date_from, @PathVariable String search_date_to, @PathVariable String term, HttpServletRequest request) throws Exception {
		JSONArray array = storage_service.snapshot(site_id, search_date_from + ":00", search_date_to + ":59", term, 100);
		return array.toString();
	}

	@RequestMapping(value = "/snapshot/export", method = RequestMethod.POST)
	public @ResponseBody JSONResult export(Model model, HttpServletRequest request) throws Exception {

		try {

			String site_id = request.getParameter("site_id");
			String from = request.getParameter("search_date_from") + ":00";
			String to = request.getParameter("search_date_to") + ":59";
			String tag_ids = request.getParameter("tag_ids");
			String term = request.getParameter("term");

			//
			long start = System.currentTimeMillis();
			JSONArray array = storage_service.snapshot(site_id, from, to, term, 100000); //TODO 스냅샷 출력은 10만건으로 제한
			long end = System.currentTimeMillis() - start;
			
			log.info("Snapshot search time = [" + (end/1000) + "]sec");
			
			//
			String file_dir = System.getProperty("java.io.tmpdir") + "/plantpulse";
			String file_name = "TAG_SNAPSHOT_" + plantpulse.server.mvc.util.DateUtils.fmtSuffix() + ".csv";
			String file_path = StringUtils.cleanPath(file_dir + "/" + file_name);

			//
			String[] tag_id_array = tag_ids.split(",");
			String[] header_array = new String[tag_id_array.length + 1];
			header_array[0] = "TIMESTAMP";
			for (int x = 1; x < tag_id_array.length + 1; x++) {
				header_array[x] = tag_id_array[x - 1];
			}

			//
			ArrayList<Map<String, Object>> list = new ArrayList<Map<String, Object>>();
			for (int i = 0; i < array.size(); i++) {
				JSONObject json = array.getJSONObject(i);
				//
				Map<String, Object> map = new HashMap<String, Object>();
				map.put("TIMESTAMP", DateUtils.fmtISO(json.getLong("timestamp"))); // format
																					// date
				for (int t = 0; t < tag_id_array.length; t++) {
					String value = (String) json.getJSONObject("data").get(tag_id_array[t]);
					map.put(tag_id_array[t], value); // 컬럼 값 추가
				}
				list.add(map);
			}

			CSVUtils.writeForSnapshot(header_array, list, file_path);

			// 파일생성
			request.getSession().setAttribute(DownloadKeys.FILE_PATH, file_path);
			request.getSession().setAttribute(DownloadKeys.SAVE_NAME, file_name);
			request.getSession().setAttribute(DownloadKeys.CONTENT_TYPE, "unknown");

			return new JSONResult(JSONResult.SUCCESS, "CSV을 정상적으로 출력하였습니다.");
		} catch (Exception e) {
			log.error("SnapshotController.export() Exception : " + e.getMessage(), e);
			return new JSONResult(JSONResult.ERROR, "CSV을 출력할 수 없습니다. : 메세지 = [" + e.getMessage() + "]");
		}
	}

}
