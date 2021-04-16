package plantpulse.server.mvc.storage;

import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.Collections;
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
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.ResponseBody;

import plantpulse.cep.engine.utils.NumberUtils;
import plantpulse.cep.engine.utils.TagUtils;
import plantpulse.cep.service.ExcelService;
import plantpulse.cep.service.ExcelSheetData;
import plantpulse.cep.service.SiteService;
import plantpulse.cep.service.StorageService;
import plantpulse.cep.service.TagService;
import plantpulse.cep.stats.PointArrayStatistics;
import plantpulse.domain.Site;
import plantpulse.domain.Tag;
import plantpulse.server.mvc.JSONResult;
import plantpulse.server.mvc.file.DownloadKeys;
import plantpulse.server.mvc.util.DateUtils;

@Controller
public class StorageController {

	private static final Log log = LogFactory.getLog(StorageController.class);

	@Autowired
	private StorageService storage_service;

	@Autowired
	private SiteService site_service;

	@Autowired
	private TagService tag_service;

	@Autowired
	private ExcelService excel_service;

	@RequestMapping(value = "/storage/index", method = RequestMethod.GET)
	public String index(Model model, HttpServletRequest request) throws Exception {
		String search_date_from = DateUtils.currDateByMinus10Minutes();
		String search_date_to = DateUtils.currDateBy24();
		model.addAttribute("total_log_count", 0);
		model.addAttribute("search_date_from", search_date_from);
		model.addAttribute("search_date_to", search_date_to);

		List<Site> site_list = site_service.selectSites();
		model.addAttribute("site_list", site_list);
		model.addAttribute("selected_site_id", (site_list != null && site_list.size() > 0) ? site_list.get(0).getSite_id() : "");
		return "storage/index";
	};
	
	
   /**
    * 검색 날자 구간
    * @param model
    * @param request
    * @return
    * @throws Exception
    */
	@RequestMapping(value = "/storage/search", method = RequestMethod.GET)
	public @ResponseBody String search(Model model, HttpServletRequest request) throws Exception {
		JSONObject result = new JSONObject();
		try {
			String from = request.getParameter("search_date_from") + ":00";
			String to = request.getParameter("search_date_to") + ":59";
			//
			String site_id = request.getParameter("site_id");
			String tag_id = request.getParameter("tag_id");
			String sampling = request.getParameter("sampling");
			String term = request.getParameter("term");
			String limit = request.getParameter("limit");
			if(StringUtils.isEmpty(limit)) {
				limit = "0";
			};
			result = data(from, to, site_id, tag_id, sampling, term, Integer.parseInt(limit));
			//
			return result.toString();
		} catch (Exception ex) {
			log.error("Storage search failed : " + ex.getMessage(), ex);
			return result.toString();
		}
	};
	
	
	/**
	    * 검색 제한
	    * @param model
	    * @param request
	    * @return
	    * @throws Exception
	    */
		@RequestMapping(value = "/storage/limit", method = RequestMethod.GET)
		public @ResponseBody String limit(Model model, HttpServletRequest request) throws Exception {
			JSONObject result = new JSONObject();
			try {
				//
				String from = "1900-01-01 00:00:00";
				String to   = "2999-12-31 23:59:59";
				//
				String site_id = request.getParameter("site_id");
				String tag_id = request.getParameter("tag_id");
				String sampling = request.getParameter("sampling");
				String term = request.getParameter("term");
				String limit = request.getParameter("limit");
				if(StringUtils.isEmpty(limit)) {
					limit = "0";
				};
				result = data(from, to, site_id, tag_id, sampling, term, Integer.parseInt(limit));
				//
				return result.toString();
			} catch (Exception ex) {
				log.error("Storage search failed : " + ex.getMessage(), ex);
				return result.toString();
			}
		};

	/**
	 * 포인트 데이터 조회
	 * @param from
	 * @param to
	 * @param site_id
	 * @param tag_id
	 * @param sampling
	 * @param term
	 * @return
	 * @throws Exception
	 */
	private JSONObject data(String from, String to, String site_id, String tag_id, String sampling, String term, int limit) throws Exception {
		
		JSONObject result = new JSONObject();
		try {
			
			long start = System.currentTimeMillis();
	
			//
			Site site = site_service.selectSiteInfo(site_id);
			Tag tag = tag_service.selectTag(tag_id);
	
			if (site == null) {
				throw new Exception("Site not found exception : " + site_id);
			}
			if (tag == null) {
				throw new Exception("Tag not found exception  : " + tag_id);
			};
			
			//
			
			JSONArray data = new JSONArray();
			
			//
			int   count  = 0;
			String first = "";
			String last  = "";
			String min   = "";
			String max   = "";
			String sum   = "";
			String avg   = "";
			String stddev = "";
			//
			try {
				
				 data = storage_service.search(tag, from, to, sampling, term, limit);
				 
				//log.info(tag.toString());
				if(data != null && data.size() > 0 ) {
				
					if ( TagUtils.isNumbericDataType(tag)) {     //숫자형일 경우
						PointArrayStatistics stats = new PointArrayStatistics(data);
						count =  stats.getCount();
						first =  NumberUtils.numberToString(stats.getFirst());
						last  =  NumberUtils.numberToString(stats.getLast());
						min   =  NumberUtils.numberToString( stats.getMin());
						max   =  NumberUtils.numberToString(stats.getMax());
						sum   =  NumberUtils.numberToString(stats.getSum());
						avg   =  NumberUtils.numberToString(stats.getAvg());
						stddev = NumberUtils.numberToString(stats.getStddev());
						
					} else if(TagUtils.isBooleanDataType(tag)) { //불린형일 경우
						//true = 1, false = 2 로 변경
						JSONArray array = new JSONArray();
						for(int i=0; i < data.size(); i++) {
							long ts = data.getJSONArray(i).getLong(0);
							boolean bool_value = data.getJSONArray(i).getBoolean(1);
							//
							JSONArray one = new JSONArray();
							one.add(ts);
							if(bool_value == true)  { one.add(1); }; 
							if(bool_value == false) { one.add(0);  };
							array.add(i, one);
						}
						data = array;
						PointArrayStatistics stats = new PointArrayStatistics(data);
						count =  stats.getCount();
						first =  NumberUtils.numberToString(stats.getFirst());
						last  =  NumberUtils.numberToString(stats.getLast());
						min   =   "";
						max   =   "";
						sum   =   NumberUtils.numberToString(stats.getSum());
						avg   =   "";
						stddev = "";
						
						
					} else if(TagUtils.isStringDataType(tag)) { //스트링형일 경우
						count =  data.size();
						first =  data.getJSONArray(0).getString(1);
						last  =  data.getJSONArray(count - 1).getString(1);
						min   =   "";
						max   =   "";
						sum   =   "";
						avg   =   "";
						stddev = "";
						
					} else if(TagUtils.isTimestampDataType(tag)) { //타임스탬프형일 경우
						count =  data.size();
						first =  NumberUtils.numberToString(data.getJSONArray(0).getLong(1));
						last  =  NumberUtils.numberToString(data.getJSONArray(count - 1).getLong(1));
						min   =  "";
						max   =  "";
						sum   =  "";
						avg   =  "";
						stddev = "";
					}
					
				//데이터가 없을 경우, 오버라이드
				} else {
					count = 0;
					first =  "";
					last  =  "";
					min   =  "";
					max   =  "";
					sum   =  "";
					avg   =  "";
					stddev = "";
			    };
				
			} catch (Exception ex) {
				log.error("Point data search failed.", ex);
			}
			long exec_time = System.currentTimeMillis() - start;
			
			//
			JSONObject summary = JSONObject.fromObject(tag);
			//요약 (태그 및 통계 및 기타 정보  추가)
			summary.put("count", count);
			summary.put("first", first);
			summary.put("last",  last);
			summary.put("min",  min);
			summary.put("max",  max);
			summary.put("sum",  sum);
			summary.put("avg",  avg);
			summary.put("stddev",   stddev);
			
			summary.put("exec_time", exec_time);
			
			result.put("count",   count);
			result.put("summary", summary);
			result.put("data",    data);
			
			
		} catch (Exception e) {
			log.error("StorageController.data() Exception : " + e.getMessage(), e);
		}
		
		return result;
		
	}
	
	
	/**
	 * search_asset
	 * @param model
	 * @param request
	 * @return
	 * @throws Exception
	 */
	@RequestMapping(value = "/storage/asset/search", method = RequestMethod.GET)
	public @ResponseBody String searchForAsset(Model model, HttpServletRequest request) throws Exception {
		JSONObject result = new JSONObject();
		try {
			String from = request.getParameter("search_date_from") + ":00";
			String to = request.getParameter("search_date_to") + ":59";

			String site_id = request.getParameter("site_id");
			String tag_id = request.getParameter("tag_id");
			String sampling = request.getParameter("sampling");
			String term = request.getParameter("term");

			result = dataForAsset(from, to, site_id, tag_id, sampling, term);
			//
			return result.toString();
		} catch (Exception ex) {
			log.error("Storage search failed : " + ex.getMessage(), ex);
			return result.toString();
		}
	}

	/**
	 * asset_data
	 * @param from
	 * @param to
	 * @param site_id
	 * @param tag_id
	 * @param sampling
	 * @param term
	 * @return
	 * @throws Exception
	 */
	private JSONObject dataForAsset(String from, String to, String site_id, String tag_id, String sampling, String term) throws Exception {
		
		JSONObject result = new JSONObject();
		try {
			
			long start = System.currentTimeMillis();
	
			//
			Site site = site_service.selectSiteInfo(site_id);
			Tag tag = tag_service.selectTag(tag_id);
	
			if (site == null) {
				throw new Exception("Site not found exception : " + site_id);
			}
			if (tag == null) {
				throw new Exception("Tag not found exception  : " + tag_id);
			};
			
			//
			int count = 0;
			Object first = null;
			Object last = null;
			JSONArray data = new JSONArray();
			JSONObject agg = new JSONObject();
			
			try {
				data = storage_service.search(tag, from, to, sampling, term, -1);
				if (data != null && data.size() > 0) {
					count = data.size();
					first = data.getJSONArray(0).get(1) + "";
					last =  data.getJSONArray(data.size() - 1).get(1) + "";
				} else {
					count = 0;
					first = "";
					last  = "";
				}
			} catch (Exception ex) {
				log.error("Point data search failed.", ex);
			}
			long exec_time = System.currentTimeMillis() - start;
			
			
			//
			JSONObject summary = JSONObject.fromObject(tag);
			summary.putAll(agg); //
			summary.put("count", count);
			summary.put("first", first);
			summary.put("last", last);
			summary.put("exec_time", exec_time);
			
			result.put("count", count);
			result.put("summary", summary);
			result.put("data", data);
			
			
		} catch (Exception e) {
			log.error("StorageController.data() Exception : " + e.getMessage(), e);
		}
		
		return result;
		
	}

	@RequestMapping(value = "/storage/export", method = RequestMethod.POST)
	public @ResponseBody JSONResult export(Model model, HttpServletRequest request) {
		try {

			String from = request.getParameter("search_date_from") + ":00";
			String to = request.getParameter("search_date_to") + ":59";
			String site_id = request.getParameter("site_id");
			String tag_ids = request.getParameter("tag_ids");
			String sampling = request.getParameter("sampling");
			String term = request.getParameter("term");

			//
			String file_dir = System.getProperty("java.io.tmpdir") + "/plantpulse";
			String file_name = "TAG_DATA_" + plantpulse.server.mvc.util.DateUtils.fmtSuffix() + ".xls";
			String file_path = StringUtils.cleanPath(file_dir + "/" + file_name);

			String[] tag_id_array = tag_ids.split(",");

			ExcelSheetData[] sheet_array = new ExcelSheetData[tag_id_array.length];
			for (int i = 0; i < tag_id_array.length; i++) {
				JSONObject json = data(from, to, site_id, tag_id_array[i], sampling, term, -1);
				ExcelSheetData sheet = new ExcelSheetData();
				// String tag_name =
				// json.getJSONObject("summary").getString("tag_name");
				// if(tag_name.length() < 31){
				// sheet.setName(tag_name);
				// }else{
				sheet.setName(tag_id_array[i]);
				// }
				JSONArray data_array = json.getJSONArray("data");
				List<Map<String, Object>> map_list = new ArrayList<Map<String, Object>>();
				for (int d = 0; d < data_array.size(); d++) {
					Map<String, Object> data = new HashMap<String, Object>();
					//
					data.put("TAG_ID", json.getJSONObject("summary").getString("tag_id"));
					data.put("TAG_NAME", json.getJSONObject("summary").getString("tag_name"));
					data.put("DATA_TYPE", json.getJSONObject("summary").getString("java_type"));
					data.put("UNIT", json.getJSONObject("summary").getString("unit"));
					//
					data.put("TIMESTAMP", new Timestamp(data_array.getJSONArray(d).getLong(0)));
					data.put("VALUE", data_array.getJSONArray(d).get(1) + "");
					map_list.add(data);
				}
				Collections.reverse(map_list);
				sheet.setMapList(map_list);
				String[] headerLabelArray = new String[] { "TAG_ID", "TAG_NAME", "DATA_TYPE", "TIMESTAMP", "VALUE", "UNIT" };
				String[] valueKeyArray = new String[] { "TAG_ID", "TAG_NAME", "DATA_TYPE", "TIMESTAMP", "VALUE", "UNIT" };
				sheet.setHeaderLabelArray(headerLabelArray);
				sheet.setValueKeyArray(valueKeyArray);
				//
				sheet_array[i] = sheet;
			}
			;

			// 파일생성
			excel_service.writeExcel(sheet_array, file_path);

			request.getSession().setAttribute(DownloadKeys.FILE_PATH, file_path);
			request.getSession().setAttribute(DownloadKeys.SAVE_NAME, file_name);
			request.getSession().setAttribute(DownloadKeys.CONTENT_TYPE, "unknown");

			return new JSONResult(JSONResult.SUCCESS, "엑셀을 정상적으로 출력하였습니다.");
		} catch (Exception e) {
			log.error(e.getMessage(), e);
			return new JSONResult(JSONResult.ERROR, "엑셀을 출력할 수 없습니다. : 메세지 = [" + e.getMessage() + "]");
		}
	}

	@RequestMapping(value = "/storage/total", method = RequestMethod.GET, headers = "Accept=*/*", produces = "application/json")
	public @ResponseBody long total(Model model, HttpServletRequest request) throws Exception {

		try {
			//String site_id = request.getParameter("site_id");
			//Site site = site_service.selectSiteInfo(site_id);
			long count = storage_service.getTotalDataCount();
			//
			return count;
		} catch (Exception ex) {
			log.error(ex.getMessage(), ex);
			return 0;
		}
	}

}
