package plantpulse.server.mvc.map;

import java.time.LocalDateTime;
import java.time.ZoneId;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Date;
import java.util.List;

import javax.servlet.http.HttpServletRequest;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.json.JSONObject;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Controller;
import org.springframework.ui.Model;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.ResponseBody;

import plantpulse.cep.service.AssetService;
import plantpulse.cep.service.OPCService;
import plantpulse.cep.service.SiteService;
import plantpulse.cep.service.TagService;
import plantpulse.cep.service.client.StorageClient;
import plantpulse.diagnostic.Diagnostic;
import plantpulse.diagnostic.DiagnosticListCache;
import plantpulse.domain.Asset;
import plantpulse.domain.OPC;
import plantpulse.domain.Site;

@Controller
public class MapController {

	private static final Log log = LogFactory.getLog(MapController.class);

	@Autowired
	private SiteService siteService;
	
	@Autowired
	private AssetService assetService;
	
	@Autowired
	private OPCService opc_service = new OPCService();
	
	@Autowired
	private TagService tagService;
	
	@Autowired
	private StorageClient storage_client;
	

	@RequestMapping(value = "/map/index", method = RequestMethod.GET)
	public String index(Model model, HttpServletRequest request) throws Exception {
		
		model.addAttribute("company", siteService.getCompany());

		List<Site> site_list = siteService.selectSites();
		model.addAttribute("site_list", site_list);
		
		List<Asset> model_list = assetService.selectModels();
		model.addAttribute("model_list", model_list);
		
		List<Asset> part_list = assetService.selectParts();
		model.addAttribute("part_list", part_list);
		
		long total_tag_count = tagService.selectTagCountTotal();
		model.addAttribute("total_tag_count", total_tag_count);
		
		
		//

		return "map/index";
	}

	
	
	@RequestMapping(value = "/map/site/{site_id}", method = RequestMethod.GET)
	public String site(@PathVariable String site_id, Model model, HttpServletRequest request) throws Exception {

		Site site = siteService.selectSiteInfo(site_id);
		model.addAttribute("site", site);

		return "map/site";
	}

	@RequestMapping(value = "/map/summary/{site_id}", method = RequestMethod.GET)
	public @ResponseBody String summary(@PathVariable String site_id, Model model, HttpServletRequest request) throws Exception {
		//
		Site site = siteService.selectSiteInfo(site_id);
		//
		Date timestamp = new Date();
		LocalDateTime local_datetime = timestamp.toInstant().atZone(ZoneId.systemDefault()).toLocalDateTime();
		//
		int year  = local_datetime.getYear();
		int month = local_datetime.getMonthValue();
		int day   = local_datetime.getDayOfMonth();
		//
		JSONObject json = new JSONObject();
		try {
		  json = siteService.selectSiteSummary(site, year, month, day);
		}catch(Exception ex) {
			json.put("site_id", site.getSite_id());
			json.put("data_count", 0);
			json.put("alarm_count", 0);
			json.put("alarm_count_by_info", 0);
			json.put("alarm_count_by_warn", 0);
			json.put("alarm_count_by_error", 0);
			json.put("data_count", 0L);
			log.error("Site summary getting error : site=[" + site.getSite_id() + "] : " + ex.getMessage());
		}
		return json.toString();
	}
	
	
	@RequestMapping(value = "/map/opc/connections", method = RequestMethod.GET)
	public @ResponseBody String connections(Model model, HttpServletRequest request) throws Exception {
		//
		//
		JSONObject result = new JSONObject();
		
		try{
			//
			result.put("total_connection_count", opc_service.selectOpcCount());
			result.put("red_count",    0);
			result.put("orange_count", 0);
			result.put("green_count",  0);
			result.put("gray_count",   0);
			//
			List<OPC> opc_list = opc_service.selectOpcList();
			for(int i=0; i < opc_list.size(); i++) {
				OPC opc = opc_list.get(i);
				JSONObject json =  storage_client.forSelect().selectOPCPointLastUpdate(opc);
				
				//
				long current_timesatmp = System.currentTimeMillis();
				//
				if(json.containsKey("last_update_timestamp") ){
					long update_timesatmp = json.getLong("last_update_timestamp");
					long updated_ms = current_timesatmp - update_timesatmp;
					if(updated_ms > ((60*1000) * 30)){
						result.put("red_count",    result.getInt("red_count") + 1);
					}else if(updated_ms > ((60*1000) * 10)){
						result.put("orange_count", result.getInt("orange_count") + 1);
					}else {
						result.put("green_count",  result.getInt("green_count") + 1);
					}
				}else{
					result.put("gray_count", result.getInt("gray_count") + 1);
				}
				//
			}
			
		}catch(Exception ex){
			log.error("OPC connections status getting error : " + ex.getMessage(), ex);
		}
		return result.toString();
	}

	@RequestMapping(value = "/map/total", method = RequestMethod.GET)
	public @ResponseBody String total(Model model, HttpServletRequest request) throws Exception {

		//
		JSONObject result = new JSONObject();

		long data_count = 0;
		long alarm_count = 0;
		long alarm_count_by_info = 0;
		long alarm_count_by_warn = 0;
		long alarm_count_by_error = 0;

		Date timestamp = new Date();
		LocalDateTime local_datetime = timestamp.toInstant().atZone(ZoneId.systemDefault()).toLocalDateTime();
		
		
		int year  = local_datetime.getYear();
		int month = local_datetime.getMonthValue();
		int day   = local_datetime.getDayOfMonth();
		
		List<Site> site_list = siteService.selectSites();
		for (int i = 0; i < site_list.size(); i++) {
			Site site = site_list.get(i);
			
			
			JSONObject json = new JSONObject();
			try {
			  json = siteService.selectSiteSummary(site, year, month, day);
			}catch(Exception ex) {
				json.put("site_id", site.getSite_id());
				json.put("data_count", 0);
				json.put("alarm_count", 0);
				json.put("alarm_count_by_info", 0);
				json.put("alarm_count_by_warn", 0);
				json.put("alarm_count_by_error", 0);
				json.put("data_count", 0L);
				log.error("Site summary getting error : site=[" + site.getSite_id() + "] : " + ex.getMessage());
			}
			
			data_count += json.getLong("data_count");
		}
		
		alarm_count = DiagnosticListCache.getInstance().getList().size();
		List<Diagnostic> diagnostic_list  = new ArrayList(DiagnosticListCache.getInstance().getList());
		Collections.reverse(diagnostic_list);
		for (int i = 0; i < diagnostic_list.size(); i++) {
			Diagnostic diagnostic = diagnostic_list.get(i);
			if(diagnostic.getLevel().equals(Diagnostic.LEVEL_INFO)){
				alarm_count_by_info++;
			}
			if(diagnostic.getLevel().equals(Diagnostic.LEVEL_WARN)){
				alarm_count_by_warn++;
			}
			if(diagnostic.getLevel().equals(Diagnostic.LEVEL_ERROR)){
				alarm_count_by_error++;
			}
		}
		
		result.put("data_count", data_count);
		result.put("alarm_count", alarm_count);
		result.put("alarm_count_by_info", alarm_count_by_info);
		result.put("alarm_count_by_warn", alarm_count_by_warn);
		result.put("alarm_count_by_error", alarm_count_by_error);

		return result.toString();
	}
	
	
	@RequestMapping(value = "/map/clusters", method = RequestMethod.GET)
	public @ResponseBody String clusters(Model model, HttpServletRequest request) throws Exception {
		JSONObject json = storage_client.forSelect().selectClusterStatusLast();
		return json.toString();
	}
	
	@RequestMapping(value = "/map/reset", method = RequestMethod.GET)
	public @ResponseBody String resetAggregation(Model model, HttpServletRequest request) throws Exception {
		return "OK";
	}
	
	@RequestMapping(value = "/map/diaclear", method = RequestMethod.GET)
	public String diaclear(Model model, HttpServletRequest request) throws Exception {

		List<Site> site_list = siteService.selectSites();
		model.addAttribute("site_list", site_list);
		
		//
		DiagnosticListCache.getInstance().setList(new ArrayList<Diagnostic>());

		return "map/index";
	};
	

}