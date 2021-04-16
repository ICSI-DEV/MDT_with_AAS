package plantpulse.server.mvc.timeline;

import java.util.List;

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

import plantpulse.cep.service.AssetService;
import plantpulse.cep.service.SiteService;
import plantpulse.cep.service.TagService;
import plantpulse.cep.service.TimelineService;
import plantpulse.domain.Asset;
import plantpulse.domain.Site;
import plantpulse.server.mvc.util.DateUtils;

@Controller
public class TimelineController {
	
	private static final Log log = LogFactory.getLog(TimelineController.class);
	
	@Autowired
	private TagService tag_service;

	@Autowired
	private SiteService site_service;
	
	@Autowired
	private AssetService asset_service;
	
	@Autowired
	private TimelineService timeline_service;
	
	
	@RequestMapping(value = "/timeline/index", method = RequestMethod.GET)
	public String index(Model model, HttpServletRequest request) throws Exception {
		String search_date_from = DateUtils.currDateBy00();
		String search_date_to = DateUtils.currDateBy24();
		List<Site> site_list = site_service.selectSites();
		
		model.addAttribute("search_date_from", search_date_from);
		model.addAttribute("search_date_to", search_date_to);
		model.addAttribute("site_list", site_list);
		model.addAttribute("selected_site_id", (site_list != null && site_list.size() > 0) ? site_list.get(0).getSite_id() : "");
		model.addAttribute("asset", new Asset());
		
		return "timeline/index";
	}
	
	@RequestMapping(value = "/timeline/search", method = RequestMethod.GET)
	public @ResponseBody String search(Model model, HttpServletRequest request) throws Exception {
		String asset_id   = request.getParameter("asset_id");
		String from     = request.getParameter("search_date_from") + ":00";
		String to       = request.getParameter("search_date_to")   + ":59";
		String condition       = request.getParameter("condition");
		int  limit      = Integer.parseInt(request.getParameter("limit"));
		Asset asset = asset_service.selectAsset(asset_id);
		JSONObject t = JSONObject.fromObject(asset);
		JSONArray  data = timeline_service.selectTimeline(asset, from, to, limit);
		JSONObject result =  new JSONObject();
		result.put("asset", t);
		result.put("data", data);
		return result.toString();
	}
	
	
	@RequestMapping(value = "/timeline/alarm", method = RequestMethod.GET)
	public @ResponseBody String alarm(Model model, HttpServletRequest request) throws Exception {
		String asset_id   = request.getParameter("asset_id");
		String from     = request.getParameter("search_date_from") + ":00";
		String to       = request.getParameter("search_date_to")   + ":59";
		String condition       = request.getParameter("condition");
		int  limit      = Integer.parseInt(request.getParameter("limit"));
		Asset asset = asset_service.selectAsset(asset_id);
		JSONObject t = JSONObject.fromObject(asset);
		JSONArray data = timeline_service.selectAssetAlarmList(asset, from, to, limit);
		
		JSONObject result =  new JSONObject();
		result.put("asset", t);
		result.put("data", data);
		
		
		return result.toString();
	}
	
	
	@RequestMapping(value = "/timeline/asset/data", method = RequestMethod.GET)
	public @ResponseBody String assetData(Model model, HttpServletRequest request) throws Exception {
		String asset_id   = request.getParameter("asset_id");
		long timestamp   = Long.parseLong(request.getParameter("timestamp"));
		
		String from     = DateUtils.fmtISO(timestamp-1000);
		String to       = DateUtils.fmtISO(timestamp+1000);
		
		Asset asset = asset_service.selectAsset(asset_id);
		JSONObject t = JSONObject.fromObject(asset);
		JSONArray data = timeline_service.selectAssetDataList(asset, from, to, 1);
		JSONObject json = new JSONObject();
		if(data != null && data.size() > 0){
			json = data.getJSONObject(0);
		};
		
		JSONObject result =  new JSONObject();
		result.put("asset", t);
		result.put("data", json);
		
		return result.toString();
	}
	
}
