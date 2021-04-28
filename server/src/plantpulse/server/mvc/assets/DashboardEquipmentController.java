package plantpulse.server.mvc.assets;

import java.time.LocalDateTime;
import java.time.ZoneId;
import java.util.Date;
import java.util.List;

import javax.servlet.http.HttpServletRequest;

import org.apache.commons.lang.StringUtils;
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

import plantpulse.cep.domain.AssetStatement;
import plantpulse.cep.engine.CEPEngineManager;
import plantpulse.cep.engine.utils.TimestampUtils;
import plantpulse.cep.service.AssetService;
import plantpulse.cep.service.SiteService;
import plantpulse.cep.service.TagService;
import plantpulse.cep.service.TimelineService;
import plantpulse.cep.service.client.StorageClient;
import plantpulse.domain.Asset;
import plantpulse.domain.Site;
import plantpulse.server.mvc.util.DateUtils;

@Controller
public class DashboardEquipmentController {

	private static final Log log = LogFactory.getLog(DashboardEquipmentController.class);

	@Autowired
	private SiteService site_service;
	
	@Autowired
	private AssetService asset_service;
	
	@Autowired
	private TagService tag_service;
	
	@Autowired
	private TimelineService timeline_service;

	@Autowired
	private StorageClient storage_client;
	
	//조회 제한
	public static final int LIMIT_ASSET_TAG_LIST    = 1000;
	
	public static final int LIMIT_ASSET_DATA_LIST        = 1;
	public static final int LIMIT_ASSET_ALARM_LIST       = 100;
	public static final int LIMIT_ASSET_TIMELINE_LIST    = 100;
	public static final int LIMIT_ASSET_CONTEXT_LIST     = 100;
	public static final int LIMIT_ASSET_AGGREGATION_LIST = 100;
	
	public static final int LIMIT_ASSET_ALL_LIST         = 20;

	
	@RequestMapping(value = "/assets/dashboard/equipment/{asset_id}", method = RequestMethod.GET)
	public String index(@PathVariable String asset_id, Model model, HttpServletRequest request) throws Exception {
		List<Site> site_list = site_service.selectSites();
		model.addAttribute("site_list", site_list);
		
		Asset asset = asset_service.selectAsset(asset_id);
		model.addAttribute("asset", asset);
		model.addAttribute("area", asset_service.selectAsset(asset.getParent_asset_id()));
		model.addAttribute("site", site_service.selectSiteInfo(asset.getSite_id()));
		
		model.addAttribute("equip_list", asset_service.selectEquipmentList(asset.getParent_asset_id()));
		
		model.addAttribute("tag_list", tag_service.selectTagListByAssetId(asset_id, LIMIT_ASSET_TAG_LIST));
		//
		Date date = null;
		String date_s = request.getParameter("date");
		if(StringUtils.isNotEmpty(date_s)) {
			date = DateUtils.toDate(date_s);
		}else {
			date = new Date();
		};
		
		model.addAttribute("bf_date", DateUtils.fmtDate(DateUtils.addDays(date, -1)));
		model.addAttribute("to_date", DateUtils.fmtDate(date));
		model.addAttribute("nt_date", DateUtils.fmtDate(DateUtils.addDays(date,  1)));
		//
		return "/assets/dashboard/equipment";
	};
	
	
	/**
	 * 에셋 전체 정보를 반환한다.
	 * 
	 * @param asset_id
	 * @param date
	 * @param model
	 * @param request
	 * @return
	 * @throws Exception
	 */
	@RequestMapping(value = "/assets/dashboard/equipment/get/assetall/{asset_id}/{date}", method = RequestMethod.GET)
	public @ResponseBody String all(@PathVariable("asset_id") String asset_id, @PathVariable("date") String date, Model model, HttpServletRequest request) throws Exception {
        

		//
		//
		String from = date  + " 00:00:00";
		String to   = date  + " 23:59:59";
		
		//
		JSONObject result = new JSONObject();
		result.put("from", DateUtils.toTimestamp(from));
		result.put("to", DateUtils.toTimestamp(to));
       
        //1. 에셋 정보
        Asset asset = asset_service.selectAssetOrSite(asset_id);
        if(asset == null){
        	throw new Exception("Asset is null.");
        };
        
        result.put("asset", JSONObject.fromObject(asset));
        
        //2. 에셋 데이터 리스트
        JSONArray data_list = new JSONArray();
        try{
        	data_list = storage_client.forSelect().selectAssetData(asset,TimestampUtils.todayYYYYMMDD(), from, to, 1);
        }catch(Exception ex){
        	log.warn("Asset data list search error : " + ex.getMessage(), ex);
        };
        result.put("data_list", data_list);
        
        //3. 알람 데이터 리스트
        JSONArray alarm_list = new JSONArray();
        try{
        	alarm_list = storage_client.forSelect().selectAssetAlarm(asset, from, to, LIMIT_ASSET_ALL_LIST);
        }catch(Exception ex){
        	log.warn("Asset alarm list search error : " + ex.getMessage(), ex);
        };
        result.put("alarm_list", alarm_list);
        
        //3. 이벤트 데이터 리스트
        JSONArray event_list = new JSONArray();
        try{
        	event_list = storage_client.forSelect().selectAssetEvent(asset, from, to, LIMIT_ASSET_ALL_LIST);
        }catch(Exception ex){
        	log.warn("Asset event list search error : " + ex.getMessage(), ex);
        };
        result.put("event_list", event_list);
        
       //3. 상황  데이터 리스트
        JSONArray context_list = new JSONArray();
        try{
        	context_list = storage_client.forSelect().selectAssetContext(asset, from, to, LIMIT_ASSET_ALL_LIST);
        }catch(Exception ex){
        	log.warn("Asset context list search error : " + ex.getMessage(), ex);
        };
        result.put("context_list", context_list);
        
        //3. 집계 데이터 리스트
        JSONArray aggregation_list = new JSONArray();
        try{
        	aggregation_list = storage_client.forSelect().selectAssetAggregation(asset, from, to, LIMIT_ASSET_ALL_LIST);
        }catch(Exception ex){
        	log.warn("Asset aggregation list search error : " + ex.getMessage(), ex);
        };
        result.put("aggregation_list", aggregation_list);
        
        //4. 태그 데이터 리스트
        JSONArray tag_list = new JSONArray();
        try{
        	tag_list = JSONArray.fromObject(tag_service.selectTagListByAssetId(asset.getAsset_id(), LIMIT_ASSET_ALL_LIST)); //태그는 로딩 속도를  위해 100개만
        }catch(Exception ex){
        	log.warn("Asset tag list search error : " + ex.getMessage(), ex);
        };
        result.put("tag_list", tag_list);
        
        //5. 알람 헬스 데이터 리스트
        JSONArray alarm_health_list = new JSONArray();
        try{
        	alarm_health_list = storage_client.forSelect().selectAssetHealthStatus(asset, from, to);
        }catch(Exception ex){
        	log.warn("Asset health list search error : " + ex.getMessage(), ex);
        };
        result.put("alarm_health_list", alarm_health_list);
        
      //5. 이벤트 헬스 데이터 리스트
        JSONArray event_health_list = new JSONArray();
        try{
        	event_health_list = storage_client.forSelect().selectAssetEvent(asset, from, to, LIMIT_ASSET_ALL_LIST);
        }catch(Exception ex){
        	log.warn("Asset event health list search error : " + ex.getMessage(), ex);
        };
        result.put("event_health_list", event_health_list);
        
        //5. 스테이트먼트 데이터 리스트
        JSONArray statement_list = new JSONArray();
        try{
        	List<AssetStatement> statements = asset_service.selectAssetStatementByAssetId(asset.getAsset_id());
        	for(int i=0; statements != null && i < statements.size(); i++){
        		AssetStatement staement = statements.get(i);
        		JSONObject json = JSONObject.fromObject(staement);
        		boolean is_started = false;
        		try{
	        		if(CEPEngineManager.getInstance().getProvider().getEPAdministrator().getStatement(staement.getAsset_id() + "_" + staement.getStatement_name()).isStarted()){
	        			is_started = true;
	        		}
        		}catch(Exception ex){
        			log.error(ex);
        		}
        		json.put("status", is_started);
        		statement_list.add(json);
        	};
        }catch(Exception ex){
        	log.warn("Asset satement list search error : " + ex.getMessage(), ex);
        };
        result.put("statement_list", statement_list);
        
        //
		return result.toString();
	}
	
	
	/**
	 * 에셋의 연결 상태를 조회한다.
	 * 
	 * @param asset_id
	 * @param model
	 * @param request
	 * @return
	 * @throws Exception
	 */
	@RequestMapping(value = "/assets/dashboard/equipment/get/assetconnection/{asset_id}/{date}", method = RequestMethod.GET)
	public @ResponseBody String assetconnection(@PathVariable("asset_id") String asset_id, @PathVariable("date") String date, Model model, HttpServletRequest request) throws Exception {
 
		//
		String from = date  + " 00:00:00";
		String to   = date  + " 23:59:59";
		//
		 Asset asset = asset_service.selectAssetOrSite(asset_id);
		//
		JSONObject result = new JSONObject();
		result.put("from", DateUtils.toTimestamp(from));
		result.put("to", DateUtils.toTimestamp(to));
       
        //5. 헬스 데이터 리스트
        JSONArray health_list = new JSONArray();
        try{
        	health_list = storage_client.forSelect().selectAssetConnectionStatus(asset, from, to);
        }catch(Exception ex){
        	log.warn("Asset connection list search error: " + ex.getMessage());
        };
        result.put("connection_list", health_list);
        
        //
		return result.toString();
	}
	
	/**
	 * 에셋의 데이터를 조회한다.
	 * 
	 * @param asset_id
	 * @param model
	 * @param request
	 * @return
	 * @throws Exception
	 */
	@RequestMapping(value = "/assets/dashboard/equipment/get/assetdata/{asset_id}/{date}", method = RequestMethod.GET)
	public @ResponseBody String assetdata(@PathVariable("asset_id") String asset_id, @PathVariable("date") String date,  Model model, HttpServletRequest request) throws Exception {
		
		//
		 Asset asset = asset_service.selectAssetOrSite(asset_id);
		 //
			String from = date  + " 00:00:00";
			String to   = date  + " 23:59:59";
		//
		JSONObject result = new JSONObject();
		result.put("asset_id", asset_id);
		result.put("from", DateUtils.toTimestamp(from));
		result.put("to",   DateUtils.toTimestamp(to));
		//
		//
        JSONArray data_list = new JSONArray();
        try{
        	data_list = storage_client.forSelect().selectAssetData(asset,TimestampUtils.todayYYYYMMDD(), from, to, LIMIT_ASSET_DATA_LIST);
        }catch(Exception ex){
        	log.warn("Asset data list search error : " + ex.getMessage(), ex);
        };
        result.put("data_list", data_list);
        result.put("exec_time", 0);
        //
    	return result.toString();
	};
	
	
	@RequestMapping(value = "/assets/dashboard/equipment/get/assettimeline/{asset_id}/{date}", method = RequestMethod.GET)
	public @ResponseBody String assettimeline(@PathVariable("asset_id") String asset_id, @PathVariable("date") String date,  Model model, HttpServletRequest request) throws Exception {
		
		//
		String from = date  + " 00:00:00";
		String to   = date  + " 23:59:59";
		//
		Asset asset = asset_service.selectAsset(asset_id);
		JSONObject t = JSONObject.fromObject(asset);
		JSONArray  data = timeline_service.selectTimeline(asset, from, to, LIMIT_ASSET_TIMELINE_LIST);
		JSONObject result =  new JSONObject();
		result.put("asset", t);
		result.put("timeline_list", data);
		return result.toString();
	};
	
	@RequestMapping(value = "/assets/dashboard/equipment/get/assettimeline_alarm/{asset_id}/{date}", method = RequestMethod.GET)
	public @ResponseBody String assettimeline_alarm(@PathVariable("asset_id") String asset_id, @PathVariable("date") String date, Model model, HttpServletRequest request) throws Exception {
		//
		String from = date  + " 00:00:00";
		String to   = date  + " 23:59:59";
		//
		Asset asset = asset_service.selectAsset(asset_id);
		JSONObject t = JSONObject.fromObject(asset);
		JSONArray data = timeline_service.selectAssetAlarmList(asset, from, to, LIMIT_ASSET_ALARM_LIST);
		
		JSONObject result =  new JSONObject();
		result.put("asset", t);
		result.put("timeline_alarm_list", data);
		return result.toString();
	}
	
	
	@RequestMapping(value = "/assets/dashboard/equipment/get/assetcount/{asset_id}/{date}", method = RequestMethod.GET)
	public @ResponseBody String assetcount(@PathVariable("asset_id") String asset_id, @PathVariable("date") String date, Model model, HttpServletRequest request) throws Exception {
		
		//
		String from = date  + " 00:00:00";
		String to   = date  + " 23:59:59";

		//
		JSONObject result = new JSONObject();

		long data_count  = 0;
		long alarm_count = 0;
		//
		long alarm_count_by_info = 0;
		long alarm_count_by_warn = 0;
		long alarm_count_by_error = 0;

		Date timestamp = new Date();
		LocalDateTime local_datetime = timestamp.toInstant().atZone(ZoneId.systemDefault()).toLocalDateTime();
		
		
		int year  = local_datetime.getYear();
		int month = local_datetime.getMonthValue();
		int day   = local_datetime.getDayOfMonth();
		
		//
		Asset asset = asset_service.selectAsset(asset_id);
		
		
		data_count = 0; //TODO 에셋 데이터 카운트 추가
		result.put("data_count",  data_count);
		
	     alarm_count_by_info = storage_client.forSelect().selectAssetAlarmCountByPriority(asset, from, to, "INFO", 1).getLong("count");
	     alarm_count_by_warn = storage_client.forSelect().selectAssetAlarmCountByPriority(asset, from, to, "WARN", 1).getLong("count");
	     alarm_count_by_error = storage_client.forSelect().selectAssetAlarmCountByPriority(asset, from, to, "ERROR", 1).getLong("count");
	     alarm_count = alarm_count_by_info + alarm_count_by_warn + alarm_count_by_error;
	    		 
		
		//
		result.put("alarm_count", alarm_count);
		result.put("alarm_count_by_info",  alarm_count_by_info);
		result.put("alarm_count_by_warn",  alarm_count_by_warn);
		result.put("alarm_count_by_error", alarm_count_by_error);

		return result.toString();
	}
	
	@RequestMapping(value = "/assets/dashboard/equipment/get/assetalarm/{asset_id}/{date}", method = RequestMethod.GET)
	public @ResponseBody String assetalarm(@PathVariable("asset_id") String asset_id, @PathVariable("date") String date, Model model, HttpServletRequest request) throws Exception {
		
		//
		String from = date  + " 00:00:00";
		String to   = date  + " 23:59:59";
		
		//
		JSONObject result = new JSONObject();
		result.put("asset_id", asset_id);
		result.put("from", DateUtils.toTimestamp(from));
		result.put("to",   DateUtils.toTimestamp(to));
		//
        JSONArray data_list = new JSONArray();
        try{
        	//1. 에셋 정보
            Asset asset = asset_service.selectAssetOrSite(asset_id);
            if(asset == null){
            	throw new Exception("Asset is null.");
            };
        	data_list = storage_client.forSelect().selectAssetAlarm(asset, from, to, 100);
        }catch(Exception ex){
        	log.warn("Asset alarm list search error : " + ex.getMessage(), ex);
        };
        result.put("alarm_list", data_list);
        result.put("exec_time", 123);
        //
    	return result.toString();
	};
	
	

	@RequestMapping(value = "/assets/dashboard/equipment/get/assetalarm_pannel/{asset_id}/{date}", method = RequestMethod.GET)
	public String assetalarm_pannel(@PathVariable("asset_id") String asset_id, @PathVariable("date") String date, Model model, HttpServletRequest request) throws Exception {
		
		//
		String from = date  + " 00:00:00";
		String to   = date  + " 23:59:59";
		//
		JSONObject result = new JSONObject();
		result.put("asset_id", asset_id);
		result.put("from", DateUtils.toTimestamp(from));
		result.put("to",   DateUtils.toTimestamp(to));
		//
        JSONArray data_list = new JSONArray();
        try{
        	//1. 에셋 정보
            Asset asset = asset_service.selectAssetOrSite(asset_id);
            if(asset == null){
            	throw new Exception("Asset is null.");
            };
        	data_list = storage_client.forSelect().selectAssetAlarm(asset, from, to, LIMIT_ASSET_ALARM_LIST);
        }catch(Exception ex){
        	log.warn("Asset alarm list search error : " + ex.getMessage(), ex);
        };
        
        //
        model.addAttribute("alarm_list", data_list);
        
		return "/assets/dashboard/pannel/alarm";
	}
	
	
	@RequestMapping(value = "/assets/dashboard/equipment/get/assetcontext_pannel/{asset_id}/{date}", method = RequestMethod.GET)
	public String assetcontext_pannel(@PathVariable("asset_id") String asset_id, @PathVariable("date") String date, Model model, HttpServletRequest request) throws Exception {
		
		//
		String from = date  + " 00:00:00";
		String to   = date  + " 23:59:59";
		//
		JSONObject result = new JSONObject();
		result.put("asset_id", asset_id);
		result.put("from", DateUtils.toTimestamp(from));
		result.put("to",   DateUtils.toTimestamp(to));
		//
        JSONArray data_list = new JSONArray();
        try{
        	//1. 에셋 정보
            Asset asset = asset_service.selectAssetOrSite(asset_id);
            if(asset == null){
            	throw new Exception("Asset is null.");
            };
        	data_list = storage_client.forSelect().selectAssetContext(asset, from, to, LIMIT_ASSET_CONTEXT_LIST);
        }catch(Exception ex){
        	log.warn("Asset context list search error : " + ex.getMessage(), ex);
        };
        
        //
        model.addAttribute("context_list", data_list);
        
		return "/assets/dashboard/pannel/context";
	};
	
	
	@RequestMapping(value = "/assets/dashboard/equipment/get/assetaggregation_pannel/{asset_id}/{date}", method = RequestMethod.GET)
	public String assetaggregation_pannel(@PathVariable("asset_id") String asset_id, @PathVariable("date") String date, Model model, HttpServletRequest request) throws Exception {
		
		//
		String from = date  + " 00:00:00";
		String to   = date  + " 23:59:59";
		//
		JSONObject result = new JSONObject();
		result.put("asset_id", asset_id);
		result.put("from", DateUtils.toTimestamp(from));
		result.put("to",   DateUtils.toTimestamp(to));
		//
        JSONArray data_list = new JSONArray();
        try{
        	//1. 에셋 정보
            Asset asset = asset_service.selectAssetOrSite(asset_id);
            if(asset == null){
            	throw new Exception("Asset is null.");
            };
        	data_list = storage_client.forSelect().selectAssetAggregation(asset, from, to, LIMIT_ASSET_AGGREGATION_LIST);
        }catch(Exception ex){
        	log.warn("Asset aggregation list search error : " + ex.getMessage(), ex);
        };
        
        //
        model.addAttribute("aggregation_list", data_list);
        
		return "/assets/dashboard/pannel/aggregation";
	};

}