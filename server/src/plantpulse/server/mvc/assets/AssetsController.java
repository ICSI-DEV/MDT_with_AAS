package plantpulse.server.mvc.assets;

import java.util.List;

import javax.servlet.http.HttpServletRequest;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.json.JSONArray;
import org.json.JSONObject;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Controller;
import org.springframework.ui.Model;
import org.springframework.web.bind.annotation.ModelAttribute;
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
import plantpulse.cep.service.client.StorageClient;
import plantpulse.domain.Asset;
import plantpulse.domain.Site;
import plantpulse.server.mvc.JSONResult;
import plantpulse.server.mvc.LimitConstants;
import plantpulse.server.mvc.Result;
import plantpulse.server.mvc.util.DateUtils;

/**
 * AssetsController
 * @author leesa
 *
 */
@Controller
@RequestMapping(value = "/assets")
public class AssetsController {

	private static final Log log = LogFactory.getLog(AssetsController.class);

	@Autowired
	private AssetService asset_service;
	
	@Autowired
	private TagService tag_service;

	@Autowired
	private SiteService site_service;
	
	@Autowired
	private StorageClient storage_client;

	@RequestMapping(value = "/index", method = RequestMethod.GET)
	public String index(Model model) throws Exception {

		List<Site> site_list = site_service.selectSites();
		model.addAttribute("site_list", site_list);
		model.addAttribute("selected_site_id", (site_list != null && site_list.size() > 0) ? site_list.get(0).getSite_id() : "");
		model.addAttribute("asset", new Asset());
		return "assets/index";
	}
	
	
	@RequestMapping(value = "/get/{asset_id}", method = RequestMethod.GET)
	public @ResponseBody String get(@PathVariable("asset_id") String asset_id, Model model, HttpServletRequest request) throws Exception {
    
		
		//
		String from   = DateUtils.currDateBy00()  + ":00";
		//String from_1d = DateUtils.currDateByMinus1Day()  + ":00";
		//String from_1m = DateUtils.currDateByMinus1Month()  + ":00";
		String to   = DateUtils.currDateBy24()  + ":59";
		
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
        	alarm_list = storage_client.forSelect().selectAssetAlarm(asset, from, to, LimitConstants.ASSET_DATA_LIST_LIMIT);
        }catch(Exception ex){
        	log.warn("Asset alarm list search error : " + ex.getMessage(), ex);
        };
        result.put("alarm_list", alarm_list);
        
        //3. 이벤트 데이터 리스트
        JSONArray event_list = new JSONArray();
        try{
        	event_list = storage_client.forSelect().selectAssetEvent(asset, from, to, LimitConstants.ASSET_DATA_LIST_LIMIT);
        }catch(Exception ex){
        	log.warn("Asset event list search error : " + ex.getMessage(), ex);
        };
        result.put("event_list", event_list);
        
       //3. 상황  데이터 리스트
        JSONArray context_list = new JSONArray();
        try{
        	context_list = storage_client.forSelect().selectAssetContext(asset, from, to, LimitConstants.ASSET_DATA_LIST_LIMIT);
        }catch(Exception ex){
        	log.warn("Asset context list search error : " + ex.getMessage(), ex);
        };
        result.put("context_list", context_list);
        
        //3. 집계 데이터 리스트
        JSONArray aggregation_list = new JSONArray();
        try{
        	aggregation_list = storage_client.forSelect().selectAssetAggregation(asset, from, to, LimitConstants.ASSET_DATA_LIST_LIMIT);
        }catch(Exception ex){
        	log.warn("Asset aggregation list search error : " + ex.getMessage(), ex);
        };
        result.put("aggregation_list", aggregation_list);
        
        //4. 태그 데이터 리스트
        long tag_count = 0;
        JSONArray tag_list = new JSONArray();
        try{
        	tag_count = tag_service.selectTagCountByLinkedAssetId(asset.getAsset_id());
        	tag_list = JSONArray.fromObject(tag_service.selectTagListByAssetId(asset.getAsset_id(), LimitConstants.ASSET_TAG_LIST_LIMIT)); //태그는 로딩 속도를  위해 100개만
        }catch(Exception ex){
        	log.warn("Asset tag list search error : " + ex.getMessage(), ex);
        };
        result.put("tag_count", tag_count);
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
        	event_health_list = storage_client.forSelect().selectAssetEvent(asset, from, to, LimitConstants.ASSET_DATA_LIST_LIMIT);
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
	
	
	@RequestMapping(value = "/add_statement/{asset_id}", method = RequestMethod.GET)
	public String add_statement(@PathVariable String asset_id, @ModelAttribute AssetStatement statement, Model model, HttpServletRequest request) {
		AssetStatement asset_statment = new AssetStatement();
		 asset_statment.setAsset_id(asset_id);
		 asset_statment.setMode("I");
		model.addAttribute("asset_statment", asset_statment);
		return "assets/add_statement";
	};
	
	@RequestMapping(value = "/update_statement/{asset_id}/{statement_name}", method = RequestMethod.GET)
	public String update_statement(@PathVariable String asset_id, @PathVariable String statement_name, @ModelAttribute AssetStatement statement, Model model, HttpServletRequest request) {
		AssetStatement asset_statment = new AssetStatement();
		try {
			asset_statment = asset_service.selectAssetStatement(asset_id, statement_name);
			asset_statment.setMode("U");
		} catch (Exception e) {
			log.error("Get asset statement exception : " + e.getMessage(), e);
		}
		model.addAttribute("asset_statment", asset_statment);
		return "assets/add_statement";
	};

	
	@RequestMapping(value = "/save_statement", method = RequestMethod.POST)
	public @ResponseBody JSONResult save_statement(@ModelAttribute AssetStatement asset_statement, HttpServletRequest request) {
		//등록
		if(asset_statement.getMode().equals("I")) { 
			try {
				//
				asset_service.deployAssetStatement(asset_statement);
				asset_service.insertAssetStatement(asset_statement);
				//
				return new JSONResult(JSONResult.SUCCESS, "데이터 생성 스테이트먼트를 생성하였습니다.");
			} catch (Exception e) {
				log.error("Save asset statement exception : " + e.getMessage(), e);
				return new JSONResult(JSONResult.ERROR, "데이터 생성 스테이트먼트를 생성할 수 없습니다. : 메세지 = [" + e.getMessage() + "]");
			}
	    //수정
		}else {
			try {
				//
				asset_service.undeployAssetStatement(asset_statement);
				asset_service.deployAssetStatement(asset_statement);
				asset_service.updateAssetStatement(asset_statement);
				//
				return new JSONResult(JSONResult.SUCCESS, "데이터 생성 스테이트먼트를 생성하였습니다.");
			} catch (Exception e) {
				log.error("Update asset statement exception : " + e.getMessage(), e);
				return new JSONResult(JSONResult.ERROR, "데이터 생성 스테이트먼트를 업데이트할 수 없습니다. : 메세지 = [" + e.getMessage() + "]");
			}
		}
	}

	@RequestMapping(value = "/delete_statement/{asset_id}/{statement_name}", method = RequestMethod.POST)
	public @ResponseBody JSONResult delete(@PathVariable String asset_id, @PathVariable String statement_name, Model model) throws Exception {
		//
		AssetStatement asset_statement = asset_service.selectAssetStatement(asset_id, statement_name);
		asset_service.deleteAssetStatement(asset_statement);
		asset_service.undeployAssetStatement(asset_statement);
		//
		return new JSONResult(Result.SUCCESS, "데이터 생성 스테이트먼트를 삭제하였습니다.");
	}
}