package plantpulse.server.mvc.api.v2;

import java.util.Map;
import java.util.concurrent.Callable;

import javax.servlet.http.HttpServletRequest;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.json.JSONArray;
import org.json.JSONObject;
import org.realdisplay.framework.util.StringUtils;
import org.springframework.beans.BeanUtils;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Controller;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.ResponseBody;

import plantpulse.cep.dao.AssetDAO;
import plantpulse.cep.dao.OPCDAO;
import plantpulse.cep.dao.SiteDAO;
import plantpulse.cep.dao.TagDAO;
import plantpulse.cep.engine.CEPEngineManager;
import plantpulse.cep.engine.config.ConfigurationManager;
import plantpulse.cep.engine.diagnostic.DiagnosticHandler;
import plantpulse.cep.engine.messaging.listener.http.HTTPTypeBaseListener;
import plantpulse.cep.engine.messaging.unmarshaller.DefaultTagMessageUnmarshaller;
import plantpulse.cep.service.ControlService;
import plantpulse.cep.service.TagService;
import plantpulse.cep.service.TokenService;
import plantpulse.cep.service.UserService;
import plantpulse.cep.service.client.StorageClient;
import plantpulse.cep.service.support.asset.AssetStatementResultService;
import plantpulse.domain.Asset;
import plantpulse.domain.OPC;
import plantpulse.domain.Site;
import plantpulse.domain.Tag;
import plantpulse.event.asset.AssetAggregation;
import plantpulse.event.asset.AssetContext;
import plantpulse.event.asset.AssetEvent;
import plantpulse.event.opc.Point;
import plantpulse.server.mvc.api.APIKeySecurity;
import plantpulse.server.mvc.api.SQLMessageConverter;

/**
 * APIController_V2
 * 
 * @author lsb
 *
 */
@Controller
@RequestMapping(value = "/api/v2")
public class APIController_V2 extends APIKeySecurity {

	private static final Log log = LogFactory.getLog(APIController_V2.class);
	
	private UserService  user_service = new UserService();
	private TokenService token_service = new TokenService();
	
	
	@Autowired
	private TagService tag_service;

	/**
	 * ping
	 * @param query
	 * @return
	 */
	@RequestMapping(value = "/ping", method = RequestMethod.GET, produces="application/json;charset=UTF-8")
	public @ResponseBody String ping(final @RequestBody String query) {
		try {
			return "SUCCESS : 플랜트펄스 API";
		} catch (Exception ex) {
			return "ERROR : " + ex.getMessage();
		}
	};
	
	
	/**
	 * API 사용을 위한 로그인 (_api_key를 반환)
	 * 
	 * <pre>
	 *  파라메터 
	 *  {
	 *  uesrname   : 'TAG_VIRTUAL_00000000',
	 *	password : 'virtual.opc.test.tag'
	 *	}
	 * </pre>
	 * @param query
	 * @return
	 */
	@RequestMapping(value = "/login", method = RequestMethod.POST, produces="application/json;charset=UTF-8")
	public @ResponseBody String login(final @RequestBody String query) {
		try {
			//
			JSONObject json = JSONObject.fromObject(query);
			//
			
			boolean is_logind = false;
			boolean is_token_login = false;
			if(json.containsKey("token") && StringUtils.isNotEmpty(json.getString("token"))) {
				is_token_login = true;
				is_logind = token_service.checkToken(json.getString("ip"), json.getString("token"));
			}else {
				is_token_login = false;
				is_logind = user_service.checkLogin(json.getString("userid"), json.getString("password"));
			};
			
			if(is_logind){
				return getApiKey();
			}else{
				return "ERROR : 로그인에 실패하였습니다.";
			}
		} catch (Exception ex) {
			log.debug("API Login error.", ex);
			return "ERROR : " + ex.getMessage();
		}
	};
	
	

	/**
	 * 플랜트펄스 환경 정보를 반환한다.
	 * 
	 * 메세지 서버 정보
	 * 데이터베이스 서버 정보 
	 * 
	 * @param query
	 * @return
	 */
	@RequestMapping(value = "/env", method = RequestMethod.POST, produces="application/json;charset=UTF-8")
	public @ResponseBody String env(final @RequestBody String query) {
		try {
			// 인증 확인
			auth(query);
			
			JSONObject json = JSONObject.fromObject(ConfigurationManager.getInstance().getServer_configuration());
			return json.toString();
			
		} catch (Exception ex) {
			log.debug("API Enviroment error.", ex);
			return "ERROR : " + ex.getMessage();
		}
	};
	
	
	/**
	 * HTTP를 통한 이벤트 전달
	 * 
	 * <pre>
	 *  파라메터 
	 *  {
	 *  id   : 'TAG_VIRTUAL_00000000',
	 *	name : 'virtual.opc.test.tag',
	 *	type : 'double',
	 *	group_name : 'group',
	 *	opc_id : 'OPC_01000',
	 *	timestamp : 123456789,
	 * 	value : '12.00',
	 * 	quality : 192,
	 * 	error_code : 0
	 *	}
	 * </pre>
	 * 
	 * @param query
	 * @return
	 */
	@RequestMapping(value = "/event", method = RequestMethod.POST, produces="application/json;charset=UTF-8")
	public @ResponseBody Callable<String> event(final @RequestBody String query) {

		return new Callable<String>() {
			@Override
			public String call() throws Exception {
				HTTPTypeBaseListener listener = new HTTPTypeBaseListener(new DefaultTagMessageUnmarshaller());
				listener.onEvent(query);
				//
				return "SUCCESS";
			}
		};
	};
	
	

	/**
	 * 사이트를 추가한다.
	 * 
	 * <pre>
	 * /vsite/add
	 *  파라메터 
	 *  {
	 *  _api_key  : 'ABCD',
	 *  site_id   : 'VOPC_00000',
	 *	site_name : 'MY_OPC',
	 *	}
	 * </pre>
	 * 
	 * @param query
	 * @return
	 */
	@RequestMapping(value = "/vsite/add", method = RequestMethod.POST, produces="application/json;charset=UTF-8")
	public @ResponseBody String vsiteAdd(final @RequestBody String query) {
		try {
			// 인증 확인
			auth(query);

			JSONObject json = JSONObject.fromObject(query);
			Site site = (Site) JSONObject.toBean(json, Site.class);
			SiteDAO dao = new SiteDAO();
			// 사이트 존재여부 체크
			if (dao.hasSite(site.getSite_id())) {
				throw new Exception("같은 ID의 SITE가 이미 존재합니다.");
			}
			;
			dao.insertVirtualSite(site);
			log.debug("API Site added : data=[" + json.toString() + "]");
			return "SUCCESS";
		} catch (Exception ex) {
			log.debug("API Site add error.", ex);
			return "ERROR : " + SQLMessageConverter.convert(ex.getMessage());
		}
	};
	
	
	@RequestMapping(value = "/vsite/update", method = RequestMethod.POST, produces="application/json;charset=UTF-8")
	public @ResponseBody String vsiteUpdate(final @RequestBody String query) {
		try {
			// 인증 확인
			auth(query);

			JSONObject json = JSONObject.fromObject(query);
			Site site = (Site) JSONObject.toBean(json, Site.class);
			SiteDAO dao = new SiteDAO();
			// OPC 존재여부 체크
			if (dao.hasSite(site.getSite_id())) {
				dao.updateVirtualSite(site); //업데이트
			}else{
				dao.insertVirtualSite(site); //등록
			}
			;
			log.debug("API Site updated : data=[" + json.toString() + "]");
			return "SUCCESS";
		} catch (Exception ex) {
			log.debug("API Site update error.", ex);
			return "ERROR : " + SQLMessageConverter.convert(ex.getMessage());
		}
	};

	
	
	/**
	 * 사이트를 삭제한다.
	 * 
	 * <pre>
	 *  파라메터 
	 *  {
	 *   _api_key   : 'ABCD',
	 *  opc_id   : 'VOPC_00000'
	 *	}
	 * </pre>
	 * 
	 * @param query
	 * @return
	 */
	@RequestMapping(value = "/vsite/delete", method = RequestMethod.POST, produces="application/json;charset=UTF-8")
	public @ResponseBody String vsiteDelete(final @RequestBody String query) {
		try {
			// 인증 확인
			auth(query);

			JSONObject json = JSONObject.fromObject(query);
			Site site = (Site) JSONObject.toBean(json, Site.class);
			SiteDAO dao = new SiteDAO();
			
			dao.deleteVirtualSite(site);
			log.debug("API Site deleted : data=[" + json.toString() + "]");
			return "SUCCESS";
		} catch (Exception ex) {
			log.debug("API Site delete error.", ex);
			return "ERROR : " + SQLMessageConverter.convert(ex.getMessage());
		}
	};
	

	/**
	 *  OPC를 추가한다.
	 * 
	 * <pre>
	 * /vopc/add
	 *  파라메터 
	 *  {
	 *  _api_key   : 'ABCD',
	 *  opc_id   : 'VOPC_00000',
	 *	opc_name : 'MY_OPC',
	 *  opc_server_ip : '127.0.0.1'
	 *	site_id  : 'SITE_00001'
	 *	}
	 * </pre>
	 * 
	 * @param query
	 * @return
	 */
	@RequestMapping(value = "/vopc/add", method = RequestMethod.POST, produces="application/json;charset=UTF-8")
	public @ResponseBody String vopcAdd(final @RequestBody String query) {
		try {
			// 인증 확인
			auth(query);

			JSONObject json = JSONObject.fromObject(query);
			OPC opc = (OPC) JSONObject.toBean(json, OPC.class);
			OPCDAO dao = new OPCDAO();
			// OPC 존재여부 체크
			if (dao.hasOPC(opc.getOpc_id())) {
				throw new Exception("같은 ID의 OPC가 이미 존재합니다.");
			}
			;
			dao.insertVituralOpc(opc);
			log.debug("API OPC added : data=[" + json.toString() + "]");
			return "SUCCESS";
		} catch (Exception ex) {
			log.debug("API OPC add error.", ex);
			return "ERROR : " + SQLMessageConverter.convert(ex.getMessage());
		}
	};
	
	
	@RequestMapping(value = "/vopc/update", method = RequestMethod.POST, produces="application/json;charset=UTF-8")
	public @ResponseBody String vopcUpdate(final @RequestBody String query) {
		try {
			// 인증 확인
			auth(query);

			JSONObject json = JSONObject.fromObject(query);
			OPC opc = (OPC) JSONObject.toBean(json, OPC.class);
			OPCDAO dao = new OPCDAO();
			// OPC 존재여부 체크
			if (dao.hasOPC(opc.getOpc_id())) {
				dao.updateVituralOpc(opc); //업데이트
			}else{
				dao.insertVituralOpc(opc); //등록
			}
			;
			log.debug("API OPC updated : data=[" + json.toString() + "]");
			return "SUCCESS";
		} catch (Exception ex) {
			log.debug("API OPC update error.", ex);
			return "ERROR : " + SQLMessageConverter.convert(ex.getMessage());
		}
	};

	
	
	/**
	 *  OPC를 삭제한다.
	 * 
	 * <pre>
	 *  파라메터 
	 *  {
	 *   _api_key   : 'ABCD',
	 *  opc_id   : 'VOPC_00000'
	 *	}
	 * </pre>
	 * 
	 * @param query
	 * @return
	 */
	@RequestMapping(value = "/vopc/delete", method = RequestMethod.POST, produces="application/json;charset=UTF-8")
	public @ResponseBody String vopcDelete(final @RequestBody String query) {
		try {
			// 인증 확인
			auth(query);

			JSONObject json = JSONObject.fromObject(query);
			OPC opc = (OPC) JSONObject.toBean(json, OPC.class);
			OPCDAO dao = new OPCDAO();
			dao.deleteVituralOpc(opc);
			log.debug("API OPC deleted : data=[" + json.toString() + "]");
			return "SUCCESS";
		} catch (Exception ex) {
			log.debug("API OPC delete error.", ex);
			return "ERROR : " + SQLMessageConverter.convert(ex.getMessage());
		}
	};
	
	
	/**
	 *  asset 를 추가한다.
	 * 
	 * <pre>
	 * /vopc/add
	 *  파라메터 
	 *  {
	 *  _api_key   : 'ABCD',
	 *  opc_id   : 'VOPC_00000',
	 *	opc_name : 'MY_OPC',
	 *  opc_server_ip : '127.0.0.1'
	 *	site_id  : 'SITE_00001'
	 *	}
	 * </pre>
	 * 
	 * @param query
	 * @return
	 */
	@RequestMapping(value = "/vasset/add", method = RequestMethod.POST, produces="application/json;charset=UTF-8")
	public @ResponseBody String vassetAdd(final @RequestBody String query) {
		try {
			// 인증 확인
			auth(query);

			JSONObject json = JSONObject.fromObject(query);
			Asset asset = (Asset) JSONObject.toBean(json, Asset.class);
			AssetDAO dao = new AssetDAO();
			// 에셋 존재여부 체크
			if (dao.hasAsset(asset.getAsset_id())) {
				throw new Exception("같은 ID의 에셋이 이미 존재합니다.");
			}
			;
			dao.insertVAsset(asset);
			dao.updateAssetWidhEquipment(asset);
			
			log.debug("API Asset added : data=[" + json.toString() + "]");
			return "SUCCESS";
		} catch (Exception ex) {
			log.debug("API Asset add error.", ex);
			return "ERROR : " + SQLMessageConverter.convert(ex.getMessage());
		}
	};
	
	
	@RequestMapping(value = "/vasset/update", method = RequestMethod.POST, produces="application/json;charset=UTF-8")
	public @ResponseBody String vassetUpdate(final @RequestBody String query) {
		try {
			// 인증 확인
			auth(query);

			JSONObject json = JSONObject.fromObject(query);
			Asset asset = (Asset) JSONObject.toBean(json, Asset.class);
			AssetDAO dao = new AssetDAO();
			// OPC 존재여부 체크
			if (dao.hasAsset(asset.getAsset_id())) {
				dao.updateAsset(asset); //업데이트
			}else{
				dao.insertVAsset(asset); //등록
			};
			
			dao.updateAssetWidhEquipment(asset);
			
			log.debug("API Asset updated : data=[" + json.toString() + "]");
			return "SUCCESS";
		} catch (Exception ex) {
			log.debug("API Asset update error.", ex);
			return "ERROR : " + SQLMessageConverter.convert(ex.getMessage());
		}
	};

	
	/**
	 *  OPC를 삭제한다.
	 * 
	 * <pre>
	 *  파라메터 
	 *  {
	 *   _api_key   : 'ABCD',
	 *  opc_id   : 'VOPC_00000'
	 *	}
	 * </pre>
	 * 
	 * @param query
	 * @return
	 */
	@RequestMapping(value = "/vasset/delete", method = RequestMethod.POST, produces="application/json;charset=UTF-8")
	public @ResponseBody String vassetDelete(final @RequestBody String query) {
		try {
			// 인증 확인
			auth(query);

			JSONObject json = JSONObject.fromObject(query);
			Asset asset = (Asset) JSONObject.toBean(json, Asset.class);
			AssetDAO dao = new AssetDAO();
			dao.deleteAsset(asset);
			log.debug("API Asset deleted : data=[" + json.toString() + "]");
			return "SUCCESS";
		} catch (Exception ex) {
			log.debug("API Asset delete error.", ex);
			return "ERROR : " + SQLMessageConverter.convert(ex.getMessage());
		}
	};
	
	
	/**
	 * 
	 * @param query
	 * @return
	 */
	@RequestMapping(value = "/vopc/tag/get", method = RequestMethod.POST, produces="application/json;charset=UTF-8", consumes="application/json;charset=UTF-8")
	public @ResponseBody String vtagGet(final @RequestBody String query) {
		try {

			// 인증 확인
			auth(query);

			JSONObject json = JSONObject.fromObject(query);
			Tag tag = (Tag) JSONObject.toBean(json, Tag.class);

			;
			//
			TagDAO dao = new TagDAO();
			Tag r_tag = dao.selectTag(tag.getTag_id());
			
			//
			
			String result = (JSONObject.fromObject(r_tag)).toString();
			
			log.debug("API Tag data get : data=[" + result + "]");
			return result;
		} catch (Exception ex) {
			log.debug("API Point add error.", ex);
			return "ERROR : " + SQLMessageConverter.convert(ex.getMessage());
		}
	};

	/**
	 *  태그를 추가한다.
	 * 
	 * <pre>
	 *  /vopc/tag/add
	 *  파라메터 
	 *  {
	 *   _api_key   : 'ABCD',
	 *  opc_id     : 'OPC_01000',
	 *  tag_id     : 'VTAG_000000',
	 *	tag_name   : 'virtual.opc.test.tag',
	 *	group_name : 'group',
	 *	tag_source : 'OPC',
	 *	java_type  : 'Integer',
	 *  alias_name : 'ali',
	 *  unit : '.C',
	 *  description: 'Test Point'
	 *	}
	 * </pre>
	 * 
	 * @param query
	 * @return
	 */
	@RequestMapping(value = "/vopc/tag/add", method = RequestMethod.POST, produces="application/json;charset=UTF-8", consumes="application/json;charset=UTF-8")
	public @ResponseBody String vtagAdd(final @RequestBody String query) {
		try {

			// 인증 확인
			auth(query);

			JSONObject json = JSONObject.fromObject(query);
			Tag tag = (Tag) JSONObject.toBean(json, Tag.class);

			// 태그 네이밍 룰 체크
			if (!tag.getTag_id().startsWith("TAG_") && !tag.getTag_id().startsWith("VTAG_")) {
				throw new Exception("태그ID는 [TAG_#####] 또는 [VTAG_#####]로 시작해야 합니다.");
			}

			// OPC 존재여부 체크
			if (!(new OPCDAO()).hasOPC(tag.getOpc_id())) {
				throw new Exception("등록하려는 [OPC_ID:" + tag.getOpc_id() + "]가 존재하지 않습니다.");
			}
			;
			
			// 에셋 존재여부 체크
			if(StringUtils.isNotEmpty(tag.getLinked_asset_id())) {
				if (!(new AssetDAO()).hasAsset(tag.getLinked_asset_id())) {
					throw new Exception("등록하려는 [ASSET_ID:" + tag.getLinked_asset_id() + "]가 존재하지 않습니다.");
				}
			}
			;
			
			// TAG 존재여부 체크
			TagDAO dao = new TagDAO();
			if (dao.selectTag(tag.getTag_id()) != null) {
				throw new Exception("같은 ID의 TAG가 이미 존재합니다.");
			}
			;
			//
			dao.insertTag(tag);
			
			//
			tag_service.refreshTag(tag);
			
			
			log.debug("API Tag data added : data=[" + json.toString() + "]");
			return "SUCCESS";
		} catch (Exception ex) {
			log.debug("API Point add error.", ex);
			return "ERROR : " + SQLMessageConverter.convert(ex.getMessage());
		}
	};
	
	
	@RequestMapping(value = "/vopc/tag/update", method = RequestMethod.POST, produces="application/json;charset=UTF-8", consumes="application/json;charset=UTF-8")
	public @ResponseBody String vtagUpdate(final @RequestBody String query) {
		try {

			// 인증 확인
			auth(query);
			
			JSONObject json = JSONObject.fromObject(query);
			
			Tag tag = (Tag) JSONObject.toBean(json, Tag.class);

			// 태그 네이밍 룰 체크
			if (!tag.getTag_id().startsWith("TAG_") && !tag.getTag_id().startsWith("VTAG_")) {
				throw new Exception("태그ID는 [TAG_#####] 또는 [VTAG_#####]로 시작해야 합니다.");
			}

			// OPC 존재여부 체크
			if (!(new OPCDAO()).hasOPC(tag.getOpc_id())) {
				throw new Exception("등록하려는 [OPC_ID:" + tag.getOpc_id() + "]가 존재하지 않습니다.");
			}
			;
			
			// 에셋 존재여부 체크
			if(StringUtils.isNotEmpty(tag.getLinked_asset_id())) {
				if (!(new AssetDAO()).hasAsset(tag.getLinked_asset_id())) {
					throw new Exception("등록하려는 [ASSET_ID:" + tag.getLinked_asset_id() + "]가 존재하지 않습니다.");
				}
			}
			;
			
			// TAG 존재여부 체크
			TagDAO dao = new TagDAO();
			Tag i_tag = dao.selectTag(tag.getTag_id());
			
			if (i_tag != null) {
				BeanUtils.copyProperties(tag, i_tag);
				dao.updateTag(i_tag); //업데이트
			}else{
				dao.insertTag(tag); //입력
			};
			
			//
			tag_service.refreshTag(tag);
		
			//
			log.debug("API Tag data updated : data=[" + json.toString() + "]");
			return "SUCCESS";
		} catch (Exception ex) {
			log.debug("API Point update error.", ex);
			return "ERROR : " + SQLMessageConverter.convert(ex.getMessage());
		}
	};
	

	/**
	 *  태그를 삭제한다.
	 * 
	 * <pre>
	 *  파라메터 
	 *  {
	 *   _api_key   : 'ABCD',
	 *  tag_id     : 'VTAG_000000'
	 *	}
	 * </pre>
	 * 
	 * @param query
	 * @return
	 */
	@RequestMapping(value = "/vopc/tag/delete", method = RequestMethod.POST, produces="application/json;charset=UTF-8")
	public @ResponseBody String vtagDelete(final @RequestBody String query) {
		try {
			// 인증 확인
			auth(query);

			JSONObject json = JSONObject.fromObject(query);
			Tag tag = (Tag) JSONObject.toBean(json, Tag.class);
			TagDAO dao = new TagDAO();
			dao.deleteVituralTag(tag);
			log.debug("API Tag data deleted : data=[" + json.toString() + "]");
			return "SUCCESS";
		} catch (Exception ex) {
			log.debug("API Tag delete error.", ex);
			return "ERROR : " + SQLMessageConverter.convert(ex.getMessage());
		}
	};

	/**
	 *  태그의 데이터를 입력한다.
	 * 
	 * <pre>
	 *  /vopc/tag/data/insert
	 *  파라메터 
	 *  {
	 *   _api_key   : 'ABCD',
	 *  id   : 'VTAG_000000',
	 *	name : 'virtual.opc.test.tag',
	 *	type : 'int',
	 *	group_name : 'group',
	 *	opc_id : 'OPC_01000',
	 *	timestamp : 123456789,
	 * 	value : '12.00',
	 * 	quality : 192,
	 * 	error_code : 0
	 *	}
	 * </pre>
	 * 
	 * @param query
	 * @return
	 */
	@RequestMapping(value = "/vopc/tag/point/insert", method = RequestMethod.POST, produces="application/json;charset=UTF-8")
	public @ResponseBody Callable<String> vtagPointInsert(final @RequestBody String query) {
		return new Callable<String>() {
			@Override
			public String call() throws Exception {
				try {
					// 인증 확인
					auth(query);

					JSONObject json = JSONObject.fromObject(query);
					// MQ
					JSONObject data = new JSONObject();
					data.put("_event_type",  "plantpulse.event.opc.Point");
					data.put("_event_class", "JSON");
					data.put("_event_data",   json);
					//
					HTTPTypeBaseListener listener = new HTTPTypeBaseListener(new DefaultTagMessageUnmarshaller());
					listener.onEvent(data.toString());
					//
					log.debug("API Point data inserted : data=[" + json.toString() + "]");
					//
					return "SUCCESS";
				} catch (Exception ex) {
					log.debug("API Point data insert error.", ex);
					return "ERROR : " + ex.getMessage();
				}
			}
		};
	};
	
	
	@RequestMapping(value = "/vopc/tag/point/read", method = RequestMethod.POST, produces="application/json;charset=UTF-8")
	public @ResponseBody String vtagPointRead(final @RequestBody String query) {
		try {
			// 인증 확인
			auth(query);
            //
			JSONObject json = JSONObject.fromObject(query);
			Tag tag = (Tag) JSONObject.toBean(json, Tag.class);
            //
			StorageClient client = new StorageClient();
			Map<String,Object> map = client.forSelect().selectPointLast(tag);
			//
			log.debug("API Point data read : data=[" + json.toString() + "]");
			return  JSONObject.fromObject(map).toString();
		} catch (Exception ex) {
			log.debug("API Point read error.", ex);
			return "ERROR : " + SQLMessageConverter.convert(ex.getMessage());
		}
	};
	
	@RequestMapping(value = "/vopc/tag/point/write", method = RequestMethod.POST, produces="application/json;charset=UTF-8")
	public @ResponseBody String vtagPointWrite(final @RequestBody String query) {
		try {
			// 인증 확인
			auth(query);

			JSONObject json = JSONObject.fromObject(query);
			Point point = (Point) JSONObject.toBean(json, Point.class);
			
			String tag_id = point.getTag_id();
			String value  = point.getValue();
            //
			ControlService service = new ControlService();
			service.setValue(tag_id, value);
			
			log.debug("API Point data writed : data=[" + json.toString() + "]");
			return "SUCCESS";
		} catch (Exception ex) {
			log.debug("API Point write error.", ex);
			return "ERROR : " + SQLMessageConverter.convert(ex.getMessage());
		}
	};

	/**
	 *  태그의 데이터를 배치 입력한다.
	 * 
	 * <pre>
	 *  /vopc/tag/data/batch
	 *  파라메터 
	 *  [{
	 *   _api_key   : 'ABCD',
	 *  id   : 'VTAG_000000',
	 *	name : 'virtual.opc.test.tag',
	 *	type : 'int',
	 *	group_name : 'group',
	 *	opc_id : 'OPC_01000',
	 *	timestamp : 123456789,
	 * 	value : '12.00',
	 * 	quality : 192,
	 * 	error_code : 0
	 *	}, .... ]
	 * </pre>
	 * 
	 * @param query
	 * @return
	 */
	@RequestMapping(value = "/vopc/tag/point/batch", method = RequestMethod.POST, produces="application/json;charset=UTF-8")
	public @ResponseBody Callable<String> vtagPointInsertBatch(final @RequestBody String query) {
		return new Callable<String>() {
			@Override
			public String call() throws Exception {
				try {

					JSONArray json_array = JSONArray.fromObject(query);

					for (int i = 0; i < json_array.size(); i++) {

						JSONObject json = json_array.getJSONObject(i);
						// 인증 확인
						auth(json);

						// MQ
						JSONObject data = new JSONObject();
						data.put("_event_type", "plantpulse.event.opc.Point");
						data.put("_event_class", "Class");
						data.put("_event_data", json);
						//
						HTTPTypeBaseListener listener = new HTTPTypeBaseListener(new DefaultTagMessageUnmarshaller());
						listener.onEvent(data.toString());
					}
					//
					log.debug("API Point data batched : batch_size=[" + json_array.size() + "]");
					//
					return "SUCCESS";
				} catch (Exception ex) {
					log.debug("API Point data batch error.", ex);
					return "ERROR : " + ex.getMessage();
				}
			}
		};
	};

	/**
	 * diagnostic
	 * @param query
	 * @return
	 */
	@RequestMapping(value = "/diagnostic", method = RequestMethod.POST, produces="application/json;charset=UTF-8")
	public @ResponseBody Callable<String> diagnostic(final HttpServletRequest request) {

		return new Callable<String>() {
			@Override
			public String call() throws Exception {
				
				//
				DiagnosticHandler handler = new DiagnosticHandler();
				handler.handleFromRequest(request);
				//
				return "SUCCESS";
			}
		};
	};
	
	
	
	/**
	 * 에셋 이벤트를 등록한다.
	 * 
	 * <pre>
	 *  파라메터 
	 *  {
	 *   _api_key   : 'ABCD',
	 *  opc_id   : 'VOPC_00000'
	 *	}
	 * </pre>
	 * 
	 * @param query
	 * @return
	 */
	@RequestMapping(value = "/vasset/event/insert", method = RequestMethod.POST, produces="application/json;charset=UTF-8")
	public @ResponseBody String assetEventInsert(final @RequestBody String query) {
		try {
			// 인증 확인
			auth(query);

			JSONObject json = JSONObject.fromObject(query);
			AssetEvent event = (AssetEvent) JSONObject.toBean(json, AssetEvent.class);
			AssetStatementResultService service = new AssetStatementResultService();
			service.insertAssetEvent(event);
			log.debug("API Asset Event inserted : data=[" + json.toString() + "]");
			return "SUCCESS";
		} catch (Exception ex) {
			log.debug("API Asset Event insert error.", ex);
			return "ERROR : " + SQLMessageConverter.convert(ex.getMessage());
		}
	};
	
	
	/**
	 * 에셋 상태를 등록한다.
	 * 
	 * <pre>
	 *  파라메터 
	 *  {
	 *   _api_key   : 'ABCD',
	 *  opc_id   : 'VOPC_00000'
	 *	}
	 * </pre>
	 * 
	 * @param query
	 * @return
	 */
	@RequestMapping(value = "/vasset/context/insert", method = RequestMethod.POST, produces="application/json;charset=UTF-8")
	public @ResponseBody String assetContextInsert(final @RequestBody String query) {
		try {
			// 인증 확인
			auth(query);

			JSONObject json = JSONObject.fromObject(query);
			AssetContext context = (AssetContext) JSONObject.toBean(json, AssetContext.class);
			AssetStatementResultService service = new AssetStatementResultService();
			service.insertAssetContext(context);
			log.debug("API Asset Context inserted : data=[" + json.toString() + "]");
			return "SUCCESS";
		} catch (Exception ex) {
			log.debug("API Asset Context insert error.", ex);
			return "ERROR : " + SQLMessageConverter.convert(ex.getMessage());
		}
	};
	
	
	/**
	 * 에셋 상태를 등록한다.
	 * 
	 * <pre>
	 *  파라메터 
	 *  {
	 *   _api_key   : 'ABCD',
	 *  opc_id   : 'VOPC_00000'
	 *	}
	 * </pre>
	 * 
	 * @param query
	 * @return
	 */
	@RequestMapping(value = "/vasset/aggregation/insert", method = RequestMethod.POST, produces="application/json;charset=UTF-8")
	public @ResponseBody String assetAggregationInsert(final @RequestBody String query) {
		try {
			// 인증 확인
			auth(query);

			JSONObject json = JSONObject.fromObject(query);
			AssetAggregation agg = (AssetAggregation) JSONObject.toBean(json, AssetAggregation.class);
			AssetStatementResultService service = new AssetStatementResultService();
			service.insertAssetAggregation(agg);
			log.debug("API Asset Aggregation inserted : data=[" + json.toString() + "]");
			return "SUCCESS";
		} catch (Exception ex) {
			log.debug("API Asset Aggregation insert error.", ex);
			return "ERROR : " + SQLMessageConverter.convert(ex.getMessage());
		}
	};
	
	
	/**
	 * 
	 * @param request
	 * @return
	 */
	@RequestMapping(value = "/site/model/update", method = RequestMethod.POST, produces="application/json;charset=UTF-8")
	public @ResponseBody Callable<String> updateSiteModel(final HttpServletRequest request) {

		return new Callable<String>() {
			@Override
			public String call() throws Exception {
				CEPEngineManager.getInstance().getSite_meta_model_updater().update();
				//
				return "SUCCESS";
			}
		};
	};

}