package plantpulse.server.mvc.api.v1;

import java.util.concurrent.Callable;

import javax.servlet.http.HttpServletRequest;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.json.JSONArray;
import org.json.JSONObject;
import org.springframework.stereotype.Controller;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.ResponseBody;

import plantpulse.cep.dao.OPCDAO;
import plantpulse.cep.dao.TagDAO;
import plantpulse.cep.engine.config.ConfigurationManager;
import plantpulse.cep.engine.diagnostic.DiagnosticHandler;
import plantpulse.cep.engine.messaging.listener.http.HTTPTypeBaseListener;
import plantpulse.cep.engine.messaging.unmarshaller.DefaultTagMessageUnmarshaller;
import plantpulse.cep.service.UserService;
import plantpulse.domain.OPC;
import plantpulse.domain.Tag;
import plantpulse.server.mvc.api.APIKeySecurity;
import plantpulse.server.mvc.api.SQLMessageConverter;

@Controller
@RequestMapping(value = "/api/v1")
public class APIController_V1 extends APIKeySecurity {

	private static final Log log = LogFactory.getLog(APIController_V1.class);
	
	private UserService service = new UserService();

	
	/**
	 * API 사용을 위한 로그인 (_api_key를 반환)
	 * 
	 * <pre>
	 *  파라메터 
	 *  {
	 *  uesrname   : 'TAG_VIRTUAL_00000000',
	 *	passwod : 'virtual.opc.test.tag'
	 *	}
	 * </pre>
	 * @param query
	 * @return
	 */
	@RequestMapping(value = "/login", method = RequestMethod.POST)
	public @ResponseBody String login(final @RequestBody String query) {
		try {
			//
			JSONObject json = JSONObject.fromObject(query);
			//
			boolean is_logind = service.checkLogin(json.getString("userid"), json.getString("password"));
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
	@RequestMapping(value = "/env", method = RequestMethod.POST)
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
	 * 가상 OPC를 추가한다.
	 * 
	 * <pre>
	 * /api/vopc/add
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
	@RequestMapping(value = "/vopc/add", method = RequestMethod.POST)
	public @ResponseBody String vopcAdd(final @RequestBody String query) {
		try {
			// 인증 확인
			auth(query);

			JSONObject json = JSONObject.fromObject(query);
			OPC opc = (OPC) JSONObject.toBean(json, OPC.class);
			OPCDAO dao = new OPCDAO();
			// OPC 존재여부 체크
			if (dao.hasOPC(opc.getOpc_id())) {
				throw new Exception("[OPC_ID]에 해당하는 값이 이미 존재합니다.");
			}
			;
			dao.insertVituralOpc(opc);
			log.info("API Virtual OPC added : data=[" + json.toString() + "]");
			return "SUCCESS";
		} catch (Exception ex) {
			log.debug("API OPC add error.", ex);
			return "ERROR : " + SQLMessageConverter.convert(ex.getMessage());
		}
	};

	/**
	 * 가상 OPC를 삭제한다.
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
	@RequestMapping(value = "/vopc/delete", method = RequestMethod.POST)
	public @ResponseBody String vopcDelete(final @RequestBody String query) {
		try {
			// 인증 확인
			auth(query);

			JSONObject json = JSONObject.fromObject(query);
			OPC opc = (OPC) JSONObject.toBean(json, OPC.class);
			OPCDAO dao = new OPCDAO();
			dao.deleteVituralOpc(opc);
			log.info("API Virtual OPC deleted : data=[" + json.toString() + "]");
			return "SUCCESS";
		} catch (Exception ex) {
			log.debug("API OPC delete error.", ex);
			return "ERROR : " + SQLMessageConverter.convert(ex.getMessage());
		}
	};

	/**
	 * 가상 태그를 추가한다.
	 * 
	 * <pre>
	 *  /api/vopc/tag/add
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
	@RequestMapping(value = "/vopc/tag/add", method = RequestMethod.POST)
	public @ResponseBody String vtagAdd(final @RequestBody String query) {
		try {

			// 인증 확인
			auth(query);

			JSONObject json = JSONObject.fromObject(query);
			Tag tag = (Tag) JSONObject.toBean(json, Tag.class);
			OPCDAO opc_dao = new OPCDAO();
			TagDAO tag_dao = new TagDAO();

			// 태그 네이밍 룰 체크
			if (!tag.getTag_id().startsWith("VTAG_")) {
				throw new Exception("태그 아이디는 반드시 [VTAG_#####] 형식으로 입력되어야 합니다. EX) VTAG_12345 ");
			}

			// OPC 존재여부 체크
			if (!opc_dao.hasOPC(tag.getOpc_id())) {
				throw new Exception("태그가 등록될 [OPC_ID]가 없습니다.");
			}
			;
			
			// TAG 존재여부 체크
			if (tag_dao.selectTag(tag.getTag_id()) != null) {
				throw new Exception("이미 등록된 [TAG_ID]가 존재합니다.");
			}
			;
			//
			tag_dao.insertVituralTag(tag);
			log.info("API Virtual Tag data added : data=[" + json.toString() + "]");
			return "SUCCESS";
		} catch (Exception ex) {
			log.debug("API Point add error.", ex);
			return "ERROR : " + SQLMessageConverter.convert(ex.getMessage());
		}
	};

	/**
	 * 가상 태그를 삭제한다.
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
	@RequestMapping(value = "/vopc/tag/delete", method = RequestMethod.POST)
	public @ResponseBody String vtagDelete(final @RequestBody String query) {
		try {
			// 인증 확인
			auth(query);

			JSONObject json = JSONObject.fromObject(query);
			Tag tag = (Tag) JSONObject.toBean(json, Tag.class);
			TagDAO dao = new TagDAO();
			dao.deleteVituralTag(tag);
			log.info("API Virtual Tag data deleted : data=[" + json.toString() + "]");
			return "SUCCESS";
		} catch (Exception ex) {
			log.debug("API Tag delete error.", ex);
			return "ERROR : " + SQLMessageConverter.convert(ex.getMessage());
		}
	};

	/**
	 * 가상 태그의 데이터를 입력한다.
	 * 
	 * <pre>
	 *  /api/vopc/tag/data/insert
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
	@RequestMapping(value = "/vopc/tag/point/insert", method = RequestMethod.POST)
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
					log.debug("API Virtual Point data inserted : data=[" + json.toString() + "]");
					//
					return "SUCCESS";
				} catch (Exception ex) {
					log.debug("API Point data insert error.", ex);
					return "ERROR : " + ex.getMessage();
				}
			}
		};
	};

	/**
	 * 가상 태그의 데이터를 배치 입력한다.
	 * 
	 * <pre>
	 *  /api/vopc/tag/data/batch
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
	@RequestMapping(value = "/vopc/tag/point/batch", method = RequestMethod.POST)
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
					log.debug("API Virtual Point data batched : batch_size=[" + json_array.size() + "]");
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
	@RequestMapping(value = "/diagnostic", method = RequestMethod.POST)
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
	
	
}


