package plantpulse.cep.service;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.json.JSONArray;
import org.json.JSONObject;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import plantpulse.cep.dao.OPCDAO;
import plantpulse.cep.dao.SiteDAO;
import plantpulse.cep.dao.TagDAO;
import plantpulse.cep.service.support.security.SecurityTools;
import plantpulse.domain.OPC;
import plantpulse.domain.Site;
import plantpulse.domain.Tag;
import plantpulse.server.mvc.util.Constants;

@Service
public class OPCService {

	private static final Log log = LogFactory.getLog(OPCService.class);

	@Autowired
	private OPCDAO opc_dao;

	@Autowired
	private SiteDAO site_dao;

	@Autowired
	private TagDAO tag_dao;

	@Autowired
	private RestService rest_service;

	public List<OPC> selectOpcs(OPC opc) {
		List<OPC> result = new ArrayList<OPC>();
		try {
			List<OPC> list = opc_dao.selectOpcs(opc);
			for (int i = 0; list != null && i < list.size(); i++) {
				OPC one = list.get(i);
				if (SecurityTools.hasPermission(one.getId())) {
					result.add(one);
				}
			}
			return result;
		} catch (Exception e) {
			log.warn("Can not read OPC Tree data." + e.getMessage(), e);
			return null;
		}
	}

	public List<OPC> opcListForAsset(OPC opc) {
		try {
			return opc_dao.selectOpcsForAsset(opc);
		} catch (Exception e) {
			log.warn("Can not read OPC Tree data." + e.getMessage(), e);
			return null;
		}
	}


	public int selectOpcCount() {
		try {
			return opc_dao.selectOpcCount();
		} catch (Exception e) {
			log.warn("Can not read OPC count." + e.getMessage(), e);
			return 0;
		}
	};

	public List<OPC> selectOpcList() {
		try {
			return opc_dao.selectOpcList();
		} catch (Exception e) {
			log.warn("Can not read OPC list." + e.getMessage(), e);
			return null;
		}
	};

	public List<OPC> selectOpcListByType(String opc_type) {
		try {
			return opc_dao.selectOpcListByType(opc_type);
		} catch (Exception e) {
			log.warn("Can not read OPC list." + e.getMessage(), e);
			return null;
		}
	};

	public OPC selectOpcInfo(OPC opc) {
		try {
			return opc_dao.selectOpcInfo(opc);
		} catch (Exception e) {
			log.warn("Can not read OPC data." + e.getMessage(), e);
			return null;
		}
	}

	public OPC selectOpcInfo(String opc_id) {
		try {
			OPC opc = new OPC();
			opc.setOpc_id(opc_id);
			return opc_dao.selectOpcInfo(opc);
		} catch (Exception e) {
			log.warn("Can not read OPC data." + e.getMessage(), e);
			return null;
		}
	}

	/**
	 * OPC의 CLS ID 또는 PROGRAM ID 중복 체크
	 *
	 * @param json
	 * @return true : 중복, false : 중복아님
	 */
	public boolean duplicateCheckOpc(Map<String, Object> json) throws Exception {
		return opc_dao.duplicateCheckOpc(convertJsonToOPC(json));
	}

	public String insertOpc(Map<String, Object> json) throws Exception {
		return opc_dao.insertOpcAndTags(convertJsonToOPC(json), convertJsonToTagList(json));
	}

	/**
	 * OPC 서버의 테그를 연결한다.
	 *
	 * @param json
	 * @throws Exception
	 */
	public void updateOpc(Map<String, Object> json) throws Exception {
		//

		List<Tag> insertTagList = new ArrayList<Tag>();

		OPC opc = convertJsonToOPC(json);
		List<Tag> tagList = convertJsonToTagList(json);

		Tag tmpTag = new Tag();
		tmpTag.setOpc_id(opc.getOpc_id());
		List<Tag> currTagList = tag_dao.selectTagList(tmpTag, Constants.TYPE_OPC);

		for (Tag tag : tagList) {
			//
			boolean isExist = false;
			for (Tag currTag : currTagList) { // Point Name 중복체크
				if (currTag.getTag_name().equals(tag.getTag_name())) {
					isExist = true;
					break;
				}
			}
			if (isExist == false)
				insertTagList.add(tag);
		}
		log.debug(insertTagList.toString());

		opc_dao.updateOpcAndTags(opc, insertTagList);
	}

	public void opcStart(Map<String, Object> json) throws Exception {
		//
		OPC opc = convertJsonToOPC(json);

		//
		String path = "http://" + opc.getOpc_agent_ip() + ":" + String.valueOf(opc.getOpc_agent_port());

		Tag tag = new Tag();
		tag.setOpc_id(opc.getOpc_id());

		Site site = site_dao.selectSiteInfo(opc.getSite_id());
		List<Tag> tagList = tag_dao.selectTagList(tag, Constants.TYPE_OPC);


		List<JSONObject> list = new ArrayList<>();

		JSONObject tagJson;
		for(Tag t : tagList) {
			tagJson = new JSONObject();
			tagJson.put("tag_id", t.getTag_id());
			tagJson.put("opc_id", t.getOpc_id());
			tagJson.put("site_id", t.getSite_id());
			tagJson.put("tag_name", t.getTag_name());
			tagJson.put("java_type", t.getJava_type());
			list.add(tagJson);
		}

		JSONObject jsonObject = new JSONObject();
		jsonObject.put("opc", opc);
		jsonObject.put("site", site);
		jsonObject.put("tags", tagList);

		Map<String, Object> map = new HashMap<String, Object>();
		map.put("jsonString", jsonObject.toString());
		map.put("path", path);

		String isSuccess = rest_service.startOPC(map);
		if (!"SUCCESS".equals(isSuccess))
			throw new Exception("OPC Agent Start Failed.");
	}

	public void opcRestart(Map<String, Object> json) throws Exception {
		//
		OPC opc = convertJsonToOPC(json);
		//
		String path = "http://" + opc.getOpc_agent_ip() + ":" + String.valueOf(opc.getOpc_agent_port());
		Tag tag = new Tag();
		tag.setOpc_id(opc.getOpc_id());

		Site site = site_dao.selectSiteInfo(opc.getSite_id());
		List<Tag> tagList = tag_dao.selectTagList(tag, Constants.TYPE_OPC);


		List<JSONObject> list = new ArrayList<>();

		JSONObject tagJson;
		for(Tag t : tagList) {
			tagJson = new JSONObject();
			tagJson.put("tag_id", t.getTag_id());
			tagJson.put("opc_id", t.getOpc_id());
			tagJson.put("site_id", t.getSite_id());
			tagJson.put("tag_name", t.getTag_name());
			tagJson.put("java_type", t.getJava_type());
			list.add(tagJson);
		}

		JSONObject jsonObject = new JSONObject();
		jsonObject.put("opc", opc);
		jsonObject.put("site", site);
		jsonObject.put("tags", tagList);

		Map<String, Object> map = new HashMap<String, Object>();
		map.put("jsonString", jsonObject.toString());
		map.put("path", path);

		String isSuccess = rest_service.restartOPC(map);
		if (!"SUCCESS".equals(isSuccess))
			throw new Exception("OPC Agent Restart Failed.");
	}

	public void deleteOpc(OPC opc) throws Exception {
		opc_dao.deleteOpcAndTags(opc);
	}

	public OPC convertJsonToOPC(Map<String, Object> json) {
		//
		JSONObject jsonObject = JSONObject.fromObject(json.get("opc"));
		OPC opc = (OPC) JSONObject.toBean(jsonObject, OPC.class);

		return opc;
	}

	public List<Tag> convertJsonToTagList(Map<String, Object> json) {
		//
		List<Tag> tagList = new ArrayList<Tag>();
		JSONArray jsonArray = JSONArray.fromObject(json.get("tagList"));

		for (int i = 0; jsonArray != null && i < jsonArray.size(); i++) { //
			JSONObject obj = jsonArray.getJSONObject(i);
			Tag tag = (Tag) JSONObject.toBean(obj, Tag.class);
			tagList.add(tag);
		}
		log.debug("tagList : " + tagList.toString());

		return tagList;
	}

}
