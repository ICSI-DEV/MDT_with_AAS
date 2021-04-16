package plantpulse.cep.service;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.apache.commons.lang.StringUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.json.JSONArray;
import org.json.JSONObject;
import org.springframework.stereotype.Service;

import plantpulse.domain.OPC;
import plantpulse.domain.Tag;
import plantpulse.domain.TagDataTableObject;
import plantpulse.server.mvc.util.RestUtils;

@Service
public class RestService {

	private static final Log log = LogFactory.getLog(RestService.class);

	/**
	 * OPC Agent에 연결여부를 질의한다.
	 * 
	 * @param opc
	 * @return
	 */
	public String getConnection(OPC opc) {
		//
		try {
			JSONObject opcObj = new JSONObject();
			opcObj.put("opc", opc);
			String url = "http://" + opc.getOpc_agent_ip() + ":" + String.valueOf(opc.getOpc_agent_port()) + "/api/opc/connection/check";
			return RestUtils.post(url, opcObj.toString());
		} catch (Exception e) {
			log.error("Agent connection failed. : " + e.getMessage(), e);
			return "ERROR";
		}
	}

	/**
	 * 태그 목록을 가져온다.
	 * 
	 * @param opc
	 * @return
	 */
	public TagDataTableObject getTags(OPC opc) {
		//
		TagDataTableObject tagObj = null;
		try {
			JSONObject opcObj = new JSONObject();
			opcObj.put("opc", opc);
			String url = "http://" + opc.getOpc_agent_ip() + ":" + String.valueOf(opc.getOpc_agent_port()) + "/api/opc/tags";
			String resultString = RestUtils.post(url, opcObj.toString());

			if (StringUtils.isNotEmpty(resultString)) {
				JSONObject replyObj = JSONObject.fromObject(resultString);
				tagObj = getTagDataTableObjectFromJson(replyObj);
			}
		} catch (Exception e) {
			log.error("Can not get Point List. : " + e.getMessage(), e);
		}
		return tagObj;
	}

	/**
	 * Agent연결하고 시작한다.
	 * 
	 * @param opc
	 * @return
	 */
	public String startOPC(Map<String, Object> map) {
		//
		String resultText = "";
		try {
			String url = (String) map.get("path") + "/api/opc/start";
			resultText = RestUtils.post(url, (String) map.get("jsonString"));
		} catch (Exception e) {
			log.error("Agent start failed. : " + e.getMessage(), e);
		}
		return resultText;
	}

	/**
	 * Agent연결하고 재시작한다.
	 * 
	 * @param opc
	 * @return
	 */
	public String restartOPC(Map<String, Object> map) {
		//
		String resultText = "";
		try {
			String url = (String) map.get("path") + "/api/opc/restart";
			resultText = RestUtils.post(url, (String) map.get("jsonString"));
		} catch (Exception e) {
			log.error("Agent restart failed. : " + e.getMessage(), e);
		}
		return resultText;
	}

	/**
	 * Agent연결하고 중지한다.
	 * 
	 * @param opc
	 * @return
	 */
	public String stopOPC(OPC opc) {
		//
		String resultText = "";
		try {
			JSONObject opcObj = new JSONObject();
			opcObj.put("opc", opc);
			String url = "http://" + opc.getOpc_agent_ip() + ":" + String.valueOf(opc.getOpc_agent_port()) + "/api/opc/stop";
			resultText = RestUtils.post(url, opcObj.toString());
		} catch (Exception e) {
			log.error("Agent stop failed. : " + e.getMessage(), e);
		}
		return resultText;
	}

	/**
	 * 
	 * @param map
	 * @return
	 */
	public String recoveryOPC(Map<String, Object> map) {
		//
		String resultText = "";
		try {
			String url = (String) map.get("path") + "/api/opc/recovery";
			resultText = RestUtils.post(url, (String) map.get("jsonString"));
		} catch (Exception e) {
			log.error("Agent recovery failed. : " + e.getMessage(), e);
		}
		return resultText;
	}

	public TagDataTableObject getTagDataTableObjectFromJson(JSONObject obj) {
		//
		TagDataTableObject tagData = new TagDataTableObject();
		List<Tag> tagList = new ArrayList<Tag>();

		tagData.setiTotalDisplayRecords(obj.getInt("iTotalRecords"));
		tagData.setiTotalRecords(obj.getInt("iTotalDisplayRecords"));
		JSONArray tagArray = obj.getJSONArray("aaData");
		
		log.info("OPC Tag array size = [" + tagArray.size() + "]");

		for (int i = 0; i < tagArray.size(); i++) {
			JSONObject tagObj = tagArray.getJSONObject(i);
			Tag tag = (Tag) JSONObject.toBean(tagObj, Tag.class);
			tagList.add(tag);
		}
		tagData.setAaData(tagList);

		return tagData;
	}

}
