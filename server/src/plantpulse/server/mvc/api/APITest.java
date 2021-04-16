package plantpulse.server.mvc.api;

import java.io.IOException;
import java.net.URLEncoder;
import java.util.Random;

import org.json.JSONObject;

import plantpulse.server.mvc.util.RestUtils;

public class APITest {

	public static void test() {
		addOpc();
		addTag();
		for (int i = 0; i < 10000; i++) {
			insertData();
		}
	};
	


	public static void addOpc() {
		try {
			JSONObject json = new JSONObject();
			json.put("site_id", "SITE_00001");
			json.put("opc_id", "VOPC_0001"); // 반드시 VOPC_ 로 시작해야함.
			json.put("opc_name", "VIRTUAL_OPC_0001");
			json.put("opc_server_ip", "0.0.0.0");

			json.put("_api_key", "GED1-PPLA-TAXD-PTL1-14DF");

			RestUtils.post("http://localhost:7000/server/api/vopc/delete", json.toString());
			String result = RestUtils.post("http://localhost:7000/server/api/vopc/add", json.toString());
			System.out.println(result);

		} catch (IOException e) {
			e.printStackTrace();
		}
	}

	public static void addTag() {
		try {
			JSONObject json = new JSONObject();
			json.put("opc_id", "VOPC_0001");
			json.put("tag_id", "VTAG_09001"); // 반드시 VTAG_로 시작해야함.
			json.put("tag_name", "VIRTUAL.OPC.TEST.VTAG_09001");
			json.put("group_name", "group");
			json.put("tag_source", "OPC");
			json.put("java_type", "Float");
			json.put("alias_name", "valias_name");
			json.put("unit", "%");
			json.put("description", URLEncoder.encode("한글_설명_DESCRIPTION", "UTF-8"));

			json.put("_api_key", "GED1-PPLA-TAXD-PTL1-14DF");

			RestUtils.post("http://localhost:7000/server/api/vopc/tag/delete", json.toString());
			String result = RestUtils.post("http://localhost:7000/server/api/vopc/tag/add", json.toString());
			System.out.println(result);

		} catch (IOException e) {
			e.printStackTrace();
		}
	}

	public static void insertData() {

		try {

			float min = 50.00f;
			float max = 100.00f;
			Random rand = new Random();
			float value = rand.nextFloat() * (max - min) + min;

			JSONObject json = new JSONObject();
			json.put("opc_id", "VOPC_0001");
			json.put("id", "VTAG_09001"); // 반드시 VTAG_로 시작해야함.
			json.put("name", "VIRTUAL.OPC.TEST.VTAG_09001");
			json.put("type", "float");
			json.put("group_name", "group");
			json.put("timestamp", System.currentTimeMillis());
			json.put("value", value);
			json.put("quality", 192);
			json.put("error_code", 0);

			json.put("_api_key", "GED1-PPLA-TAXD-PTL1-14DF");

			String result = RestUtils.post("http://localhost:7000/server/api/vopc/tag/data/insert", json.toString());
			System.out.println(result);
		} catch (IOException e) {
			e.printStackTrace();
		}

	}

}
