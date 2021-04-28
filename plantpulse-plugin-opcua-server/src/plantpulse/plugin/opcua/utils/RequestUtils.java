package plantpulse.plugin.opcua.utils;

import java.util.Enumeration;
import java.util.HashMap;
import java.util.Map;

import javax.servlet.http.HttpServletRequest;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.http.HttpResponse;
import org.apache.http.client.HttpClient;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.client.methods.HttpPut;
import org.apache.http.entity.StringEntity;
import org.apache.http.impl.client.HttpClientBuilder;

import plantpulse.json.JSONObject;
import plantpulse.plugin.opcua.server.PluginMessageProcessor;

/**
 * RequestUtils
 *
 * @author lsb
 *
 */
public class RequestUtils {
	
	private static final Log log = LogFactory.getLog(RequestUtils.class);

	public static Map<String, Object> getParameterMap(HttpServletRequest request) {
		Map<String,Object> parameterMap = new HashMap<>();
		Enumeration<?> enums = request.getParameterNames();
		while (enums.hasMoreElements()) {
			String paramName = (String) enums.nextElement();
			String[] parameters = request.getParameterValues(paramName);
			if (parameters.length > 1) {
				parameterMap.put(paramName, parameters);
			} else {
				parameterMap.put(paramName, parameters[0]);
			}
		};
		return parameterMap;
	}

	public static void httpPut(String requestURL,JSONObject json) {
		try {
			HttpClient client = HttpClientBuilder.create().build();
			HttpPut putRequest = new HttpPut(requestURL);
			putRequest.setHeader("Accept", "application/json");
			putRequest.setHeader("Connection", "keep-alive");
			putRequest.setHeader("Content-Type", "application/json");
			putRequest.setEntity(new StringEntity(json.toString()));

			HttpResponse response = client.execute(putRequest);

			//Response 출력
			if (response.getStatusLine().getStatusCode() == 200 || response.getStatusLine().getStatusCode() == 201) {
//				ResponseHandler<String> handler = new BasicResponseHandler();
//				String body = handler.handleResponse(response);
//				System.out.println(body);
			} else {
				System.out.println("response is error : " + response.getStatusLine().getStatusCode());
			}
		} catch (Exception e){
			log.error(e,e);
		}
	}


	public static void httpPost(String requestURL) {

		try {
			HttpClient client = HttpClientBuilder.create().build();
			HttpPost postRequest = new HttpPost(requestURL);
			HttpResponse response = client.execute(postRequest);

			//Response 출력
			if (response.getStatusLine().getStatusCode() == 200 || response.getStatusLine().getStatusCode() == 201) {
//				ResponseHandler<String> handler = new BasicResponseHandler();
//				String body = handler.handleResponse(response);
//				System.out.println(body);
			} else {
				System.out.println("response is error : " + response.getStatusLine().getStatusCode());
			}
		} catch (Exception e){
			log.error(e,e);
		}
	}

}