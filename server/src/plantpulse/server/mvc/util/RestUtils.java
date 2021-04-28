package plantpulse.server.mvc.util;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.http.HttpHeaders;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.entity.StringEntity;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClients;

/**
 * RestUtils
 * @author lsb
 *
 */
public class RestUtils {

	private static final Log log = LogFactory.getLog(RestUtils.class);

	public static String post(String url, String paramString) throws IOException {
		return post(url, true, "application/json", 60 * 1000, 60 * 1000, paramString);
	}

	public static String post(String url, boolean doOutput, String contentType, int connectionTimeout, int readTimeout, String paramString) throws IOException {

		StringBuffer result = new StringBuffer();
		CloseableHttpClient httpclient = null;
		HttpPost httpPost = null;
		CloseableHttpResponse response = null;

		try {

			httpclient = HttpClients.createDefault();
			httpPost = new HttpPost(url);
			StringEntity entity = new StringEntity(paramString);
			httpPost.setEntity(entity);
			httpPost.setHeader(HttpHeaders.CONTENT_TYPE, contentType);

			response = httpclient.execute(httpPost);
			if (response.getStatusLine().getStatusCode() != 200) {
				throw new IOException("Failed : HTTP error code  : " + response.getStatusLine().getStatusCode());
			}
			//
			BufferedReader br = new BufferedReader(new InputStreamReader((response.getEntity().getContent())));
			String line = "";
			while ((line = br.readLine()) != null) {
				result.append(line);
			}
			log.info("Rest POST API execute success : URL=[" + url + "],  STATUS_CODE = [" + response.getStatusLine().getStatusCode() + "]");
			return result.toString();
		} catch (IOException e) {
			if(paramString != null && paramString.length() > 100) {
				paramString = paramString.substring(0, 99)  + "...";
			}
			log.error("Rest POST API execute failed : URL=[" + url + "], PARAMETER=[" + paramString + "], ERROR=[" + e.getMessage() + "]");
			throw e;
		} finally {
			if (response != null)
				response.close();
			if (httpclient != null)
				httpclient.close();
		}

	};
	
	
	public static String get(String url)  throws IOException {
		StringBuffer result = new StringBuffer();
		CloseableHttpClient httpclient = null;
		HttpGet getRequest  = null;
		CloseableHttpResponse response = null;
		try {

			httpclient = HttpClients.createDefault();
			 getRequest = new HttpGet(url);
			getRequest.addHeader("accept", "application/json");

			response = httpclient.execute(getRequest);

			if (response.getStatusLine().getStatusCode() != 200) {
				throw new RuntimeException("Failed : HTTP error code : " + response.getStatusLine().getStatusCode());
			}

			//
			BufferedReader br = new BufferedReader(new InputStreamReader((response.getEntity().getContent())));
			String line = "";
			while ((line = br.readLine()) != null) {
				result.append(line);
			}
			log.info("Rest GET API execute success : URL=[" + url + "],  STATUS_CODE = [" + response.getStatusLine().getStatusCode() + "]");
			return result.toString();
			
		  } catch (IOException e) {
			    log.error("Rest GET API execute failed : URL=[" + url + "], ERROR=[" + e.getMessage() + "]");
				throw e;
		  }finally {
				if (response != null)
					response.close();
				if (httpclient != null)
					httpclient.close();
			}
	}

}
