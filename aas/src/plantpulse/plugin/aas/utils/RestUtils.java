package plantpulse.plugin.aas.utils;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.http.HttpHeaders;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.entity.StringEntity;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClients;

public class RestUtils {

	private static final Log log = LogFactory.getLog(RestUtils.class); 
	
	public static String post(String url, String jsonString) throws IOException {
		return post(url, true, "application/json", 60 * 1000, 60 * 1000, jsonString);
	}
	
	public static String post(String url, boolean doOutput, String contentType, int connectionTimeout, int readTimeout, String jsonString) throws IOException {
		
		StringBuffer result = new StringBuffer();
		CloseableHttpClient httpclient = null;
		HttpPost httpPost = null;
		CloseableHttpResponse response = null;
		
		try {
			
			httpclient  = HttpClients.createDefault();
			httpPost = new HttpPost(url);
			StringEntity entity = new StringEntity(jsonString);
			httpPost.setEntity(entity);
			httpPost.setHeader(HttpHeaders.CONTENT_TYPE, contentType);
			
			response = httpclient.execute(httpPost);
			if(response.getStatusLine().getStatusCode() != 200){
				throw new IOException("HTTP RESPONSE STATUS CODE ERORR : " + response.getStatusLine().getStatusCode());
			}
		    //
		    BufferedReader br = new BufferedReader(new InputStreamReader((response.getEntity().getContent())));
			String line = "";
			while ((line = br.readLine()) != null) {
				result.append(line);
			}
			log.debug("Rest API execute success : URL=[" + url + "], JSON_STRING=[" + jsonString + "], STATUS_CODE = [" + response.getStatusLine().getStatusCode() + "], OUTPUT = [" + result.toString() + "]");
		    return result.toString();
		}catch(IOException e){
			log.error("Rest API execute failed : URL=[" + url + "], JSON_STRING=[" + jsonString + "], ERROR=[" + e.getMessage() + "]", e);
			throw e;
		} finally {
			if(response != null) response.close();
			if(httpclient != null) httpclient.close();
		}
		
		
	}
	
}
