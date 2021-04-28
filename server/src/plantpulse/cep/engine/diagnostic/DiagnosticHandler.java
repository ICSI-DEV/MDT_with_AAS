package plantpulse.cep.engine.diagnostic;

import java.io.UnsupportedEncodingException;
import java.net.InetAddress;
import java.net.URLDecoder;
import java.net.UnknownHostException;

import javax.servlet.http.HttpServletRequest;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.json.JSONObject;

import plantpulse.cep.engine.CEPEngineManager;
import plantpulse.cep.engine.monitoring.MonitorConstants;
import plantpulse.cep.listener.push.PushClientFactory;
import plantpulse.diagnostic.Diagnostic;
import plantpulse.diagnostic.DiagnosticListCache;
import plantpulse.server.mvc.util.DateUtils;

/**
 * DiagnosticHandler
 * @author lsb
 *
 */
public class DiagnosticHandler {
	
	private static Log log = LogFactory.getLog(DiagnosticHandler.class);
	
	
	public static void handleFormLocal(long timestamp, String level, String msg){
		Diagnostic d = new Diagnostic();
		d.setTimestamp(timestamp);
		try {
			d.setHost(InetAddress.getLocalHost().getHostAddress());
		} catch (UnknownHostException e) {
			d.setHost("UNKNOWN");
		}
		d.setType("SERVER");
		d.setLevel(level);
		d.setMessage(msg);
		//
		DiagnosticListCache.getInstance().add(d);
		
		//
		try{
			if(CEPEngineManager.getInstance().isStarted()){
				JSONObject json = JSONObject.fromObject(d);
				PushClientFactory.createPushClient(MonitorConstants.PUSH_URL_DIAGNOSTIC).sendJSON(json);
			}
		}catch(Exception ex){
			
		}
	}
	
	/**
	 * handleFromRequest
	 * @param request
	 * @throws Exception
	 */
	public void handleFromRequest(HttpServletRequest request) {
		Diagnostic d = new Diagnostic();
		d.setTimestamp(Long.parseLong(request.getParameter("timestamp")));
		d.setHost(request.getParameter("host"));
		d.setType(request.getParameter("type"));
		d.setLevel(request.getParameter("level"));
		try {
			d.setMessage(URLDecoder.decode(request.getParameter("message"), "UTF-8") );
		} catch (UnsupportedEncodingException e) {
			log.error(e);
		}
		//
		DiagnosticListCache.getInstance().add(d);
		//
		
		String fMM_msg = String.format("[%s] [%s] [%s] [%s] [%s]", DateUtils.fmtISO(d.getTimestamp()), d.getHost(), d.getType(), d.getLevel(), d.getMessage());
		if(d.getLevel().equals("INFO")){
			log.info(fMM_msg);
		}else if(d.getLevel().equals(DiagnosticLevel.WARN)){
			log.warn(fMM_msg);
		}else if(d.getLevel().equals(DiagnosticLevel.ERROR)){
			log.error(fMM_msg);
		}
		
		//
		try{
			if(CEPEngineManager.getInstance().isStarted()){
				JSONObject json = JSONObject.fromObject(d);
				PushClientFactory.createPushClient(MonitorConstants.PUSH_URL_DIAGNOSTIC).sendJSON(json);
			}
		}catch(Exception ex){
			
		}
	}

}
