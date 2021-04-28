package plantpulse.cep.engine.logging;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Date;
import java.util.List;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import plantpulse.cep.engine.CEPEngineManager;
import plantpulse.cep.engine.diagnostic.DiagnosticHandler;

/**
 * EngineLogger
 * 
 * @author lenovo
 *
 */
public class  EngineLogger {

	private static final Log log4j = LogFactory.getLog(EngineLogger.class);

	private static List<String> logs = new ArrayList<String>();

	public static final String INFO = "INFO";
	public static final String WARN = "WARN";
	public static final String ERROR = "ERROR";

	private static boolean hasError;

	public static List<String> getLogs() {
		return logs;
	}

	@SuppressWarnings("deprecation")
	public static void info(String message) {
		logs.add(String.format("[%s] [%s] [%s] \n", new Date().toLocaleString(), INFO, message));
		log4j.info(message);
		LoggingCount.INFO_COUNT.incrementAndGet();
		//
		DiagnosticHandler.handleFormLocal(System.currentTimeMillis(), INFO, message);
		//
		if(CEPEngineManager.getInstance().isStarted()){
			plantpulse.event.Log log = new plantpulse.event.Log();
			log.setTimestamp(System.currentTimeMillis());
			log.setLevel(INFO);
			log.setMessage(message);
			CEPEngineManager.getInstance().getProvider().getEPRuntime().sendEvent(log);
		}
	}

	@SuppressWarnings("deprecation")
	public static void warn(String message) {
		logs.add(String.format("[%s] [%s] [%s] \n", new Date().toLocaleString(), WARN, message));
		log4j.warn(message);
		LoggingCount.WARN_COUNT.incrementAndGet();
		//
		DiagnosticHandler.handleFormLocal(System.currentTimeMillis(), WARN, message);
		//
		if(CEPEngineManager.getInstance().isStarted()){
			plantpulse.event.Log log = new plantpulse.event.Log();
			log.setTimestamp(System.currentTimeMillis());
			log.setLevel(WARN);
			log.setMessage(message);
			CEPEngineManager.getInstance().getProvider().getEPRuntime().sendEvent(log);
		}
	}

	@SuppressWarnings("deprecation")
	public static void error(String message) {
		logs.add(String.format("[%s] [%s] [%s] \n", new Date().toLocaleString(), ERROR, message));
		log4j.error(message);
		LoggingCount.ERROR_COUNT.incrementAndGet();
		//
		DiagnosticHandler.handleFormLocal(System.currentTimeMillis(), ERROR, message);
		//
		if(CEPEngineManager.getInstance().isStarted()){
			plantpulse.event.Log log = new plantpulse.event.Log();
			log.setTimestamp(System.currentTimeMillis());
			log.setLevel(ERROR);
			log.setMessage(message);
			CEPEngineManager.getInstance().getProvider().getEPRuntime().sendEvent(log);
		}

		hasError = true;
	}

	public static void reset() {
		hasError = false;
		logs = new ArrayList<String>();
	}

	public static String getPrintableText() {
		StringBuffer buffer = new StringBuffer();
		Collections.reverse(logs);
		for (int i = 0; i < logs.size(); i++) {
			buffer.append(logs.get(i));
		}
		return buffer.toString();
	}

	public static boolean isHasError() {
		return hasError;
	}

}
