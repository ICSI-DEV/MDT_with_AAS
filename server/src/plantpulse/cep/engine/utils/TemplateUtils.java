package plantpulse.cep.engine.utils;

import java.io.StringReader;
import java.io.StringWriter;
import java.util.Map;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import freemarker.template.Configuration;
import freemarker.template.ObjectWrapper;
import freemarker.template.Template;
import freemarker.template.TemplateExceptionHandler;

public class TemplateUtils {

	private static final Log log = LogFactory.getLog(TemplateUtils.class);

	/**
	 * 프리메이커 템플릿을 파싱한다.
	 * 
	 * @param content
	 * @param dataModel
	 * @return
	 * @throws Exception
	 */
	public static String parse(String content, Map<String, ?> data) throws Exception {
		try {
			// Using String
			Configuration configuration = new Configuration();
			configuration.setTemplateUpdateDelay(0);
			configuration.setTemplateExceptionHandler(TemplateExceptionHandler.DEBUG_HANDLER);
			configuration.setObjectWrapper(ObjectWrapper.BEANS_WRAPPER);
			configuration.setDefaultEncoding("UTF-8");
			//
			//
			Template template = new Template("PTT", new StringReader(content), configuration);
			StringWriter output = new StringWriter();
			template.process(data, output);

			// System.out.println("TPL OUTPUT : " + output.toString());

			return output.toString();

		} catch (Exception ex) {
			log.warn("Freemarker template parsing error : content=[" + content + "], data=[" + data.toString() + "]" + ex.getMessage(), ex);
		}

		return content;
	}

}
