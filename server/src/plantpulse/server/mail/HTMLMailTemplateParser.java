package plantpulse.server.mail;

import java.io.StringWriter;
import java.util.Map;

import freemarker.template.Configuration;
import freemarker.template.ObjectWrapper;
import freemarker.template.Template;
import freemarker.template.TemplateExceptionHandler;

/**
 * MailTemplateParser
 * @author leesa
 *
 */
public class HTMLMailTemplateParser {
	
	/**
	 * 프리메이커 HTML 메일 템플릿을 파싱한다.
	 * 
	 * @param dir
	 * @param fileName
	 * @param dataModel
	 * @return
	 * @throws Exception
	 */
	public static String parseFreemarkerTemplate(String templateDir, String templateFileName, Map<String, Object> dataModel) throws Exception {
		try {
			Configuration configuration = new Configuration();
			configuration.setDirectoryForTemplateLoading(new java.io.File(templateDir));
			configuration.setTemplateUpdateDelay(0);
			configuration.setTemplateExceptionHandler(TemplateExceptionHandler.DEBUG_HANDLER);
			configuration.setObjectWrapper(ObjectWrapper.BEANS_WRAPPER);
			configuration.setDefaultEncoding("UTF-8");

			Template template = configuration.getTemplate(templateFileName);
			StringWriter output = new StringWriter();
			template.process(dataModel, output);

			return output.toString();

		} catch (Exception ex) {
			throw new Exception("Freemarker html mail template parsing error : " + ex.getMessage(), ex);
		}
	}

}
