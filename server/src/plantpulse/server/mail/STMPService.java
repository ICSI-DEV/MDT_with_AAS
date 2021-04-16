package plantpulse.server.mail;

import java.util.Arrays;
import java.util.Map;
import java.util.Properties;

import javax.mail.Authenticator;
import javax.mail.BodyPart;
import javax.mail.Message;
import javax.mail.Multipart;
import javax.mail.PasswordAuthentication;
import javax.mail.Session;
import javax.mail.Transport;
import javax.mail.internet.InternetAddress;
import javax.mail.internet.MimeBodyPart;
import javax.mail.internet.MimeMessage;
import javax.mail.internet.MimeMultipart;
import javax.mail.internet.MimeUtility;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import plantpulse.cep.engine.utils.PropertiesUtils;

public class STMPService {

	private static final Log log = LogFactory.getLog(STMPService.class);

	private static final String MAIL_CONFIG_PATH = "/mail.properties";

	/**
	 * 템플릿 메일 발송하기
	 * 
	 * @param to
	 * @param cc
	 * @param from
	 * @param subject
	 * @param templateDir
	 * @param templateFileName
	 * @param map
	 * @throws Exception
	 */
	public static void sendTemplateMail(InternetAddress[] to, InternetAddress[] cc, InternetAddress from, String subject, String templateDir, String templateFileName, Map<String, Object> map)
			throws Exception {
		String text = HTMLMailTemplateParser.parseFreemarkerTemplate(templateDir, templateFileName, map);
		sendMail(to, cc, from, subject, text);

	}

	private static void sendMail(InternetAddress[] to, InternetAddress[] cc, InternetAddress from, String subject, String html) throws Exception {
		try {

			final Properties config = PropertiesUtils.read(MAIL_CONFIG_PATH);

			Properties properties = System.getProperties();
			properties.putAll(config);
			
			//
			Session mail_session = null;
			if(properties.getProperty("mail.smtp.auth").equals("true")) {
				 mail_session = Session.getDefaultInstance(properties, new Authenticator() {
					@Override
					protected PasswordAuthentication getPasswordAuthentication() {
						return new PasswordAuthentication(config.getProperty("mail.smtp.user"), config.getProperty("mail.smtp.password"));
					}
				});
			}else {
			  mail_session = Session.getInstance(properties);
			}

			if (from == null) {
				from = new InternetAddress(config.getProperty("mail.smtp.user"));
			}

			MimeMessage msg = new MimeMessage(mail_session);

			// 메일 발송일
			msg.setSentDate(new java.util.Date());

			// 메일 수신자
			for (int i = 0; to != null && i < to.length; i++) {
				try {
					msg.addRecipient(Message.RecipientType.TO, to[i]);
				} catch (Exception ex) {
					log.error("Mail add recipient[to] error: address=[" + to[i] + "]", ex);
				}
			}

			// 메일 수신 참조자
			for (int i = 0; cc != null && i < cc.length; i++) {
				try {
					msg.addRecipient(Message.RecipientType.CC, cc[i]);
				} catch (Exception ex) {
					log.error("Mail add recipient[cc] error: address=[" + cc[i] + "]", ex);
				}
			}

			// 메일 발송자
			msg.setFrom(from);

			// 메일 제목
			String encode_subject = MimeUtility.encodeText(subject, "UTF-8", "B");
			msg.setSubject(encode_subject);

			Multipart multipart = new MimeMultipart();

			// 메세지
			BodyPart messageBodyPart = new MimeBodyPart();

			messageBodyPart.setContent(html, "text/html; charset=" + "UTF-8");

			multipart.addBodyPart(messageBodyPart);

			// 메일 내용
			msg.setContent(multipart);

			// 전송
			Transport.send(msg);

			log.info("Mail send successfully : to=" + Arrays.toString(to) + ", subject=[" + subject + "]");

		} catch (Exception ex) {
			String msg = "Mail service send error : " + ex.getMessage();
			log.error(msg, ex);
			throw new Exception(msg, ex);
		}

	}



}
