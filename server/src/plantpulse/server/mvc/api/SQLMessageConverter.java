package plantpulse.server.mvc.api;

public class SQLMessageConverter {

	public static String convert(String original) {
		if (original == null) {
			return "NULL";
		}
		if (original.indexOf("integrity constraint violation: unique constraint or index violation") > -1) {
			return "요청하신 ID 의 오브젝트가 이미 존재합니다.";
		}
		return original;
	}

}
