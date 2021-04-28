package plantpulse.server.mvc.util;

public class ParamUtils {

	public static String[] commaStringToArray(String comstr) {
		return comstr.split(",");
	}

	public static String commaStringToSQLInString(String comstr) {
		StringBuffer buffer = new StringBuffer();
		String[] array = comstr.split(",");
		for (int i = 0; i < array.length; i++) {
			buffer.append("'" + array[i] + "'");
			if (i < (array.length - 1)) {
				buffer.append(", ");
			}
		}
		return buffer.toString();
	}

}
