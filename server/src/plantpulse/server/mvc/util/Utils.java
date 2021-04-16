package plantpulse.server.mvc.util;

import javax.servlet.http.HttpServletRequest;

public class Utils {

	public static String removeLastSemiColon(String str) {
		if (str.endsWith(";")) {
			return str.substring(0, str.length() - 1);
		} else {
			return str;
		}
	}

	/**
	 * isIE
	 * 
	 * @param request
	 * @return
	 */
	public static boolean isIE(HttpServletRequest request) {
		String userAgent = request.getHeader("User-Agent");
		if (userAgent.indexOf("rv:12") > -1) {
			return true;
		} else if (userAgent.indexOf("rv:11") > -1) {
			return true;
		} else if (userAgent.indexOf("MSIE 10") > -1) {
			return true;
		} else if (userAgent.indexOf("MSIE 9") > -1) {
			return true;
		} else if (userAgent.indexOf("MSIE 8") > -1) {
			return true;
		} else if (userAgent.indexOf("MSIE 7") > -1) {
			return true;
		} else if (userAgent.indexOf("MSIE 6") > -1) {
			return true;
		} else {
			return false;
		}
	}

	/**
	 * covertStringArrayToCommaString
	 * 
	 * @param name
	 * @return
	 */
	public static String covertStringArrayToCommaString(String[] name) {
		StringBuilder sb = new StringBuilder();
		for (String st : name) {
			sb.append('\'').append(st).append('\'').append(',');
		}
		if (name.length != 0)
			sb.deleteCharAt(sb.length() - 1);
		return sb.toString();
	}

}
