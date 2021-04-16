package plantpulse.cep.engine.utils;

public class TextUtils {
	
	public static String min(String text) {
		if(text.length() > 200) {
			return text.substring(0, 200) + " ...";
		}else {
			return text;
		}
	}

}
