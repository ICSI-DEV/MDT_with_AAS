package plantpulse.cep.engine.utils;

import java.text.SimpleDateFormat;
import java.util.Date;

public class TimestampUtils {

	public static long getNowTimestamp() {
		return System.currentTimeMillis();
	};

	public static int timestampToYYYYMMDD(long timestamp) {
		SimpleDateFormat formatter = new SimpleDateFormat("yyyyMMdd");
		return Integer.parseInt(formatter.format(new Date(timestamp)));
	};

	public static int todayYYYYMMDD() {
		return timestampToYYYYMMDD(System.currentTimeMillis());
	};

	public static int getHH(long timestamp) {
		Date date = new Date(timestamp);
		return (date.getHours());
	};
	
	public static int getMM(long timestamp) {
		Date date = new Date(timestamp);
		return (date.getMinutes());
	};
	
	public static int getSS(long timestamp) {
		Date date = new Date(timestamp);
		return (date.getSeconds());
	};

}
