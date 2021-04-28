package plantpulse.plugin.aas.utils;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;

public class SQLUtils {
	
	private static final String ISO_DATE_PATTERN = "yyyy-MM-dd HH:mm:ss";
	
	public static String convert(String sql, Date last_scan_date, Date current_date){
		sql = sql.replace("{last_scan_date}", isodate(last_scan_date));
		sql = sql.replace("{current_date}", isodate(current_date));
		return sql;
	}

	public static String isodate(Date date){
		SimpleDateFormat format = new SimpleDateFormat(ISO_DATE_PATTERN);
		return format.format(date);
	}
	
	public static Date dateiso(String date) throws Exception {
		SimpleDateFormat transFormat = new SimpleDateFormat(ISO_DATE_PATTERN);
		try {
			return transFormat.parse(date);
		} catch (ParseException e) {
			throw e;
		}
	}
}
