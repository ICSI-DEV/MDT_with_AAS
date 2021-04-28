package plantpulse.cep.engine.utils;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.Date;
import java.util.GregorianCalendar;
import java.util.List;


/**
 * DateUtils
 * @author leesa
 *
 */
public class DateUtils {

	public static String format(Date date) {
		if (date == null) return "NULL";
		SimpleDateFormat formatted = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
		return formatted.format(date);
	}
	
	
	public static int formatInt(Date date) {
		SimpleDateFormat formatted = new SimpleDateFormat("yyyyMMdd");
		return Integer.parseInt(formatted.format(date));
	}
	
	public static Date stringToDate(String fmt, String str) throws ParseException {
		SimpleDateFormat formatted = new SimpleDateFormat(fmt);
		return formatted.parse(str);
	}

	public static String getTableName(String itemId) {
		String tmp1 = itemId.toLowerCase().replaceAll(" ", "");
		String tmp[] = tmp1.split("\\.");
		String tableName = null;

		if (tmp.length > 0) {
			String tmp2 = tmp[tmp.length - 1];

			if (tmp2.charAt(0) == '_') {
				tableName = tmp[0] + tmp[tmp.length - 1];
			} else {
				tableName = tmp[0] + "_" + tmp[tmp.length - 1];
			}
		} else {
			tableName = tmp1;
		}

		return tableName;
	}

	public static List<Date> getBetweenDates(Date startdate, Date enddate) {
		List<Date> dates = new ArrayList<Date>();
		Calendar calendar = new GregorianCalendar();
		calendar.setTime(startdate);
		//
		while (calendar.getTime().before(enddate)) {
			Date result = calendar.getTime();
			dates.add(result);
			calendar.add(Calendar.DATE, 1);
		}
		return dates;
	}

	public static String fmtSnapshotDate(long timestamp) {
		SimpleDateFormat formatter = new SimpleDateFormat("yyyy-MM-dd_HH:mm");
		return formatter.format(new Date(timestamp));
	}
	
	public static Date stringToTimestamp(String str) {
		SimpleDateFormat formatter = new SimpleDateFormat("yyyy-MM-dd");
		try {
			return formatter.parse(str);
		} catch (ParseException e) {
			e.printStackTrace();
			return null;
		}
	}

	public static Date isoToTimestamp(String from, boolean b) throws ParseException {
		if(from.length() == 19) {
			SimpleDateFormat formatter = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
			return formatter.parse(from);
		}else if(from.length() > 19){
			SimpleDateFormat formatter = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.S");
			return formatter.parse(from);
		}else {
		 throw new ParseException("Date iso string format only [yyyy-MM-dd HH:mm:ss] or [yyyy-MM-dd HH:mm:ss.S] : " + from, 1);
		}
	}
	
	public static LocalDateTime isoToLocalDateTime(String from) throws ParseException {
		if(from.length() == 19) {
			DateTimeFormatter formatter = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss");
			return  LocalDateTime.parse(from, formatter);
		}else if(from.length() > 19){
			DateTimeFormatter formatter = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss.S");
			return LocalDateTime.parse(from, formatter);
		}else {
		 throw new ParseException("Date iso string format only [yyyy-MM-dd HH:mm:ss] or [yyyy-MM-dd HH:mm:ss.S] : " + from, 1);
		}
	}
	
	

}
