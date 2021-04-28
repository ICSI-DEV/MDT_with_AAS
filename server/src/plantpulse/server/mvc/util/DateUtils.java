package plantpulse.server.mvc.util;

import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Date;
import java.util.TimeZone;

public class DateUtils {

	public static final String START_TIME = "00:00:00";
    public static final String END_TIME = "23:59:59";
    
	/**
	 * getToDate
	 * 
	 * @return java.sql.Date
	 */
	public static java.sql.Date getToDate() {
		return new java.sql.Date(System.currentTimeMillis());
	}

	public static long toTimestamp(String from) throws Exception{
		DateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
		Date date = dateFormat.parse(from);
		long time = date.getTime();
		return time;
	};
	
	public static Date addMonths(Date toDate, int number) {
		Calendar calendar = Calendar.getInstance();
		calendar.setTime(toDate);
		calendar.add(Calendar.MONTH, number);
		return calendar.getTime();
	}

	public static Date addDays(Date toDate, int number) {
		Calendar calendar = Calendar.getInstance();
		calendar.setTime(toDate);
		calendar.add(Calendar.DATE, number);
		return calendar.getTime();
	}

	public static Date addHours(Date toDate, int number) {
		Calendar calendar = Calendar.getInstance();
		calendar.setTime(toDate);
		calendar.add(Calendar.HOUR, number);
		return calendar.getTime();
	}

	public static Date addMinutes(Date toDate, int number) {
		Calendar calendar = Calendar.getInstance();
		calendar.setTime(toDate);
		calendar.add(Calendar.MINUTE, number);
		return calendar.getTime();
	}

	public static String fmtDate(long timestamp) {
		SimpleDateFormat formatter = new SimpleDateFormat("yyyy-MM-dd HH:mm");
		return formatter.format(new Date(timestamp));
	}

	public static String currDateBy00() {
		SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd");
		return sdf.format(new Date()) + " 00:00";
	}

	public static String currDateByMinus10Minutes() {
		SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm");
		Calendar cal = Calendar.getInstance();
		cal.setTime(new Date());
		cal.add(Calendar.MINUTE, -10);
		return sdf.format(cal.getTime());
	};

	
	public static String currDateByMinus1Hours() {
		SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm");
		Calendar cal = Calendar.getInstance();
		cal.setTime(new Date());
		cal.add(Calendar.HOUR, -1);
		return sdf.format(cal.getTime());
	};


	public static String currDateByMinus1Day() {
		SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm");
		Calendar cal = Calendar.getInstance();
		cal.setTime(new Date());
		cal.add(Calendar.DATE, -1);
		return sdf.format(cal.getTime());
	};
	
	public static String currDateByMinus7Day() {
		SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm");
		Calendar cal = Calendar.getInstance();
		cal.setTime(new Date());
		cal.add(Calendar.DATE, -7);
		return sdf.format(cal.getTime());
	};

	public static String currDateByMinus1Month() {
		SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm");
		Calendar cal = Calendar.getInstance();
		cal.setTime(new Date());
		cal.add(Calendar.MONTH, -1);
		return sdf.format(cal.getTime());
	};
	
	@Deprecated
	public static String toDate() {
		SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd");
		return sdf.format(new Date());
	};
	
	public static Date toDate(String date_s) throws Exception  {
		SimpleDateFormat dt = new SimpleDateFormat("yyyyy-MM-dd"); 
		Date date = dt.parse(date_s); 
		return date;
	}

	public static String currDateBy24() {
		SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd");
		return sdf.format(new Date()) + " 23:59";
	}

	public static String currDate() {
		SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm");
		return sdf.format(new Date());
	}
	
	public static String fmtDate(Date date) {
		SimpleDateFormat formatter = new SimpleDateFormat("yyyy-MM-dd");
		return formatter.format(date);
	}
	
	public static String fmtDateTime(Date date) {
		SimpleDateFormat formatter = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
		return formatter.format(date);
	}

	public static String fmtISO(long timestamp) {
		SimpleDateFormat formatter = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
		return formatter.format(new Date(timestamp));
	}

	public static String fmtARFF(long timestamp) {
		SimpleDateFormat formatter = new SimpleDateFormat("yyyy-MM-dd_HH:mm:ss");
		return formatter.format(new Date(timestamp));
	}

	public static String fmtSuffix() {
		SimpleDateFormat formatter = new SimpleDateFormat("yyyyMMdd_HHmmss");
		return formatter.format(new Date());
	}
	
	/**
	 * 이번주의 시작일자를 반환한다.
	 * @return
	 */
	public static String getFirstDateOfWeek() {
		DateFormat df = new SimpleDateFormat("yyyy-MM-dd");
		String date = "";
		Calendar currentCalendar = Calendar.getInstance();
		currentCalendar.add(Calendar.DATE, 1 - currentCalendar.get(Calendar.DAY_OF_WEEK));
		return df.format(currentCalendar.getTime());
	}
	
	/**
	 * 이번주의 마지막일자를 반환한다.
	 * @return
	 */
	public static String getLastDateOfWeek() {
		DateFormat df = new SimpleDateFormat("yyyy-MM-dd");
		String date = "";
		Calendar currentCalendar = Calendar.getInstance();
		currentCalendar.add(Calendar.DATE, 7 - currentCalendar.get(Calendar.DAY_OF_WEEK));
		return df.format(currentCalendar.getTime());
	}
	
	/**
	 * UTC 스트링 날자를 로컬 데이트로 반환한다.
	 * @param date_s
	 * @return
	 * @throws Exception
	 */
	public static Date utcToLocal(String date_s) throws Exception {
		SimpleDateFormat dt = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS"); 
		dt.setTimeZone(TimeZone.getTimeZone("UTC"));
		Date date = dt.parse(date_s); 
		return date;
	}


}
