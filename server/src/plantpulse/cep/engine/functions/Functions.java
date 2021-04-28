package plantpulse.cep.engine.functions;

import java.text.DecimalFormat;
import java.text.SimpleDateFormat;
import java.util.Date;

/**
 * Functions
 * <p>
 * Esper CEP User-defined functions
 * </p>
 * 
 * @author lsb
 *
 */
public class Functions {

	
	public static final String ISO_DATE_FORMAT = "yyyy-MM-dd";
	public static final String ISO_DATE_TIME_FORMAT = "yyyy-MM-dd HH:mm:ss";
	public static final String ISO_DATE_TIME_MS_FORMAT = "yyyy-MM-dd HH:mm:ss.SSSSS";
	public static final String ISO_TIME_FORMAT = "HH:mm:ss";

	/**
	 * formatISODate
	 * 
	 * @param timestamp
	 * @return yyyy-MM-dd
	 */
	public static String format_date(long timestamp) {
		SimpleDateFormat formatter = new SimpleDateFormat(ISO_DATE_FORMAT);
		return formatter.format(new Date(timestamp));
	}

	/**
	 * formatISODateTime
	 * 
	 * @param timestamp
	 * @return yyyy-MM-dd HH-mm-ss
	 */
	public static String format_datetime(long timestamp) {
		SimpleDateFormat formatter = new SimpleDateFormat(ISO_DATE_TIME_FORMAT);
		return formatter.format(new Date(timestamp));
	}
	
	public static String format_datetime_ms(long timestamp) {
		SimpleDateFormat formatter = new SimpleDateFormat(ISO_DATE_TIME_MS_FORMAT);
		return formatter.format(new Date(timestamp));
	}

	/**
	 * formatISOTime
	 * 
	 * @param timestamp
	 * @return
	 */
	public static String format_time(long timestamp) {
		SimpleDateFormat formatter = new SimpleDateFormat(ISO_TIME_FORMAT);
		return formatter.format(new Date(timestamp));
	}

	/**
	 * formatTimestamp
	 * 
	 * @param timestamp
	 * @param format
	 * @return
	 */
	public static String format_timestamp(long timestamp, String format) {
		SimpleDateFormat formatter = new SimpleDateFormat(format);
		return formatter.format(new Date(timestamp));
	}

	/**
	 * format_number
	 * @param number
	 * @param format
	 * @return
	 */
	public static String format_number(long number, String format) {
		DecimalFormat formatter = new DecimalFormat(format);
		return formatter.format(number);
	}

	/**
	 * format_number
	 * @param number
	 * @param format
	 * @return
	 */
	public static String format_number(int number, String format) {
		DecimalFormat formatter = new DecimalFormat(format);
		return formatter.format(number);
	}

	/**
	 * format_number
	 * @param number
	 * @param format
	 * @return
	 */
	public static String format_number(double number, String format) {
		DecimalFormat formatter = new DecimalFormat(format);
		return formatter.format(number);
	}

	/**
	 * format_number
	 * @param number
	 * @param format
	 * @return
	 */
	public static String format_number(float number, String format) {
		DecimalFormat formatter = new DecimalFormat(format);
		return formatter.format(number);
	}

	/**
	 * format_bytes
	 * @param bytes
	 * @return
	 */
	public static String format_bytes(long bytes) {
		boolean si = true;
		int unit = si ? 1000 : 1024;
		if (bytes < unit)
			return bytes + " B";
		int exp = (int) (Math.log(bytes) / Math.log(unit));
		String pre = (si ? "kMGTPE" : "KMGTPE").charAt(exp - 1) + (si ? "" : "i");
		return String.format("%.1f %sB", bytes / Math.pow(unit, exp), pre);
	}

	/**
	 * format_bytes
	 * @param bytes
	 * @return
	 */
	public static String format_bytes(double bytes) {
		boolean si = true;
		int unit = si ? 1000 : 1024;
		if (bytes < unit)
			return bytes + " B";
		int exp = (int) (Math.log(bytes) / Math.log(unit));
		String pre = (si ? "kMGTPE" : "KMGTPE").charAt(exp - 1) + (si ? "" : "i");
		return String.format("%.1f %sB", bytes / Math.pow(unit, exp), pre);
	}

	/**
	 * to_int
	 * @param str
	 * @return
	 */
	public static int to_int(String str) {
		try {
			return Integer.parseInt(str);
		} catch (Exception ex) {
			ex.printStackTrace();
			return -1;
		}
	}

	/**
	 * to_long
	 * @param str
	 * @return
	 */
	public static long to_long(String str) {
		try {
			return Long.parseLong(str);
		} catch (Exception ex) {
			ex.printStackTrace();
			return -1;
		}
	}

	/**
	 * to_float
	 * @param str
	 * @return
	 */
	public static float to_float(String str) {
		try {
			return Float.parseFloat(str);
		} catch (Exception ex) {
			ex.printStackTrace();
			return -1;
		}
	}

	/**
	 * to_double
	 * @param str
	 * @return
	 */
	public static double to_double(String str) {
		try {
			return Double.parseDouble(str);
		} catch (Exception ex) {
			ex.printStackTrace();
			return -1;
		}
	};
	
	public static boolean to_boolean(String str) {
		try {
			return Boolean.parseBoolean(str);
		} catch (Exception ex) {
			ex.printStackTrace();
			return false;
		}
	};
	

}
