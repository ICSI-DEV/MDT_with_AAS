package plantpulse.cep.engine.scheduling;

import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.Date;

public class JobDateUtils {
	
	public static final String DATE_FORMAT = "yyyy-MM-dd HH:mm:ss";
	
	public static String from(long timestamp, String term){
		int min = getMinusMinutes(term);
		SimpleDateFormat format = new SimpleDateFormat(DATE_FORMAT);
		return format.format(new Date(timestamp - (1 * 1000 * 60 * min)));
	};
	
	public static String to(long timestamp){
		SimpleDateFormat format = new SimpleDateFormat(DATE_FORMAT);
		return format.format(new Date(timestamp));
	};
	
	
	public static String fromYesterday(){
		LocalDateTime datetime = LocalDateTime.now().minusDays(1);
		DateTimeFormatter format = DateTimeFormatter.ofPattern("yyyy-MM-dd");
		return datetime.format(format) + " 00:00:00.000";
	};
	
	public static String toYesterday(){
		LocalDateTime datetime = LocalDateTime.now().minusDays(1);
		DateTimeFormatter format = DateTimeFormatter.ofPattern("yyyy-MM-dd");
		return datetime.format(format) + " 23:59:59.999";
	};
	
	public static long toTimestamp(String from) throws Exception{
		DateFormat dateFormat = new SimpleDateFormat(DATE_FORMAT);
		Date date = dateFormat.parse(from);
		long time = date.getTime();
		return time;
	};
	
	public static int getMinusMinutes(String term){
		int    num  = Integer.parseInt(term.split(" ")[0]);
		String unit = term.split(" ")[1];
		if(unit.indexOf("MINUTES") > -1){
			return num * 1;
		}else if(unit.indexOf("HOURS") > -1){
			return num * 60;
		}else if(unit.indexOf("DAYS") > -1){
			return num * 60 * 24;
		}else{
			return -1;
		}
	};
	
	public static int getTodayIntType(){
		return getYYYMMDDIntType(System.currentTimeMillis());
	};
	
	public static int getTohourIntType(){
		return getHHIntType(System.currentTimeMillis());
	};
	
	
	public static int getYesterdayIntType(){
		return getYYYMMDDIntType(System.currentTimeMillis() - (1000*60*60*24));
	};
	
	
	public static String intToDashDate(int date){
		String ds = date + "";
		return ds.substring(0,4) + "-" + ds.substring(4,6) + "-" + ds.substring(6,8);
	};
	
	public static int getYYYMMDDIntType(long timestamp){
		DateFormat df = new SimpleDateFormat("yyyyMMdd");
		String s = df.format(new Date(timestamp));
		return Integer.valueOf(s);
	};
	
	public static int getHHIntType(long timestamp){
		DateFormat df = new SimpleDateFormat("HH");
		String s = df.format(new Date(timestamp));
		return Integer.valueOf(s);
	};
	
	public static String getCronPattern(String term){
		int    num  = Integer.parseInt(term.split(" ")[0]);
		String unit = term.split(" ")[1];
		String cron = "";
		if(unit.indexOf("SECONDS") > -1){
			cron = "0/" + num + " * * * * ?";
			if(num == 1) { cron = "0/1 * * * * ?"; }
			return cron;
		}else if(unit.indexOf("MINUTES") > -1){
			cron = "0 0/" + num + " * * * ?";
			if(num == 1) { cron = "0 * * * * ?"; }
			return cron;
		}else if(unit.indexOf("HOURS") > -1){
			cron = "0 0 0/" + num + " * * ?";
			if(num == 1) { cron = "0 0 * * * ?"; }
			return cron;
		}else if(unit.indexOf("DAYS") > -1){
			cron = "0 0 0 0/" + num + " * ?"; //매 하루마다
			if(num == 1) { cron = "0 0 0 * * ?"; }
			return cron;
		}else{
			return "CRON-PATTERN-ERROR [" +  cron + "]";
		}
	}
};
