/*
 * Copyright 2002-2012 the original author or authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package plantpulse.server.mvc.util;

import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.time.temporal.ChronoUnit;
import java.util.Date;
import java.util.Locale;

import org.apache.commons.lang.StringUtils;
import org.joda.time.DateTime;
import org.joda.time.format.DateTimeFormat;
import org.joda.time.format.DateTimeFormatter;


/**
 * Created by tw.jang on 16. 2. 1.
 */
public final class DateUtils2 {
	
	private DateUtils2() { }
	
	public static final int HOURS_24 = 24;
	
	public static final int MINUTES_60 = 60;
	
	public static final int SECONDS_60 = 60;

	public static final int MILLI_SECONDS_1000 = 1000;
	
	private static final int UNIT_HEX = 16;
	
	/** Date pattern */
	public static final String DATE_PATTERN_DASH = "yyyy-MM-dd";

	/** Time pattern */
	public static final String TIME_PATTERN = "HH:mm";

	/** Date Time pattern */
	public static final String DATE_TIME_PATTERN = "yyyy-MM-dd HH:mm:ss";

	/** Date Time pattern */
	public static final String KOR_DATE_TIME_PATTERN = "yyyy년MM월dd일 HH시mm분ss초";

	/** Date Time pattern */
	public static final String KOR_DATE_PATTERN = "yyyy년MM월dd일";

	/** Date HMS pattern */
	public static final String DATE_HMS_PATTERN = "yyyyMMddHHmmss";

	/** Time stamp pattern */
	public static final String TIMESTAMP_PATTERN = "yyyy-MM-dd HH:mm:ss.SSS";

	/** year pattern (yyyy)*/
    public static final String YEAR_PATTERN = "yyyy";

    /** month pattern (MM) */
    public static final String MONTH_PATTERN = "MM";

    /** day pattern (dd) */
    public static final String DAY_PATTERN = "dd";

    /** date pattern (yyyyMMdd) */
    public static final String DATE_PATTERN = "yyyyMMdd";

    /** hour, minute, second pattern (HHmmss) */
    public static final String TIME_HMS_PATTERN = "HHmmss";

    /** hour, minute, second pattern (HH:mm:ss) */
    public static final String TIME_HMS_PATTERN_COLONE = "HH:mm:ss";
    
    public static final String START_TIME = "00:00:00";
    public static final String END_TIME = "23:59:59";
    

	/**
	 * 현재 날짜, 시간을 조회하여 문자열 형태로 반환한다.<br>
	 *
	 * @return (yyyy-MM-dd HH:mm:ss) 포맷으로 구성된 현재 날짜와 시간
	 */
	public static String getNow() {
		return getNow(DATE_TIME_PATTERN);
	}

	/**
	 * 현재 날짜, 시간을 조회을 조회하고, pattern 형태의 포맷으로 문자열을 반환한다.<br><br>
	 *
	 * DateUtils.getNow("yyyy년 MM월 dd일 hh시 mm분 ss초") = "2012년 04월 12일 20시 41분 50초"<br>
	 * DateUtils.getNow("yyyy-MM-dd HH:mm:ss") = "2012-04-12 20:41:50"
	 *
	 * @param pattern 날짜 및 시간에 대한 포맷
	 * @return patter 포맷 형태로 구성된 현재 날짜와 시간
	 */
	public static String getNow(String pattern) {
		LocalDateTime dateTime = LocalDateTime.now();
		java.time.format.DateTimeFormatter formatter = java.time.format.DateTimeFormatter.ofPattern(pattern);
		return dateTime.format(formatter);
	}

	/**
	 * 입력받은 일자의 요일을 반환한다.<br><br>
	 *
	 * DateUtils.getDayOfWeek("2010-11-26") = "금"
	 *
	 * @param str (yyyy-MM-dd) 포맷의 문자열
	 * @return 입력받은 일자에 해당하는 요일
	 */
	public static String getDayOfWeek(String str) {
		return getDayOfWeek(str, true, new Locale("ko","KR"));
	}
	
	/**
	 * 입력받은 일자의 요일을 반환한다.<br>
	 * Locale 정보를 받아 해당하는 언어에 대해서 약어로 보여주거나 전체 요일 형태로 보여준다.<br><br>
	 *
	 * DateUtil.getDayOfWeek("2011-02-04", true, Locale.US) = "Fri";<br>
	 * DateUtil.getDayOfWeek("2011-02-04", false, Locale.US) = "Friday";<br>
	 * DateUtil.getDayOfWeek("2011-02-04", true, Locale.KOREA) = "금";<br>
	 * DateUtil.getDayOfWeek("2011-02-04", false, Locale.KOREA) = "금요일";<br>
	 *
	 * @param str (yyyy-MM-dd) 포맷의 문자열
	 * @param abbreviation true면 약어, false면 전체 요일 형태
	 * @param locale locale
	 * @return 입력받은 일자에 해당하는 요일
	 */
	public static String getDayOfWeek(String str, Boolean abbreviation, Locale locale) {
		
		DateTimeFormatter fmt = DateTimeFormat.forPattern(DATE_PATTERN_DASH);
		DateTime dt = fmt.parseDateTime(str);
		DateTime.Property dayOfWeek = dt.dayOfWeek();

		if (abbreviation) {
			return dayOfWeek.getAsShortText(locale);
		} else {
			return dayOfWeek.getAsText(locale);
		}	
	}

	/**
	 * 입력받은 두 날짜 사이의 일자를 계산한다.<br>
	 * -startDate와 endDate는 patter의 포맷형식을 꼭 따라야 한다.<br><br>
	 *
	 * DateUtils.getDays("2010-11-24", "2010-12-30", "yyyy-MM-dd") = 36
	 *
	 * @param startDate 시작일
	 * @param endDate 종료일
	 * @param pattern 날짜 포맷
	 * @return 두 날짜 사이의 일자
	 */
	public static int getDays(String startDate, String endDate, String pattern) {

		java.time.format.DateTimeFormatter formatter = java.time.format.DateTimeFormatter.ofPattern(pattern);

		LocalDate localStartDate = LocalDate.parse(startDate, formatter);
		LocalDate localEndDate = LocalDate.parse(endDate, formatter);

		return (int) ChronoUnit.DAYS.between(localStartDate, localEndDate);
	}

	/**
	 * 입력받은 두 날짜 사이의 일자를 계산한다.<br><br>
	 *
	 * DateUtils.getDays("2010-11-24", "2010-12-30") = 36
	 *
	 * @param startDate (yyyy-MM-dd) 포맷 형태의 시작일
	 * @param endDate (yyyy-MM-dd) 포맷 형태의 종료일
	 * @return 두 날짜 사이의 일자
	 */
	public static int getDays(String startDate, String endDate) {
		return getDays(startDate, endDate, DATE_PATTERN_DASH);
	}

	public static int getDays(LocalDate startDate, LocalDate endDate) {
		return (int) ChronoUnit.DAYS.between(startDate, endDate);
	}

	public static int getDays(LocalDateTime startDateTime, LocalDateTime endDateTime) {
		return (int) ChronoUnit.DAYS.between(startDateTime, endDateTime);
	}

	/**
	 * 입력받은 두 일자가 같은지 여부를 확인한다.<br><br>
	 *
	 * DateUtils.equals(new LocalDate(1292252400000l), new LocalDate(1292252400000l)) = true
	 *
	 * @param date1 LocalDate형의 일자
	 * @param date2 LocalDate형의 일자
	 * @return 입력받은 두 일자가 같으면 true를 그렇지않으면 false를 반환.
	 */
	public static boolean equals(LocalDate date1, LocalDate date2) {
		return date1.isEqual(date2);
	}

	/**
	 * 입력받은 두 일자가 같은지 여부를 확인한다.<br><br>
	 *
	 * DateUtils.equals(new Date(1292252400000l), "2010-12-14") = true
	 *
	 * @param date LocalDate형의 일자
	 * @param dateStr (yyyy-MM-dd) 포맷형태의 일자
	 * @return 일치하면 true를 그렇지않으면 false를 반환
	 */
	public static boolean equals(LocalDate date, String dateStr) {
		return equals(date, dateStr, DATE_PATTERN_DASH);
	}

	/**
	 * 입력받은 두 일자가 같은지 여부를 확인한다.<br><br>
	 *
	 * DateUtils.equals(new LocalDate(1292252400000l), "2010/12/14", "yyyy/MM/dd") = true
	 *
	 * @param date Date형의 일자
	 * @param dateStr 포맷형태의 일자
	 * @param pattern 날짜 포맷
	 * @return 입력받은 두 일자가 같으면 true를 그렇지않으면 false를 반환.
	 */
	public static boolean equals(LocalDate date, String dateStr, String pattern) {

		java.time.format.DateTimeFormatter formatter = java.time.format.DateTimeFormatter.ofPattern(pattern);
		LocalDate parsedDate = LocalDate.parse(dateStr, formatter);

		return equals(date, parsedDate);
	}

	/**
	 * 입력일을 기준으로 이달 마지막 날짜를 반환한다.<br><br>
	 * 
	 * DateUtils.getLastDateOfMonth("2010-11-20") = "2010-11-01"
	 * 
	 * @param date 일자
	 * @param dateFormat 일자에 대한 날짜형식
	 * @return 입력일을 기준으로한 이번달 첫번째 일자
	 */
	public static String getFirstDateOfMonth(String date, String dateFormat) {
		DateTimeFormatter fmt = DateTimeFormat.forPattern(dateFormat);
		DateTime dt = fmt.parseDateTime(date);
		DateTime dtRet = new DateTime(dt.getYear(), dt.getMonthOfYear(), 1, 0, 0, 0, 0);
		return fmt.print(dtRet);
	}

	/**
	 * 입력일을 기준으로 이달 첫번째 일자를 반환한다.<br><br>
	 *
	 * DateUtils.getFirstDateOfMonth("2010-12-18") = "2010-12-01"
	 *
	 * @param date (yyyy-MM-dd) 포맷형태의 일자
	 * @return 입력을을 기준으로한 이달 첫번째 일자
	 */
	public static String getFirstDateOfMonth(String date) {
		return getFirstDateOfMonth(date, DATE_PATTERN_DASH);
	}

	/**
	 * 입력일을 기준으로 이달 마지막 일자를 반환한다.<br><br>
	 *
	 * DateUtils.getLastDateOfMonth("2010-11-20") = "2010-11-30"
	 *
	 * @param date 일자
	 * @param dateFormat 일자에 대한 날짜형식
	 * @return 입력일을 기준으로한 이번달 첫번째 일자
	 */
	public static String getLastDateOfMonth(String date, String dateFormat) {
		String firstDateOfMonth = getFirstDateOfMonth(date, dateFormat);

		DateTimeFormatter fmt = DateTimeFormat.forPattern(dateFormat);
		DateTime dt = fmt.parseDateTime(firstDateOfMonth);
		dt = dt.plusMonths(1).minusDays(1);
		return fmt.print(dt);
	}

	/**
	 * 입력일을 기준으로 이달 마지막 일자를 반환한다.<br><br>
	 *
	 * DateUtils.getLastDateOfMonth("2010-11-20") = "2010-11-30"
	 *
	 * @param date (yyyy-MM-dd) 포맷형태의 일자
	 * @return 입력을을 기준으로한 이달 마지막 일자
	 */
	public static String getLastDateOfMonth(String date) {
		return getLastDateOfMonth(date, DATE_PATTERN_DASH);
	}

	/**
	 * 입력일을 기준으로 전달의 첫번째 일자를 반환한다.<br><br>
	 * 
	 * DateUtils.getFirstDateOfPrevMonth("2010-11-20") = "2010-10-01"
	 *
	 * @param date (yyyy-MM-dd) 포맷형태의 일자
	 * @return 입력을을 기준으로한 전달 첫번째 일자
	 */
	public static String getFirstDateOfPrevMonth(String date) {
		String firstDateOfMonth = getFirstDateOfMonth(date);

		DateTimeFormatter fmt = DateTimeFormat.forPattern(DATE_PATTERN_DASH);
		DateTime dt = fmt.parseDateTime(firstDateOfMonth);
		dt = dt.minusMonths(1);
		return fmt.print(dt);
	}

	/**
	 * 입력일을 기준으로 전달의 마지막 일자를 반환한다.<br><br>
	 * 
	 * DateUtils.getLastDateOfPrevMonth("2010-11-20") = "2010-10-31"
	 *
	 * @param date (yyyy-MM-dd) 포맷형태의 일자
	 * @return 입력을을 기준으로한 전달 마지막 일자
	 */
	public static String getLastDateOfPrevMonth(String date) {
		String firstDateOfMonth = getFirstDateOfMonth(date);

		DateTimeFormatter fmt = DateTimeFormat.forPattern(DATE_PATTERN_DASH);
		DateTime dt = fmt.parseDateTime(firstDateOfMonth);
		dt = dt.minusDays(1);
		return fmt.print(dt);
	}
	
	/**
	 * 오늘을 기준으로 어제의 시작일자를 반환한다.
	 * 
	 * @return
	 */
	public static String getFirstDateOfPrevDay() {
		DateTime date = new DateTime().withTimeAtStartOfDay().minusDays(1);
		DateTimeFormatter fmt = DateTimeFormat.forPattern(DateUtils2.DATE_TIME_PATTERN);
		return fmt.print(date);
	}
	
	/**
	 * 오늘을 기준으로 어제의 마지막 일자를 반환한다.
	 * 
	 * @return
	 */
	public static String getLastDateOfPrevDay() {
		DateTime date = new DateTime().withTimeAtStartOfDay().minusSeconds(1);
		DateTimeFormatter fmt = DateTimeFormat.forPattern(DateUtils2.DATE_TIME_PATTERN);
		return fmt.print(date);
	}


    /**
     * 입력된 일자를 기준으로 해당년도가 윤년인지 여부를 반환한다.
     * 
     * @param  inputDate (yyyy-MM-dd) 형식의 일자
     * @return 윤년이면 true를 그렇지 않으면 false를 반환
     */
	public static boolean isLeapYear(String inputDate) {
		return isLeapYear(Integer.parseInt(inputDate.substring(0, 4)));
	}
    
    /**
     * 정수형태로 입력된 년도를 기준으로 해당년도가 윤년인지 여부를 반환한다.
     * 
     * @param year 년도
     * @return year이 윤년이면 true를 그렇지 않으면 false를 반환
     */
    public static boolean isLeapYear(int year) {
    	return LocalDate.of(year, 1, 1).isLeapYear();
    }
    
	public static LocalDateTime convertToDateTime(String dateTime, String pattern) {

		if (StringUtils.isEmpty(dateTime)) {
			return null;
		}

		if (dateTime.length() == 8) {
			return DateUtils2.convertToDate(dateTime, "yyyyMMdd").atTime(0, 0, 0);
		}

		if (dateTime.length() == 10) {
			return DateUtils2.convertToDate(dateTime).atTime(0, 0, 0);
		}

		java.time.format.DateTimeFormatter formatter = java.time.format.DateTimeFormatter.ofPattern(pattern);
		return LocalDateTime.parse(dateTime, formatter);
	}

	public static LocalDateTime convertToDateTime(String dateTime) {
		return convertToDateTime(dateTime, DATE_TIME_PATTERN);
	}

	public static LocalDateTime convertToDateTime(long timeMillis) {
		Date date = new Date(timeMillis);
		return date.toInstant().atZone(ZoneId.of("Asia/Seoul")).toLocalDateTime();
	}

	public static LocalDate convertToDate(String date, String pattern) {
		java.time.format.DateTimeFormatter formatter = java.time.format.DateTimeFormatter.ofPattern(pattern);
		return LocalDate.parse(date, formatter);
	}

	public static LocalDate convertToDate(String date) {
		return convertToDate(date, DATE_PATTERN_DASH);
	}

	public static String convertToString(LocalDateTime dateTime, String pattern) {
		java.time.format.DateTimeFormatter formatter = java.time.format.DateTimeFormatter.ofPattern(pattern);
		return dateTime.format(formatter);
	}

	public static String convertToString(LocalDateTime dateTime) {
		java.time.format.DateTimeFormatter formatter = java.time.format.DateTimeFormatter.ofPattern(DATE_TIME_PATTERN);
		return dateTime.format(formatter);
	}

	public static String convertToString(LocalDate date, String pattern) {
		java.time.format.DateTimeFormatter formatter = java.time.format.DateTimeFormatter.ofPattern(pattern);
		return date.format(formatter);
	}

	public static String convertToString(LocalDate date) {
		java.time.format.DateTimeFormatter formatter = java.time.format.DateTimeFormatter.ofPattern(DATE_PATTERN_DASH);
		return date.format(formatter);
	}

	public static String convertToString(LocalTime time, String pattern) {
		java.time.format.DateTimeFormatter formatter = java.time.format.DateTimeFormatter.ofPattern(pattern);
		return time.format(formatter);
	}

	public static String convertToString(LocalTime time) {
		java.time.format.DateTimeFormatter formatter = java.time.format.DateTimeFormatter.ofPattern(TIME_HMS_PATTERN_COLONE);
		return time.format(formatter);
	}

	public static String convertToStrKorDateTime(LocalDateTime dateTime) {
		java.time.format.DateTimeFormatter formatter = java.time.format.DateTimeFormatter.ofPattern(KOR_DATE_TIME_PATTERN);
		return dateTime.format(formatter);
	}

	public static String convertToStrKorDateTime(String dateTimeStr) {
		return convertToStrKorDateTime(dateTimeStr, DATE_TIME_PATTERN);
	}

	public static String convertToStrKorDateTime(String dateTimeStr, String pattern) {
		LocalDateTime dateTime = convertToDateTime(dateTimeStr, pattern);
		java.time.format.DateTimeFormatter formatter = java.time.format.DateTimeFormatter.ofPattern(KOR_DATE_TIME_PATTERN);
		return dateTime.format(formatter);
	}

	public static String convertToStrKorDate(LocalDateTime dateTime) {
		java.time.format.DateTimeFormatter formatter = java.time.format.DateTimeFormatter.ofPattern(KOR_DATE_PATTERN);
		return dateTime.format(formatter);
	}

	public static String convertToStrKorDate(String dateStr) {
		return convertToStrKorDate(dateStr, DATE_PATTERN_DASH);
	}

	public static String convertToStrKorDate(String dateStr, String pattern) {
		LocalDateTime dateTime = convertToDateTime(dateStr, pattern);
		java.time.format.DateTimeFormatter formatter = java.time.format.DateTimeFormatter.ofPattern(KOR_DATE_PATTERN);
		return dateTime.format(formatter);
	}

	public static long convertToTimeMillis(LocalDateTime dateTime) {
		ZonedDateTime zdt = dateTime.atZone(ZoneId.of("Asia/Seoul"));
		return zdt.toInstant().toEpochMilli();
	}

/*	public static void main(String[] args) {
		System.out.println(convertToStrKorDateTime("2016-09-04 12:22:22"));
		System.out.println(convertToStrKorDateTime(LocalDateTime.now()));
		System.out.println(convertToStrKorDate(LocalDateTime.now()));
		System.out.println(convertToStrKorDate("2016-09-04 12:22:22", "yyyy-MM-dd HH:mm:ss"));
		System.out.println(convertToDateTime(System.currentTimeMillis()));
		System.out.println(convertToDateTime(convertToTimeMillis(LocalDateTime.now())));
	}*/

}
