package com.zt.flink.test.utils;

import lombok.extern.slf4j.Slf4j;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.time.*;
import java.time.temporal.ChronoUnit;
import java.time.temporal.Temporal;
import java.util.Calendar;
import java.util.Date;

/**
 * @author zt
 */
@Slf4j
public class DateUtils {
    /**
     * timeStamp->string
     * @param timeStamp
     * @param pattern
     * @return
     */
    public static String format(long timeStamp,String pattern) {
        SimpleDateFormat dateFormat = new SimpleDateFormat(pattern);
        return dateFormat.format(new Date(timeStamp));
    }

    /**
     * string->date
     * @param date
     * @param pattern
     * @return
     */
    public static Date format(String date, String pattern) {
        SimpleDateFormat dateFormat = new SimpleDateFormat(pattern);
        try {
            return dateFormat.parse(date);
        } catch (ParseException e) {
            log.error("{} to Date failed", date);
        }
        return null;
    }

    /**
     * 当前日期与传入日期的相差天数
     * @param date
     * @return
     */
    public static long dateInterval(Date date) {
        //当前日期
        LocalDate now = LocalDate.now();
        //把传入日期转为localDateTime
        Instant instant = date.toInstant();
        LocalDate localDateTime = instant.atZone(ZoneId.systemDefault()).toLocalDate();
        return Math.abs(localDateTime.until(now, ChronoUnit.DAYS));
    }
    /**
     * 当前日期与传入日期的相差天数
     * @param date
     * @return
     */
    public static long dateInterval(String date) {
        //当前日期
        LocalDate now = LocalDate.now();
        //把传入日期转为localDate
        LocalDate parse = LocalDate.parse(date);
        return Math.abs(parse.until(now, ChronoUnit.DAYS));
    }

    /**
     * 传入日期几天后的日期
     * @return
     */
    public static Date daysAfter(String date,int interval) {
        //把传入日期转为localDate
        LocalDate parse = LocalDate.parse(date);
        LocalDate localDate = parse.plusDays(interval);
        Instant instant = localDate.atStartOfDay(ZoneId.systemDefault()).toInstant();
        return Date.from(instant);
    }

}
