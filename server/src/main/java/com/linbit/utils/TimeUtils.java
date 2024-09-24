package com.linbit.utils;

import com.linbit.linstor.core.LinStor;

import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.time.temporal.ChronoField;

public class TimeUtils
{

    // this needs to stay the same since journalctl needs the date this way
    public static final DateTimeFormatter JOURNALCTL_DF = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss");
    public static final DateTimeFormatter DTF_NO_SPACE = DateTimeFormatter.ofPattern("yyyy-MM-dd_HH-mm-ss");
    public static final DateTimeFormatter DTF_NO_TIME = DateTimeFormatter.ofPattern("yyyy-MM-dd");

    /**
     * ATTENTION: DateTimeFormatter.ofPattern("X") requires an {@link ChronoField#OFFSET_SECONDS} (which results in "Z"
     * or "+0100" string). LocalDateTime does NOT have such that {@link ChronoField}. If you want to use a format with
     * such an offset use {@link #toLocalZonedDateTime(long)} instead of this method.
     */
    public static LocalDateTime millisToDate(long millis)
    {
        return LocalDateTime.ofInstant(Instant.ofEpochMilli(millis), ZoneId.systemDefault());
    }

    public static ZonedDateTime toLocalZonedDateTime(long millis)
    {
        return millisToDate(millis).atZone(ZoneId.systemDefault());
    }

    public static long getEpochMillis(LocalDateTime date)
    {
        return date.toInstant(LinStor.LOCAL_ZONE_OFFSET).toEpochMilli();
    }

    private TimeUtils()
    {
        // utils-class, do not allow instance
    }
}
