package com.linbit.utils;

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
    public static final DateTimeFormatter DTF_ISO_8601_FOR_ZFS_RENAME = DateTimeFormatter.ofPattern(
        "yyyy-MM-dd'T'HH-mm-ss-SSS"
    );

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
        return date.atZone(ZoneId.systemDefault()).toInstant().toEpochMilli();
    }

    public static String getZfsRenameTime()
    {
        return getZfsRenameTime(LocalDateTime.now());
    }

    public static String getZfsRenameTime(LocalDateTime nowRef)
    {
        return DTF_ISO_8601_FOR_ZFS_RENAME.format(nowRef);
    }

    private TimeUtils()
    {
        // utils-class, do not allow instance
    }

}
