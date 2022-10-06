package com.linbit.linstor.backupshipping;

import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.time.format.DateTimeFormatter;
import java.util.Date;

public class S3Consts
{
    public static final String BACKUP_PREFIX = "back_";
    public static final int BACKUP_PREFIX_LEN = BACKUP_PREFIX.length();
    public static final String SNAP_NAME_SEPARATOR = "^";
    public static final int SNAP_NAME_SEPARATOR_LEN = SNAP_NAME_SEPARATOR.length();
    public static final String META_SUFFIX = ".meta";
    public static final int META_SUFFIX_LEN = META_SUFFIX.length();
    private static final DateFormat DATE_FORMAT = new SimpleDateFormat("yyyyMMdd_HHmmss");

    /**
     * This method is needed because {@link DateFormat} is not thread-safe.
     * This is only a quick-fix, a better solution is to switch to {@link DateTimeFormatter} as described in internal
     * issue #774
     */
    public static String format(Date date)
    {
        synchronized (DATE_FORMAT)
        {
            return DATE_FORMAT.format(date);
        }
    }

    public static Date parse(String timestamp) throws ParseException
    {
        synchronized (DATE_FORMAT)
        {
            return DATE_FORMAT.parse(timestamp);
        }
    }
}
