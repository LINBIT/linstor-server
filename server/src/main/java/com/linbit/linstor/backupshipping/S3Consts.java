package com.linbit.linstor.backupshipping;

import java.text.DateFormat;
import java.text.SimpleDateFormat;

public class S3Consts
{
    public static final String BACKUP_PREFIX = "back_";
    public static final int BACKUP_PREFIX_LEN = BACKUP_PREFIX.length();
    public static final String SNAP_NAME_SEPARATOR = "^";
    public static final int SNAP_NAME_SEPARATOR_LEN = SNAP_NAME_SEPARATOR.length();
    public static final String META_SUFFIX = ".meta";
    public static final int META_SUFFIX_LEN = META_SUFFIX.length();
    public static final DateFormat DATE_FORMAT = new SimpleDateFormat("yyyyMMdd_HHmmss");
}
