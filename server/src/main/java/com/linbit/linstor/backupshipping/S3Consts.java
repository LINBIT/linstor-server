package com.linbit.linstor.backupshipping;

import java.text.DateFormat;
import java.text.SimpleDateFormat;

public class BackupShippingConsts
{
    public static final String BACKUP_KEY_FORMAT = "%s%s_%05d_%s";
    public static final String SNAP_PREFIX = "back_";
    public static final int SNAP_PREFIX_LEN = SNAP_PREFIX.length();
    public static final String META_SUFFIX = ".meta";
    public static final int META_SUFFIX_LEN = META_SUFFIX.length();
    public static final DateFormat DATE_FORMAT = new SimpleDateFormat("yyyyMMdd_HHmmss");
}
