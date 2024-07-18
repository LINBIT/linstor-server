package com.linbit.linstor.backupshipping;

import com.linbit.linstor.api.ApiConsts;
import com.linbit.linstor.storage.kinds.ExtTools;
import com.linbit.linstor.storage.kinds.ExtToolsInfo.Version;

import java.time.format.DateTimeFormatter;
import java.util.Collections;
import java.util.Map;

public class BackupConsts
{
    public static final String BACKUP_PREFIX = "back_";
    public static final int BACKUP_PREFIX_LEN = BACKUP_PREFIX.length();
    public static final String SNAP_NAME_SEPARATOR = "^";
    public static final int SNAP_NAME_SEPARATOR_LEN = SNAP_NAME_SEPARATOR.length();
    public static final String META_SUFFIX = ".meta";
    public static final int META_SUFFIX_LEN = META_SUFFIX.length();
    public static final Map<ExtTools, Version> S3_REQ_EXT_TOOLS = Collections.unmodifiableMap(
        Collections.singletonMap(ExtTools.ZSTD, null)
    );
    public static final Map<ExtTools, Version> S3_OPT_EXT_TOOLS = Collections.unmodifiableMap(
        Collections.emptyMap()
    );
    public static final Map<ExtTools, Version> L2L_REQ_EXT_TOOLS = Collections.unmodifiableMap(
        Collections.singletonMap(ExtTools.SOCAT, null)
    );
    public static final Map<ExtTools, Version> L2L_OPT_EXT_TOOLS = Collections.unmodifiableMap(
        Collections.singletonMap(ExtTools.ZSTD, null)
    );
    public static final String CONCURRENT_BACKUPS_KEY = ApiConsts.NAMESPC_BACKUP_SHIPPING + "/" +
        ApiConsts.KEY_MAX_CONCURRENT_BACKUPS_PER_NODE;
    public static final DateTimeFormatter DATE_FORMAT = DateTimeFormatter.ofPattern("yyyyMMdd_HHmmss");

    private BackupConsts()
    {
        // utils-class, do not allow instance
    }
}
