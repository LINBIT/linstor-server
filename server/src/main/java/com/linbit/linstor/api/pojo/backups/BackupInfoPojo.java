package com.linbit.linstor.api.pojo.backups;

import java.util.List;

public class BackupInfoPojo
{
    private final String rscName;
    private final String snapName;
    private final String fullBackupName;
    private final String latestBackupName;
    private final int backupCount;
    private final long dlSizeKib;
    private final long allocSizeKib;
    private final List<BackupInfoStorPoolPojo> storpools;

    public BackupInfoPojo(
        String rscNameRef,
        String snapNameRef,
        String fullBackupNameRef,
        String latestBackupNameRef,
        int backupCountRef,
        long dlSizeKibRef,
        long allocSizeKibRef,
        List<BackupInfoStorPoolPojo> storpoolsRef
    )
    {
        rscName = rscNameRef;
        snapName = snapNameRef;
        fullBackupName = fullBackupNameRef;
        latestBackupName = latestBackupNameRef;
        backupCount = backupCountRef;
        dlSizeKib = dlSizeKibRef;
        allocSizeKib = allocSizeKibRef;
        storpools = storpoolsRef;
    }

    public String getRscName()
    {
        return rscName;
    }

    public String getSnapName()
    {
        return snapName;
    }

    public String getFullBackupName()
    {
        return fullBackupName;
    }

    public String getLatestBackupName()
    {
        return latestBackupName;
    }

    public int getBackupCount()
    {
        return backupCount;
    }

    public long getDlSizeKib()
    {
        return dlSizeKib;
    }

    public long getAllocSizeKib()
    {
        return allocSizeKib;
    }

    public List<BackupInfoStorPoolPojo> getStorpools()
    {
        return storpools;
    }
}
