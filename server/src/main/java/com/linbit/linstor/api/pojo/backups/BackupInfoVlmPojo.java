package com.linbit.linstor.api.pojo.backups;

import com.linbit.linstor.storage.kinds.DeviceLayerKind;

import javax.annotation.Nullable;

public class BackupInfoVlmPojo
{
    private final String backupVlmName;
    private final DeviceLayerKind layerType;
    private final Long dlSizeKib;
    private final Long allocSizeKib;
    private final Long useSizeKib;

    public BackupInfoVlmPojo(
        String backupVlmNameRef,
        DeviceLayerKind layerTypeRef,
        @Nullable Long dlSizeKibRef,
        @Nullable Long allocSizeKibRef,
        @Nullable Long useSizeKibRef
    )
    {
        backupVlmName = backupVlmNameRef;
        layerType = layerTypeRef;
        dlSizeKib = dlSizeKibRef;
        allocSizeKib = allocSizeKibRef;
        useSizeKib = useSizeKibRef;
    }

    public String getBackupVlmName()
    {
        return backupVlmName;
    }

    public DeviceLayerKind getLayerType()
    {
        return layerType;
    }

    public Long getDlSizeKib()
    {
        return dlSizeKib;
    }

    public Long getAllocSizeKib()
    {
        return allocSizeKib;
    }

    public Long getUseSizeKib()
    {
        return useSizeKib;
    }
}
