package com.linbit.linstor.api.pojo.backups;

import com.linbit.linstor.annotation.Nullable;
import com.linbit.linstor.storage.kinds.DeviceLayerKind;

public class BackupInfoVlmPojo
{
    private final String backupVlmName;
    private final DeviceLayerKind layerType;
    private final @Nullable Long dlSizeKib;
    private final @Nullable Long allocSizeKib;
    private final @Nullable Long useSizeKib;

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

    public @Nullable Long getDlSizeKib()
    {
        return dlSizeKib;
    }

    public @Nullable Long getAllocSizeKib()
    {
        return allocSizeKib;
    }

    public @Nullable Long getUseSizeKib()
    {
        return useSizeKib;
    }
}
