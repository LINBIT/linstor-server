package com.linbit.linstor.api.pojo.backups;

import com.linbit.linstor.annotation.Nullable;
import com.linbit.linstor.storage.kinds.DeviceProviderKind;

import java.util.List;

public class BackupInfoStorPoolPojo
{
    private final String storPoolName;
    private final DeviceProviderKind providerKind;
    private final @Nullable String targetStorPoolName;
    private final @Nullable Long remainingSpaceKib;
    private final List<BackupInfoVlmPojo> vlms;

    public BackupInfoStorPoolPojo(
        String storPoolNameRef,
        DeviceProviderKind providerKindRef,
        @Nullable String targetStorPoolNameRef,
        @Nullable Long remainingSpaceKibRef,
        List<BackupInfoVlmPojo> vlmsRef
    )
    {
        storPoolName = storPoolNameRef;
        providerKind = providerKindRef;
        targetStorPoolName = targetStorPoolNameRef;
        remainingSpaceKib = remainingSpaceKibRef;
        vlms = vlmsRef;
    }

    public String getStorPoolName()
    {
        return storPoolName;
    }

    public DeviceProviderKind getProviderKind()
    {
        return providerKind;
    }

    public @Nullable String getTargetStorPoolName()
    {
        return targetStorPoolName;
    }

    public @Nullable Long getRemainingSpaceKib()
    {
        return remainingSpaceKib;
    }

    public List<BackupInfoVlmPojo> getVlms()
    {
        return vlms;
    }
}
