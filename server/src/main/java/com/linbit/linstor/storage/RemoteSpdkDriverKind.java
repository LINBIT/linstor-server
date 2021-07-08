package com.linbit.linstor.storage;

import com.linbit.linstor.api.ApiConsts;
import com.linbit.linstor.storage.kinds.DeviceProviderKind;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;

public class RemoteSpdkDriverKind implements StorageDriverKind
{
    public RemoteSpdkDriverKind()
    {
    }

    @Override
    public String getDriverName()
    {
        return "RemoteSpdkDriver";
    }

    @Override
    public Map<String, String> getStaticTraits()
    {
        final HashMap<String, String> traits = new HashMap<>();

        traits.put(ApiConsts.KEY_STOR_POOL_PROVISIONING, ApiConsts.VAL_STOR_POOL_PROVISIONING_FAT);

        return traits;
     }

    @Override
    public Set<String> getConfigurationKeys()
    {
        return Collections.emptySet();
    }

    @Override
    public boolean isSnapshotSupported()
    {
        return DeviceProviderKind.REMOTE_SPDK.isSnapshotSupported();
    }

    @Override
    public boolean hasBackingStorage()
    {
        return DeviceProviderKind.REMOTE_SPDK.hasBackingDevice();
    }
}
