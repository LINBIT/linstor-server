package com.linbit.linstor.storage;

import com.linbit.linstor.api.ApiConsts;

import java.util.*;

public class SpdkDriverKind implements StorageDriverKind
{
    public SpdkDriverKind()
    {
    }

    @Override
    public String getDriverName()
    {
        return "SpdkDriver";
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
        return false;
    }

    @Override
    public boolean hasBackingStorage()
    {
        return true;
    }
}
