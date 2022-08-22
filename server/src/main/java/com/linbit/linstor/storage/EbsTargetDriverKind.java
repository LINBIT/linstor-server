package com.linbit.linstor.storage;

import com.linbit.linstor.api.ApiConsts;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;

public class EbsTargetDriverKind implements StorageDriverKind
{
    public EbsTargetDriverKind()
    {
    }

    @Override
    public String getDriverName()
    {
        return "EbsTargetDriver";
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
        return true;
    }

    @Override
    public boolean hasBackingStorage()
    {
        return true;
    }
}
