package com.linbit.linstor.storage;

import com.linbit.linstor.api.ApiConsts;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

public class FileThinDriverKind implements StorageDriverKind
{
    public FileThinDriverKind()
    {
    }

    @Override
    public String getDriverName()
    {
        return "FileThinDriver";
    }

    @Override
    public Map<String, String> getStaticTraits()
    {
        final HashMap<String, String> traits = new HashMap<>();

        traits.put(ApiConsts.KEY_STOR_POOL_PROVISIONING, ApiConsts.VAL_STOR_POOL_PROVISIONING_THIN);

        return traits;
    }

    @Override
    public Set<String> getConfigurationKeys()
    {
        final HashSet<String> keySet = new HashSet<>();

        keySet.add(StorageConstants.CONFIG_FILE_DIRECTORY_KEY);

        return keySet;
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
